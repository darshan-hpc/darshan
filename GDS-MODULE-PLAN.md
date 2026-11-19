# Plan: Darshan GDS module (NVIDIA GPUDirect Storage)

Status: DRAFT for review. Implementer: follow the phases in order; each phase
should build and pass existing tests before moving on.

## 0. Summary / key decisions

* New instrumentation module named **GDS** (decided: that's the community
  name). Module ID `DARSHAN_GDS_MOD`, name string `"GDS"`, counter prefix
  `GDS_` / `GDS_F_`, files `darshan-gds*`. The wrapped functions keep their
  real `cuFile*` names, and docs should say "GDS (cuFile API)" once so
  users can connect the two.
* Templates, by layer:
  - **Runtime skeleton: `darshan-runtime/lib/darshan-null.c`.** Take the
    lifecycle and boilerplate from here: the recursive mutex,
    `__darshan_disabled` snapshot, `*_WTIME()`, PRE/POST_RECORD,
    `*_runtime_initialize`, `*_track_new_record`, output and cleanup.
    It's small and carries no DFS baggage (mount tracking, parent object
    names, pool/container UUIDs, DAOS event trackers).
  - **Runtime I/O statistics and MPI reduction: lift from DFS/POSIX.**
    That's the read/write record macros, seq/consec, common access sizes,
    and `*_record_reduction_op` / `*_shared_record_variance`. darshan-null
    has no reduction (`mod_redux_func = NULL`).
  - **darshan-util logutils: `darshan-dfs-logutils.c`, not null.** The null
    logutils lacks `log_sizeof_record` and `log_record_metrics`, which the
    accumulator / derived-metrics code needs.
  - **Log format header:** DFS (it has the bucket and rank counters).
* cuFile operates on a POSIX fd registered via `cuFileHandleRegister()`. We
  get the file name from `darshan_posix_lookup_record_name(fd)` (STDIO does
  the same thing at `darshan-stdio.c:371`). The record ID is
  `darshan_core_gen_record_id(path)`, which is **the same ID as the POSIX
  record**. Analysis tools can then join GDS and POSIX records, just as
  they join MPI-IO and POSIX records.
* **No link-time dependency on libcufile or CUDA.** Wrappers resolve the real
  symbols with `dlsym(RTLD_NEXT)` (`MAP_OR_FAIL`) like every other module.
* **Real header if available, private types otherwise** (decided; see §2).
  `cufile.h` pulls in `<cuda.h>`, which isn't on this machine and often
  isn't on HPC login nodes. Configure tests whether `cufile.h` *compiles*.
  If it does, use it; if not, fall back to a private header that declares
  the few cuFile types needed. Either way the module builds.
* Module is **disabled by default** (`--enable-gds-mod`), like DAOS.
* New module ID is **appended** to the end of `DARSHAN_MODULE_IDS` (after
  DAOS), so existing logs and module IDs stay valid.

## 0.1 Environment facts (verified on dev box)

* `libcufile0` / `libcufile-dev` **1.9.1** (CUDA 12.4 packaging):
  `/usr/lib/x86_64-linux-gnu/libcufile.so.0 -> libcufile.so.1.9.1`, header
  `/usr/include/cufile.h` (same version). There is also
  `libcufile_static.a`, so static linking via `--wrap` is a real use case.
* libcufile has **no `NEEDED` entry for libcuda**. It `dlopen`s
  `libcuda.so.1` at run time. No libcuda is installed here, and there is
  no `/etc/cufile.json`.
* libcufile imports `open`, `pread`, `pwrite`, `read`, `write` and `ioctl`
  through normal dynamic symbols. So under LD_PRELOAD, Darshan's POSIX
  wrappers **will** see libcufile's compat-mode I/O (see the layering note
  in §1).
* The header has `#define cuFileDriverClose cuFileDriverClose_v2`. If the
  driver calls are ever wrapped, wrap `cuFileDriverClose_v2`.
  `cuFileDriverClose` is not called through that name.
* Exported symbols include all 6 functions wrapped in v1.

## 1. What gets instrumented (v1)

| cuFile call | Action |
|---|---|
| `cuFileHandleRegister(fh*, descr*)` | Call real function first. Only on success (`ret.err == DARSHAN_CU_FILE_SUCCESS`) and valid pointers (`descr != NULL`, `fh && *fh`), check `descr->type == CU_FILE_HANDLE_TYPE_OPAQUE_FD` (1) before dereferencing `descr->handle.fd`. Look up the name from `descr->handle.fd`, create/find the record. Check `handle_hash` and delete any preexisting entry for `*fh` (avoiding uthash table corruption on handle reuse) before mapping `*fh` -> rec_ref. Count `GDS_HANDLE_REGISTERS`, set open timestamps, add meta time. |
| `cuFileHandleDeregister(fh)` | Count `GDS_HANDLE_DEREGISTERS`, set close timestamps, add meta time, remove from `handle_hash`. Returns void. |
| `cuFileRead(fh, buf, size, file_off, buf_off)` | Sync read. `ret >= 0` = bytes. Update reads, bytes, max offset, size histogram, common access sizes, seq/consec, rw switches, read time, max read time/size. If `ret < 0`, count `GDS_READ_ERRORS` and skip the byte stats. |
| `cuFileWrite(...)` | Same as read, write side. |
| `cuFileBatchIOSubmit(batch, nr, iocbp[], flags)` | Only record if submission succeeded (`ret.err == DARSHAN_CU_FILE_SUCCESS`) and `iocbp != NULL`. For each of the `nr` entries, look up `iocbp[i].fh` in `handle_hash` (skip if untracked; entries in a single batch may target different handles). Count `GDS_BATCH_READS/WRITES` and add `u.batch.size` to `GDS_BATCH_BYTES_READ/WRITTEN` (*requested* bytes, since completion is not tracked). Feed the same size into the histograms/common-access logic. No per-op timing. |
| `cuFileReadAsync / cuFileWriteAsync(fh, buf, size_p, foff_p, boff_p, bytes_p, stream)` | Only record if submission succeeded (`ret.err == DARSHAN_CU_FILE_SUCCESS`). Ensure `size_p != NULL` before dereferencing. Count `GDS_ASYNC_READS/WRITES` only. `*size_p` may be only an upper bound and `*bytes_p` is filled in later by the stream, so record `*size_p` into `GDS_ASYNC_BYTES_REQ_READ/WRITTEN` and nothing else. Do **not** dereference `bytes_p`. |
| `cuFileBufRegister / cuFileBufDeregister` | Not tied to a file, so they don't fit a per-file record. **v1: do not wrap.** (See §8, open question 3.) |
| Driver/property/stream calls | Not wrapped in v1. |

Behavior required of every wrapper: call the real function first and keep
its return value and `errno` unchanged. Use the `GDS_PRE_RECORD()` /
`GDS_POST_RECORD()` lock pattern from DFS. Return early (no recording) if
`darshan_core_disabled_instrumentation()` is set or the handle isn't in the
hash.

### Name lookup fallback
Declare `darshan_posix_lookup_record_name(fd)` as a weak symbol
(`extern char *darshan_posix_lookup_record_name(int fd) __attribute__((weak));`)
or guard with `#ifdef DARSHAN_POSIX`, so `--disable-posix-mod` or static
linking without POSIX does not fail at link time.

If `darshan_posix_lookup_record_name(fd)` returns NULL (POSIX disabled, or fd
opened before Darshan initialized), fall back to `readlink("/proc/self/fd/<fd>")`.
Ensure buffer size is at least `PATH_MAX` and null-terminate (`buf[ret] = '\0'`),
since `readlink(2)` does not append a null byte. If that also fails, skip the
file. This only happens at register time, so the extra syscall is fine.
Implementer: check `darshan_core_register_record()` in darshan-core.c to
confirm that registering a name for a record ID POSIX already registered is
harmless (it should dedupe on the ID). Pass the name either way.

### Interaction with POSIX (document it, don't work around it)
* With nvidia-fs (true GDS), the data moves through ioctls on
  `/dev/nvidia-fs*`. POSIX sees only open/close, plus no read/write counts.
* In **compat mode** (which is what runs on this laptop), libcufile does
  `pread`/`pwrite` internally. Darshan's POSIX wrappers *will* count those,
  so GDS time includes the POSIX time beneath it. That is the same
  layering as MPI-IO over POSIX, and it's useful because it shows which path
  ran. Do **not** add suppression logic.

### Known limitation: dlopen'd libcufile
Some frameworks (e.g. RAPIDS KvikIO) `dlopen("libcufile.so")` and call
through pointers from `dlsym(handle, ...)`. That bypasses symbol
interposition, so neither LD_PRELOAD nor `--wrap` will catch those calls.
Document this; v1 won't try to fix it. (Possible future fix: wrap `dlsym`.
Probably not worth it.)

## 2. Types: real cufile.h when it compiles, private header otherwise

`/usr/include/cufile.h` includes `<cuda.h>`, which isn't on this system, so
on this box the fallback path is the one that gets built and tested.

**Configure** (Phase 3) does this when `--enable-gds-mod` is given: add
`--with-cuda=DIR` (optional; adds `-IDIR/include` to CPPFLAGS for the check
and for the build). Then run
`AC_CHECK_HEADERS([cufile.h], [], [], [[#include <stdbool.h>]])`. That is a
compile test, so it fails if `<cuda.h>` is missing, which is the result we
want. Print which path was chosen in the configure summary ("GDS module
types - cufile.h" / "private"). Never error out just because the header
is missing.

**`darshan-runtime/lib/darshan-gds-types.h`** is the only file
darshan-gds.c includes for cuFile types:

```c
#include <sys/types.h>
#include <stddef.h>

#ifdef HAVE_CUFILE_H
#include <cufile.h>
#define DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD CU_FILE_HANDLE_TYPE_OPAQUE_FD
#define DARSHAN_GDS_READ  CUFILE_READ
#define DARSHAN_GDS_WRITE CUFILE_WRITE
#define DARSHAN_CU_FILE_SUCCESS CU_FILE_SUCCESS
#else
/* private declarations below */
#endif
```

The .c file uses only the `DARSHAN_*` constants and the type names that are
common to both branches. It reads only these fields: `descr->type`,
`descr->handle.fd`, `iocbp[i].fh`, `.opcode`, `.u.batch.size`,
`.u.batch.file_offset`, and `CUfileError_t.err`. It must compile cleanly
both ways. Private branch:

```c
typedef void *CUfileHandle_t;
typedef void *CUfileBatchHandle_t;
typedef struct CUstream_st *CUstream;            /* opaque, as in cuda.h */
typedef struct { int err; int cu_err; } CUfileError_t;   /* enum, CUresult are int-sized */
typedef struct {
    int type;                                    /* enum CUfileFileHandleType */
    union { int fd; void *handle; } handle;
    const void *fs_ops;
} CUfileDescr_t;
typedef struct {
    int mode;                                    /* CUfileBatchMode_t, must be first */
    union { struct { void *devPtr_base; off_t file_offset;
                     off_t devPtr_offset; size_t size; } batch; } u;
    CUfileHandle_t fh;
    int opcode;                                  /* CUFILE_READ=0, CUFILE_WRITE=1 */
    void *cookie;
} CUfileIOParams_t;
#define DARSHAN_CU_FILE_HANDLE_TYPE_OPAQUE_FD 1
#define DARSHAN_GDS_READ  0
#define DARSHAN_GDS_WRITE 1
#define DARSHAN_CU_FILE_SUCCESS 0
```

Add a comment saying these were checked against cufile.h from libcufile-dev
1.9.1. As part of Phase 2, add a one-off check that compiles a tiny program
with `-I/usr/include` and a stub `cuda.h` (`typedef int CUresult; typedef
struct CUstream_st *CUstream;`). It should `_Static_assert` that `sizeof`
and `offsetof` match between the private types and the real
`CUfileDescr_t`, `CUfileIOParams_t` and `CUfileError_t`. Keep this as a
dev-only script under `darshan-test/gds/`, not part of the build. (Here the
real header can only be used with that stub `cuda.h`. On a machine with
CUDA, the `HAVE_CUFILE_H` build path covers this automatically.) Name the
private struct tags differently from the real ones (e.g.
`struct darshan_cufile_descr`) so that, if both are ever included by
mistake, the clash shows up as a typedef-redefinition error.

## 3. Phase 1: log format (`include/`)

New `include/darshan-gds-log-format.h`, modeled on the DFS one:

* `#define DARSHAN_GDS_VER 1`
* `GDS_COUNTERS` X-macro (int64):
  `GDS_HANDLE_REGISTERS, GDS_HANDLE_DEREGISTERS,
  GDS_READS, GDS_WRITES, GDS_READ_ERRORS, GDS_WRITE_ERRORS,
  GDS_BATCH_READS, GDS_BATCH_WRITES,
  GDS_ASYNC_READS, GDS_ASYNC_WRITES,
  GDS_BYTES_READ, GDS_BYTES_WRITTEN,
  GDS_BATCH_BYTES_READ, GDS_BATCH_BYTES_WRITTEN,
  GDS_ASYNC_BYTES_REQ_READ, GDS_ASYNC_BYTES_REQ_WRITTEN,
  GDS_MAX_BYTE_READ, GDS_MAX_BYTE_WRITTEN,
  GDS_CONSEC_READS, GDS_CONSEC_WRITES, GDS_SEQ_READS, GDS_SEQ_WRITES,
  GDS_RW_SWITCHES, GDS_MAX_READ_TIME_SIZE, GDS_MAX_WRITE_TIME_SIZE,
  GDS_SIZE_READ_0_100 ... _1G_PLUS (10), GDS_SIZE_WRITE_0_100 ... (10),
  GDS_ACCESS1..4_ACCESS, GDS_ACCESS1..4_COUNT,
  GDS_FASTEST_RANK, GDS_FASTEST_RANK_BYTES,
  GDS_SLOWEST_RANK, GDS_SLOWEST_RANK_BYTES, GDS_NUM_INDICES`
* `GDS_F_COUNTERS` (double): open/read/write/close start+end timestamps
  (8), `READ_TIME, WRITE_TIME, META_TIME, MAX_READ_TIME, MAX_WRITE_TIME,
  FASTEST_RANK_TIME, SLOWEST_RANK_TIME, GDS_F_NUM_INDICES`.
  ("open" = HandleRegister, "close" = HandleDeregister.)
* `struct darshan_gds_file { struct darshan_base_record base_rec;
  int64_t counters[...]; double fcounters[...]; };`. No extra fields.

Edit `include/darshan-log-format.h`: `#include` the new header after DAOS,
and append `X(DARSHAN_GDS_MOD, "GDS", DARSHAN_GDS_VER,
&gds_logutils)` after the DAOS line. Because the darshan-util logutils
table always references `&gds_logutils`, the util side is built
unconditionally (same as DFS/DAOS).

## 4. Phase 2: runtime module (`darshan-runtime/lib/darshan-gds.c`)

Start from a copy of darshan-null.c (rename null->gds and
`foo`->the real wrappers). Add the statistics and reduction pieces from
DFS/POSIX as listed below. Rules learned from darshan-null.c:

* Every wrapper declares `int __darshan_disabled;`, calls `MAP_OR_FAIL`
  (which maps the symbol and sets `__darshan_disabled = darshan_core_disabled_instrumentation()`),
  then samples `tm1 = GDS_WTIME()` (which reads `__darshan_disabled`), calls
  the real function, and samples `tm2 = GDS_WTIME()`. That order matches
  `foo()` in darshan-null.c.
* `GDS_PRE_RECORD()` must gate **only on `__darshan_disabled`**, like
  NULL_PRE_RECORD. DFS's version also checks `!ret`. That's wrong here:
  `cuFileRead` returns a positive byte count and `CUfileError_t` is a
  struct. Each RECORD macro checks success itself (`ret >= 0`, or
  `ret.err == 0`).
* NULL_PRE_RECORD's `return(ret);` doesn't work for the `void`
  `cuFileHandleDeregister`. Either add a `GDS_PRE_RECORD_VOID()`
  variant, or write that wrapper's lock/init/disabled check inline.
* Keep the mutex `PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP`.
* **Lock ordering & safety:** In `cuFileHandleRegister`, verify that
  `ret.err == DARSHAN_CU_FILE_SUCCESS`, `descr != NULL`,
  `descr->type == CU_FILE_HANDLE_TYPE_OPAQUE_FD`, and `fh && *fh` before
  dereferencing `descr->handle.fd`. Call `darshan_posix_lookup_record_name()`
  (which takes the POSIX lock) and the `readlink` fallback *before*
  `GDS_PRE_RECORD()`, passing the resulting name into the record macro.
  Then the two module locks are never held at once. In compat mode,
  libcufile's internal `pread` takes the POSIX lock from inside our wrapper,
  but that happens in the real call, outside the GDS lock, so it's fine.
* Keep null's `track_new_record` order: add the hash ref, then
  `darshan_core_register_record`, and on failure delete the ref and free it.
  Also add the handle->rec_ref mapping to `handle_hash` (non-owning; when
  cleaning up, free only through `rec_id_hash`, as DFS does with
  `file_obj_hash`). To avoid uthash corruption if handle pointer values
  are recycled, check `handle_hash` for `*fh` first and call
  `darshan_delete_record_ref` if an existing entry is found before re-adding.

Pieces taken from darshan-dfs.c:
* `DARSHAN_FORWARD_DECL` for the 6 wrapped functions, with the exact
  signatures from cufile.h:
  - `CUfileError_t cuFileHandleRegister(CUfileHandle_t *fh, CUfileDescr_t *descr)`
  - `void cuFileHandleDeregister(CUfileHandle_t fh)`
  - `ssize_t cuFileRead(CUfileHandle_t fh, void *bufPtr_base, size_t size, off_t file_offset, off_t bufPtr_offset)`
  - `ssize_t cuFileWrite(CUfileHandle_t fh, const void *bufPtr_base, size_t size, off_t file_offset, off_t bufPtr_offset)`
  - `CUfileError_t cuFileBatchIOSubmit(CUfileBatchHandle_t batch_idp, unsigned nr, CUfileIOParams_t *iocbp, unsigned int flags)`
  - `CUfileError_t cuFileReadAsync(CUfileHandle_t fh, void *bufPtr_base, size_t *size_p, off_t *file_offset_p, off_t *bufPtr_offset_p, ssize_t *bytes_read_p, CUstream stream)`
  - `CUfileError_t cuFileWriteAsync(...same, bytes_written_p...)`
* Runtime struct: `rec_id_hash`, `handle_hash` (key = the `CUfileHandle_t`
  value, `sizeof(void*)`), `file_rec_count`, `frozen`.
* rec_ref struct: `file_rec`, `last_byte_read`, `last_byte_written`,
  `last_io_type`, `last_meta_end`, `last_read_end`, `last_write_end`,
  `access_root`, `access_count`. These are what POSIX uses for seq/consec
  and common-access tracking (`darshan_common_val_counter`,
  `DARSHAN_BUCKET_INC`, `DARSHAN_TIMER_INC_NO_OVERLAP`, all in
  darshan-common.h).
* Lazy init on first wrapper call (`gds_runtime_initialize`), calling
  `darshan_core_register_module(DARSHAN_GDS_MOD, ...)` with redux
  (MPI builds only), output, and cleanup callbacks.
* Reduction of shared records: copy DFS's `dfs_record_reduction_op` and
  `dfs_shared_record_variance`, renaming the counters. Sum the counters,
  min/max the timestamps, use the common-access merge logic, and fill in
  fastest/slowest rank.
* Optional and cheap: register a heatmap (`heatmap_register("heatmap:GDS")`)
  and call `heatmap_update` in the sync read/write paths, as POSIX does at
  darshan-posix.c:300. (Note: if enabled, its record ID hash
  `darshan_core_gen_record_id("heatmap:GDS")` must also be added to
  pydarshan's `_nrecs_heatmap` in `darshan/report.py`.) Implementer: include
  it if it's a few lines; otherwise skip it.
* No DXT in v1.

## 5. Phase 3: build system (runtime)

* `darshan-runtime/configure.ac`: add `--enable-gds-mod` (default no) and
  `--with-cuda=DIR`, next to the DAOS block. Do the `cufile.h` compile
  check from §2 (it defines `HAVE_CUFILE_H` in darshan-runtime-config.h).
  As with DAOS, `--with-cuda` alone may imply `--enable-gds-mod`. Add `AM_CONDITIONAL(BUILD_GDS_MODULE, ...)`,
  `AC_SUBST(ENABLE_GDS_MOD)`, and a line in the summary printout
  (~line 1130).
* `darshan-runtime/lib/Makefile.am`: `if BUILD_GDS_MODULE` ->
  `C_SRCS += darshan-gds.c`, `AM_CPPFLAGS += -DDARSHAN_GDS`. Add
  `darshan-gds.c` and `darshan-gds-types.h` to `EXTRA_DIST`/the
  noinst list where DFS appears (~line 136).
* Static linking: `darshan-runtime/share/ld-opts/darshan-gds-ld-opts`
  with one `--wrap=<fn>` line per wrapped function. Hook it into
  `share/ld-opts/Makefile.am` the way DFS is (lines 32 and 64).
* Check `darshan-runtime/share/darshan-runtime.pc.in` / `darshan-config.in`
  for per-module flags (DAOS appears there via `DAOS_PATH`). The GDS
  module needs no extra libs.
* `darshan-config.c` / runtime config: module enable/disable by name should
  work generically. Verify that `MOD_DISABLE GDS` works with
  `DARSHAN_CONFIG_PATH`.

## 6. Phase 4: darshan-util + pydarshan

* `darshan-util/darshan-gds-logutils.{c,h}`, copied from DFS logutils:
  `gds_counter_names[]`, `gds_f_counter_names[]`, get_record (with
  byte swap), put_record, print_record, print_description, print_diff,
  agg_records (accumulator), **`log_sizeof_record` and `log_record_metrics`**.
  The null logutils lacks the last two, so don't use it as the template. Add them to `darshan-util/Makefile.am` (lines
  25, 41, 55 pattern), and include the .h in `darshan-logutils.h` (~line 187).
* `darshan-util/darshan-parser.c`: add GDS everywhere DFS is
  special-cased (lines ~55, 334, 469, 487, 659) so `--total` / `--file`
  modes support it.
* pydarshan:
  - `darshan/backend/api_def_c.py`: add `struct darshan_gds_file` cdef.
  - `darshan/backend/cffi_backend.py`: append `"GDS"` to `_mod_names`
    (preserving index alignment with `DARSHAN_MODULE_IDS`), map
    `"GDS": "struct darshan_gds_file **"` in `_structdefs`. Note that since
    `struct darshan_gds_file` contains only `base_rec`, `counters`, and
    `fcounters` (no trailing UUIDs or custom structs like DFS/DAOS), it
    automatically uses the default `rec_arr` branch
    (`if mod_name not in ('DFS', 'DAOS')`).
  - `darshan/report.py`: add to the module lists where DFS appears. If
    heatmap tracking was implemented, add the `"heatmap:GDS"` record ID hash to
    `_nrecs_heatmap`.
  - Summary report / plots (`cli/summary.py`, `experimental/plots/*`): v1
    should only make sure GDS records don't crash anything. Add opcount
    and IO-cost plot support only if it's a trivial extension of the DFS
    entries.
* Docs: `darshan-util/doc/darshan-util.rst`, a new "GDS (GPUDirect
  Storage) module" counter table after DFS (~line 796).
  `darshan-runtime/doc/*.rst`: document `--enable-gds-mod` and the
  POSIX layering note from §1.

## 7. Phase 5: testing (no GPU available)

1. **Fake libcufile (primary, works here and in CI).**
   `darshan-test/gds/fake-cufile.c` builds `libfakecufile.so`, which
   exports the 6 wrapped symbols plus `cuFileDriverOpen/Close`. Handle =
   malloc'd struct holding the fd. Read/Write = `pread`/`pwrite` into host
   memory. BatchIOSubmit performs the ops synchronously. Async does the I/O
   immediately and writes `*bytes_p`. It uses only `darshan-gds-types.h`
   types, so no CUDA is needed.
   Test program `darshan-test/gds/gds-test.c`: open with O_DIRECT
   (fall back to plain open if the fs rejects it), register, do N sync
   writes and reads of known sizes, one batch of mixed ops, one async op,
   deregister. Run it with
   `LD_PRELOAD=libdarshan.so:libfakecufile.so DARSHAN_ENABLE_NONMPI=1`.
   Parse the log with `darshan-parser` and assert on exact counter values
   (reads, writes, bytes, batch counts, histogram buckets), and check that
   the GDS record ID equals the POSIX record ID for the file.
   Wire it into `darshan-runtime/test/` or `darshan-test/automated/` as a
   script, run only when the module is enabled.
2. **Real libcufile 1.9.1 (manual, best effort).** Build the same test
   program against `-lcufile`. Write a cufile.json with
   `"properties": {"allow_compat_mode": true}` and point
   `CUFILE_ENV_PATH_JSON` at it. Call `cuFileDriverOpen()` first, and use
   host (malloc'd) buffers. libcufile dlopens `libcuda.so.1`, which isn't
   installed here, so driver open will probably fail with a CUDA driver
   error. Run it once anyway and record the result. Even if it fails, it
   confirms that the wrappers call through to the real library and return
   its error code unchanged (`cuFileHandleRegister` should still be called
   and should return an error with no record created). Don't let this step
   block v1.
3. pydarshan: add a test that loads a GDS log produced by test 1
   (commit a small log into the darshan-logs test-data repo, or generate it
   in the test if that's the convention). Check that `report.records["GDS"]`
   to_df() works and the summary report renders.
4. **Build both type paths.** (a) Plain `--enable-gds-mod`: the private
   types are used on this box. (b) `--enable-gds-mod --with-cuda=<dir>`,
   where `<dir>/include/cuda.h` is the stub from §2 and `/usr/include`
   supplies the real `cufile.h`. Configure should report `cufile.h`, and
   the module must compile warning-free with `-Wall`. Run test 1 against
   both builds.
5. Run the existing darshan-util and pydarshan test suites to confirm
   nothing breaks when the module is absent from a log.

## 8. Open questions for reviewer

1. ~~Name~~: **decided: GDS.**
2. ~~Types~~: **decided: real cufile.h if it compiles, private types otherwise.**
3. Buffer registration: worth tracking? One cheap, useful diagnostic would
   be to wrap `cuFileBufRegister/Deregister`, keep a small interval set of
   registered device ranges, and per I/O count
   `GDS_REGISTERED_BUF_OPS` vs unregistered ones. Unregistered buffers
   go through bounce buffers, a common performance problem. Proposed for v2.
4. Async/batch completion tracking (real bytes, real latency) would need
   `cuLaunchHostFunc` callbacks or tracking `cuFileBatchIOGetStatus`
   cookies. That adds a CUDA driver dependency and perturbs streams.
   Proposed: out of scope.
5. Job-level info such as `cuFileDriverGetProperties` (compat mode vs
   nvidia-fs, max direct IO size) doesn't fit per-file records. It could go
   in the job's metadata key/value string later.
6. DXT tracing for cuFile: out of scope for v1.
