.PHONY: clean
BINS = darshan-tester darshan-tester-mpi
OBJS = darshan-lustre.o darshan-core-stub.o
CFLAGS = -O0 -g -I../.. -I../../darshan-runtime

### Include -I. when building non-MPI tests to include the mpi.h stub header
CFLAGS += -I.

darshan-tester: $(OBJS)
	$(CC) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

darshan-tester-mpi: $(OBJS)
	$(CC) $(LDFLAGS) $^ $(LOADLIBES) $(LDLIBS) -o $@

darshan-lustre.o: ../../darshan-runtime/lib/darshan-lustre.c
	$(CC) $(CPPFLAGS) $(CFLAGS) -c $? -o $@

clean:
	-@rm -v $(OBJS) $(BINS)
