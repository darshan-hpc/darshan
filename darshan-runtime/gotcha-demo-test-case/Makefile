# - load hdf5 into the environment with spack (or similar)
# - set PKG_CONFIG_PATH to include manual Darshan install dir
#   - export PKG_CONFIG_PATH="/home/carns/working/install/lib/pkgconfig/:$PKG_CONFIG_PATH"
# - edit darshan-runtime.pc to remove mpich and zlib requirements if they
#   don't work
# - make
# - run ./ph5-example-<whatever> -f foo

# no pkg-config for gotcha
GOTCHA_LIBS=-L/home/carns/working/src/spack/opt/spack/linux-ubuntu21.10-skylake/gcc-11.2.0/gotcha-develop-hh6bbd66iffumta52cctin62spfptfhp/lib -lgotcha

all: ph5example-good ph5example-bad

ph5example-bad: ph5example.c
	mpicc $(CFLAGS) $(shell pkg-config hdf5 --cflags) ph5example.c -o ph5example-bad $(LDFLAGS) $(shell pkg-config hdf5 --libs) $(shell pkg-config darshan-runtime --libs) $(GOTCHA_LIBS)


ph5example-good: ph5example.c
	mpicc $(CFLAGS) $(shell pkg-config hdf5 --cflags) ph5example.c -o ph5example-good $(LDFLAGS) $(shell pkg-config darshan-runtime --libs) $(GOTCHA_LIBS) $(shell pkg-config hdf5 --libs)

clean::
	rm -f ph5example-good ph5example-bad
