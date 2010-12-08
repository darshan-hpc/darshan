all: libdarshan-posix.a libdarshan-mpi-io.a libdarshan-preload.so

MPICC=mpicc

darshan-mpi-io.o: darshan-mpi-io.c 
	$(MPICC) $(CFLAGS) -c $< -o $@

darshan-posix.o: darshan-posix.c 
	$(CC) $(CFLAGS) -c $< -o $@

darshan-posix-preload.o: darshan-posix.c 
	$(CC) $(CFLAGS) -DDARSHAN_PRELOAD -c $< -o $@

libdarshan-mpi-io.a: darshan-mpi-io.o 
	ar rcs $@ $^

libdarshan-posix.a: darshan-posix.o 
	ar rcs $@ $^

libdarshan-preload.so: darshan-mpi-io.o darshan-posix-preload.o
	$(MPICC) $(CFLAGS) -fpic -DPIC -shared -ldl -o $@ $^

clean::
	rm -f *.o *.a *.so 

