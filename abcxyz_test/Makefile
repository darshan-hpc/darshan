
CC=mpicc
CLFAGS=-g

all: abcxyz


libabcxyz:
	${CC} ${CLFAGS} -fPIC -c -Wall $@.c
	${CC} -shared -Wl,-soname,$@.so.1 -o $@.so.1.0.1 $@.o
	ln -s $@.so.1.0.1 $@.so
	ln -s $@.so.1.0.1 $@.so.1


abcxyz: libabcxyz
	${CC} ${CFLAGS} -o $@ $@.c -L. -l$@


run:
	./run.sh



clean:
	rm -f *.darshan
	rm -rf *.o
	rm -f *.so*
	rm -f abcxyz

