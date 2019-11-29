CLFAGS=-g


all: abcxyz


mycpplib:
	g++ -fPIC -shared -o lib$@.so $@.cpp

abcxyz: mycpplib
	# NOTE: mpic++!
	mpic++ $(CFLAGS) abcxyz.cpp -o $@ -L. -lmycpplib


run:
	./run.sh


clean:
	rm -f *.darshan
	rm -rf *.o
	rm -f *.so*
	rm -f abcxyz
