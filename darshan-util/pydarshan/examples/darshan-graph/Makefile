CC=mpicc
CLFAGS=-g


all:
	${CC} -o app_write -DWRITE app.c
	${CC} -o app_read -DREAD app.c
	${CC} -o app_readAB_writeC app_rw_mpiio.c

run: clean all
	./run.sh

clean:
	rm -f ${USER}_*.darshan
	rm -rf *.o
	rm -f app_read
	rm -f app_write
	rm -f app_readAB_writeC
	rm -f A B C Z
