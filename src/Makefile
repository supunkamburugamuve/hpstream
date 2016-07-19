CC=g++
CFLAGS=-Wall -g -ggdb -std=gnu++11
LDFLAGS=-libverbs -lrt -pthread -L/N/u/skamburu/projects/fabbuild/fab/lib -lfabric -shared
INC=-I/N/u/skamburu/projects/fabbuild/fab/include

hpstream: buffer.o
    $(CC) $(CFLAGS) buffer.o -o hpstream $(LDFLAGS)

buffer.o: buffer.cpp
	${CC} ${CFLAGS} $(INC) -c utils.cpp buffer.cpp