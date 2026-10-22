# Makefile - Part 2 Case 2: Fault-Tolerant with Session Management
# IT305 - Computer Networks Lab Project
# Author: Anuj Madhani (202401017)

CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LDFLAGS = -lpthread

COMMON_SRC = protocol.c topic_map.c session.c
COMMON_HDR = protocol.h topic_map.h session.h

.PHONY: all clean

all: server client

server: server.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o $@ server.c $(COMMON_SRC) $(LDFLAGS)

client: client.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o $@ client.c $(COMMON_SRC) $(LDFLAGS)

clean:
	rm -f server client *.o
