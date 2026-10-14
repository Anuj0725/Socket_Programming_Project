# Makefile - Part 1: Basic Topic-Based File Distribution
# IT305 - Computer Networks Lab Project
# Author: Anuj Madhani (202401017)
#
# Usage:
#   make          - Build both server and client
#   make clean    - Remove build artifacts

CC = gcc
CFLAGS = -Wall -Wextra -O2 -g
LDFLAGS = -lpthread

COMMON_SRC = protocol.c topic_map.c
COMMON_HDR = protocol.h topic_map.h

.PHONY: all clean

all: server client

server: server.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o server server.c $(COMMON_SRC) $(LDFLAGS)

client: client.c $(COMMON_SRC) $(COMMON_HDR)
	$(CC) $(CFLAGS) -o client client.c $(COMMON_SRC) $(LDFLAGS)

clean:
	rm -f server client *.o
