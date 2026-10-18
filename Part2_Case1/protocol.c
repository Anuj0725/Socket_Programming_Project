/*
 * protocol.c - Binary protocol implementation
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 */

#include "protocol.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>

/* Send exactly n bytes */
int send_all(int fd, const void *buf, size_t n) {
    const uint8_t *p = (const uint8_t *)buf;
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t sent = send(fd, p, remaining, MSG_NOSIGNAL);
        if (sent <= 0) {
            if (sent < 0 && errno == EINTR) continue;
            return -1;
        }
        p += sent;
        remaining -= sent;
    }
    return 0;
}

/* Receive exactly n bytes */
int recv_all(int fd, void *buf, size_t n) {
    uint8_t *p = (uint8_t *)buf;
    size_t remaining = n;
    while (remaining > 0) {
        ssize_t received = recv(fd, p, remaining, 0);
        if (received <= 0) {
            if (received < 0 && errno == EINTR) continue;
            return -1;
        }
        p += received;
        remaining -= received;
    }
    return 0;
}

/* Send a complete message (header + payload) in a single send buffer */
int send_message(int fd, uint8_t msg_type, uint8_t status,
                 const void *payload, uint32_t payload_len) {
    MessageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type = msg_type;
    hdr.payload_length = htonl(payload_len);
    hdr.status_code = status;

    if (payload_len == 0 || payload == NULL) {
        return send_all(fd, &hdr, HEADER_SIZE);
    }

    size_t total_len = HEADER_SIZE + (size_t)payload_len;
    if (total_len <= 65536 + HEADER_SIZE) {
        uint8_t buf[HEADER_SIZE + 65536];
        memcpy(buf, &hdr, HEADER_SIZE);
        memcpy(buf + HEADER_SIZE, payload, payload_len);
        return send_all(fd, buf, total_len);
    } else {
        uint8_t *buf = (uint8_t *)malloc(total_len);
        if (!buf) return -1;
        memcpy(buf, &hdr, HEADER_SIZE);
        memcpy(buf + HEADER_SIZE, payload, payload_len);
        int ret = send_all(fd, buf, total_len);
        free(buf);
        return ret;
    }
}

/* Receive a complete message with MAX_PAYLOAD cap */
int recv_message(int fd, MessageHeader *hdr, uint8_t **payload, uint32_t *payload_len) {
    if (recv_all(fd, hdr, HEADER_SIZE) < 0) return -1;
    hdr->payload_length = ntohl(hdr->payload_length);

    /* Reject payload_length above limit to prevent hostile allocations */
    if (hdr->payload_length > MAX_PAYLOAD) {
        fprintf(stderr, "[ERROR] Payload length %u exceeds MAX_PAYLOAD (%u)\n",
                hdr->payload_length, (unsigned int)MAX_PAYLOAD);
        return -1;
    }

    *payload_len = hdr->payload_length;
    if (*payload_len > 0) {
        *payload = (uint8_t *)malloc(*payload_len);
        if (*payload == NULL) return -1;
        if (recv_all(fd, *payload, *payload_len) < 0) {
            free(*payload);
            *payload = NULL;
            return -1;
        }
    } else {
        *payload = NULL;
    }
    return 0;
}

/* Enable TCP_NODELAY to avoid Nagle algorithm 40ms latency on small messages */
int set_tcp_nodelay(int fd) {
    int flag = 1;
    return setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, (char *)&flag, sizeof(int));
}
