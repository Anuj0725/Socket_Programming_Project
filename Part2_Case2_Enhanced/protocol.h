/*
 * protocol.h - Binary protocol definition (Part 2 Case 2 Enhanced)
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Extends Case 2 with parallel connections:
 *   PARALLEL_INIT (0x20), FILE_ASSIGNMENT (0x21)
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>

#define HEADER_SIZE       9
#define CHUNK_SIZE        16384 /* Enhanced chunk size (16 KB) for higher throughput */
#define MAX_PAYLOAD       (4u * 1024 * 1024) /* 4 MB cap */
#define MAX_TOPIC_LEN     64
#define MAX_FILENAME_LEN  256
#define MAX_PATH_LEN      512
#define MAX_KEY_LEN       64
#define MAX_TOPICS        256     
#define MAX_FILES         65536   
#define SESSION_ID_LEN    36      /* UUID string length */
#define DEFAULT_PARALLEL_CONNS 4

/* Part 1 message types */
#define MSG_TOPIC_REQUEST       0x01
#define MSG_FILE_HEADER         0x02
#define MSG_FILE_DATA           0x03
#define MSG_TRANSFER_COMPLETE   0x04
#define MSG_ERROR_RESPONSE      0x05
#define MSG_TOPIC_LIST          0x06

/* Part 2 Case 2: Session management */
#define MSG_SESSION_INIT        0x10
#define MSG_SESSION_ACK         0x11
#define MSG_CHECKPOINT          0x12
#define MSG_RESUME_REQUEST      0x13

/* Part 2 Case 2 Enhanced: Parallel connections */
#define MSG_PARALLEL_INIT       0x20
#define MSG_FILE_ASSIGNMENT     0x21

/* Status codes */
#define STATUS_OK               0x00
#define STATUS_ERROR            0x01
#define STATUS_END_OF_TRANSFER  0x02

/* Message header (9 bytes on wire) */
typedef struct __attribute__((packed)) {
    uint8_t  msg_type;
    uint32_t payload_length;
    uint8_t  status_code;
    uint8_t  reserved[3];
} MessageHeader;

int send_all(int fd, const void *buf, size_t n);
int recv_all(int fd, void *buf, size_t n);
int send_message(int fd, uint8_t msg_type, uint8_t status,
                 const void *payload, uint32_t payload_len);
int recv_message(int fd, MessageHeader *hdr, uint8_t **payload, uint32_t *payload_len);
int set_tcp_nodelay(int fd);

#endif /* PROTOCOL_H */
