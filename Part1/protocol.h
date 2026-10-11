/*
 * protocol.h - Binary protocol definition for topic-based file distribution
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * All messages use a uniform 9-byte binary header followed by variable-length payload.
 * Multi-byte integers are in network byte order (big-endian).
 */

#ifndef PROTOCOL_H
#define PROTOCOL_H

#include <stdint.h>
#include <stdlib.h>

/* ============================================================
 * Message header constants
 * ============================================================ */
#define HEADER_SIZE       9       /* Fixed 9-byte binary header */
#define CHUNK_SIZE        8192    /* 8 KB data chunks           */
#define MAX_PAYLOAD       (4u * 1024 * 1024) /* 4 MB maximum payload cap */
#define MAX_TOPIC_LEN     64
#define MAX_FILENAME_LEN  256
#define MAX_PATH_LEN      512
#define MAX_KEY_LEN       64
#define MAX_TOPICS        256     /* Increased to prevent silent truncation */
#define MAX_FILES         65536   /* Increased to handle large topics */
#define SESSION_ID_LEN    36

/* ============================================================
 * Message types (matching design document Section 3.1.2)
 * ============================================================ */
#define MSG_TOPIC_REQUEST       0x01  /* Client -> Server: GET <topic>            */
#define MSG_FILE_HEADER         0x02  /* Server -> Client: file metadata          */
#define MSG_FILE_DATA           0x03  /* Server -> Client: file data chunk        */
#define MSG_TRANSFER_COMPLETE   0x04  /* Server -> Client: all files sent         */
#define MSG_ERROR_RESPONSE      0x05  /* Server -> Client: error                  */
#define MSG_TOPIC_LIST          0x06  /* Server -> Client: available topics       */
#define MSG_SESSION_INIT        0x10  /* Client -> Server: init session           */
#define MSG_SESSION_ACK         0x11  /* Server -> Client: session acknowledged   */
#define MSG_CHECKPOINT          0x12  /* Server -> Client: checkpoint notification*/
#define MSG_RESUME_REQUEST      0x13  /* Client -> Server: resume session         */
#define MSG_PARALLEL_INIT       0x20  /* Client -> Server: parallel group init    */
#define MSG_FILE_ASSIGNMENT     0x21  /* Server -> Client: parallel file assign   */

/* Status codes */
#define STATUS_OK               0x00
#define STATUS_ERROR            0x01
#define STATUS_END_OF_TRANSFER  0x02

/* ============================================================
 * Message header structure (9 bytes on the wire)
 * ============================================================
 * Offset  Size    Field
 * 0       1       msg_type       (uint8)
 * 1       4       payload_length (uint32, network byte order)
 * 5       1       status_code    (uint8)
 * 6       3       reserved       (zero-filled)
 */
typedef struct __attribute__((packed)) {
    uint8_t  msg_type;
    uint32_t payload_length;  /* network byte order */
    uint8_t  status_code;
    uint8_t  reserved[3];
} MessageHeader;

/* ============================================================
 * Protocol I/O functions
 * ============================================================ */

/*
 * Send exactly `n` bytes from `buf` on socket `fd`.
 * Returns 0 on success, -1 on error/disconnect.
 */
int send_all(int fd, const void *buf, size_t n);

/*
 * Receive exactly `n` bytes into `buf` from socket `fd`.
 * Returns 0 on success, -1 on error/disconnect.
 */
int recv_all(int fd, void *buf, size_t n);

/*
 * Send a complete message: header + payload.
 * `msg_type`: one of MSG_* constants
 * `status`:   one of STATUS_* constants
 * `payload`:  pointer to payload data (can be NULL if payload_len == 0)
 * `payload_len`: length of payload
 * Returns 0 on success, -1 on error.
 */
int send_message(int fd, uint8_t msg_type, uint8_t status,
                 const void *payload, uint32_t payload_len);

/*
 * Receive a complete message: reads header, allocates and reads payload.
 * On success, fills in `hdr` and sets `*payload` to allocated buffer
 * (caller must free), `*payload_len` to length.
 * Returns 0 on success, -1 on error/disconnect.
 */
int recv_message(int fd, MessageHeader *hdr, uint8_t **payload, uint32_t *payload_len);
int set_tcp_nodelay(int fd);

#endif /* PROTOCOL_H */
