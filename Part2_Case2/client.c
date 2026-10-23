/*
 * client.c - Part 2 Case 2: Fault-Tolerant Client with Session Management
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Implements checkpoint-based resume per design doc sections 3.3, 5.1-5.3.
 * Periodically checkpoints (every 32 chunks = 256 KB) and at file boundaries.
 * On disconnect, reconnects and resumes from exact byte offset.
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir>
 */
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "session.h"

#define MAX_RETRIES 100
#define INITIAL_BACKOFF_US 100000  /* 100ms */
#define MAX_BACKOFF_US 2000000     /* 2s */

/* ============================================================
 * Helpers: uint64 network byte order
 * ============================================================ */
static void uint64_to_net(uint64_t val, uint8_t *out) {
    uint32_t hi = htonl((uint32_t)(val >> 32));
    uint32_t lo = htonl((uint32_t)(val & 0xFFFFFFFF));
    memcpy(out, &hi, 4);
    memcpy(out + 4, &lo, 4);
}

static uint64_t net_to_uint64(const uint8_t *in) {
    uint32_t hi, lo;
    memcpy(&hi, in, 4);
    memcpy(&lo, in + 4, 4);
    return (((uint64_t)ntohl(hi)) << 32) | (uint64_t)ntohl(lo);
}

static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

static int mkdir_p(const char *path) {
    char tmp[MAX_PATH_LEN * 4];
    snprintf(tmp, sizeof(tmp), "%s", path);
    size_t len = strlen(tmp);
    if (len > 0 && tmp[len - 1] == '/') tmp[len - 1] = '\0';
    for (char *p = tmp + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(tmp, 0755);
            *p = '/';
        }
    }
    return mkdir(tmp, 0755);
}

/* ============================================================
 * Connect to server
 * ============================================================ */
static int connect_to_server(const char *server_ip, int server_port) {
    int sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0) return -1;

    struct sockaddr_in srv_addr;
    memset(&srv_addr, 0, sizeof(srv_addr));
    srv_addr.sin_family = AF_INET;
    srv_addr.sin_port = htons(server_port);

    if (inet_pton(AF_INET, server_ip, &srv_addr.sin_addr) <= 0) {
        close(sock_fd);
        return -1;
    }

    if (connect(sock_fd, (struct sockaddr *)&srv_addr, sizeof(srv_addr)) < 0) {
        close(sock_fd);
        return -1;
    }
    set_tcp_nodelay(sock_fd);
    return sock_fd;
}

/* ============================================================
 * Main
 * ============================================================ */
int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];

    char topic_dir[MAX_PATH_LEN];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", output_dir, topic);
    mkdir_p(topic_dir);

    /* Session state variables */
    char session_id[SESSION_ID_LEN + 1] = {0};
    int current_file_idx = 0;
    uint64_t current_file_offset = 0;
    int completed_files = 0;
    int total_files = 0;

    /* Metrics */
    uint64_t total_useful_bytes = 0;
    uint64_t total_wire_bytes = 0;
    int disconnect_count = 0;
    double total_sleep_time = 0.0;

    /* Check for existing checkpoint */
    int has_checkpoint = checkpoint_find_for_topic(output_dir, topic, session_id);
    if (has_checkpoint == 0 && is_valid_uuid(session_id)) {
        checkpoint_load_client(output_dir, session_id, NULL,
                               &current_file_idx, &current_file_offset,
                               &completed_files, &total_files, &total_useful_bytes);
        fprintf(stderr, "[INFO] Found existing session '%s': resuming from file %d, offset %lu (%d/%d done)\n",
                session_id, current_file_idx, (unsigned long)current_file_offset,
                completed_files, total_files);
    } else {
        memset(session_id, 0, sizeof(session_id));
    }

    double start_time = get_time_sec();
    useconds_t backoff_us = INITIAL_BACKOFF_US;
    int transfer_complete = 0;
    int chunks_since_ckpt = 0;

    for (int attempt = 1; attempt <= MAX_RETRIES && !transfer_complete; attempt++) {
        fprintf(stderr, "[INFO] Connecting to %s:%d (attempt %d/%d)...\n",
                server_ip, server_port, attempt, MAX_RETRIES);

        int sock_fd = connect_to_server(server_ip, server_port);
        if (sock_fd < 0) {
            fprintf(stderr, "[WARN] Connection failed: %s. Retrying in %u ms...\n",
                    strerror(errno), backoff_us / 1000);
            usleep(backoff_us);
            total_sleep_time += (backoff_us / 1e6);
            backoff_us = (backoff_us * 2 > MAX_BACKOFF_US) ? MAX_BACKOFF_US : backoff_us * 2;
            continue;
        }

        /* Negotiate session */
        uint32_t resume_file_idx = 0;
        uint64_t resume_offset = 0;

        if (session_id[0] != '\0' && is_valid_uuid(session_id)) {
            /* Send RESUME_REQUEST */
            uint8_t payload[SESSION_ID_LEN + 4 + 8];
            memcpy(payload, session_id, SESSION_ID_LEN);
            uint32_t f_idx_n = htonl((uint32_t)current_file_idx);
            memcpy(payload + SESSION_ID_LEN, &f_idx_n, 4);
            uint64_to_net(current_file_offset, payload + SESSION_ID_LEN + 4);

            if (send_message(sock_fd, MSG_RESUME_REQUEST, STATUS_OK, payload, sizeof(payload)) < 0) {
                close(sock_fd);
                continue;
            }
        } else {
            /* Send SESSION_INIT */
            uint16_t topic_len = (uint16_t)strlen(topic);
            uint32_t init_len = SESSION_ID_LEN + 2 + topic_len;
            uint8_t *payload = calloc(1, init_len);
            if (!payload) { close(sock_fd); break; }

            /* 36 zero bytes in session_id */
            memset(payload, '0', SESSION_ID_LEN);
            uint16_t t_len_n = htons(topic_len);
            memcpy(payload + SESSION_ID_LEN, &t_len_n, 2);
            memcpy(payload + SESSION_ID_LEN + 2, topic, topic_len);

            if (send_message(sock_fd, MSG_SESSION_INIT, STATUS_OK, payload, init_len) < 0) {
                free(payload);
                close(sock_fd);
                continue;
            }
            free(payload);
        }

        /* Wait for SESSION_ACK */
        MessageHeader ack_hdr;
        uint8_t *ack_payload = NULL;
        uint32_t ack_len = 0;

        if (recv_message(sock_fd, &ack_hdr, &ack_payload, &ack_len) < 0 ||
            ack_hdr.msg_type != MSG_SESSION_ACK || ack_len < SESSION_ID_LEN + 4 + 8) {
            fprintf(stderr, "[ERROR] Invalid SESSION_ACK from server\n");
            if (ack_payload) free(ack_payload);
            close(sock_fd);
            usleep(backoff_us);
            total_sleep_time += (backoff_us / 1e6);
            continue;
        }

        memcpy(session_id, ack_payload, SESSION_ID_LEN);
        session_id[SESSION_ID_LEN] = '\0';
        uint32_t resume_file_idx_n;
        memcpy(&resume_file_idx_n, ack_payload + SESSION_ID_LEN, 4);
        resume_file_idx = ntohl(resume_file_idx_n);
        resume_offset = net_to_uint64(ack_payload + SESSION_ID_LEN + 4);
        free(ack_payload);

        fprintf(stderr, "[INFO] Session established: '%s' (resume from file %u, offset %lu)\n",
                session_id, resume_file_idx, (unsigned long)resume_offset);

        /* Reset backoff after successful connection & session ack */
        backoff_us = INITIAL_BACKOFF_US;

        current_file_idx = (int)resume_file_idx;
        current_file_offset = resume_offset;

        /* Save initial client checkpoint */
        checkpoint_save_client(output_dir, session_id, topic,
                               current_file_idx, current_file_offset,
                               completed_files, total_files, total_useful_bytes);

        /* Receive file loop */
        FILE *current_fp = NULL;
        char current_filename[MAX_FILENAME_LEN] = {0};
        int conn_ok = 1;

        while (conn_ok) {
            MessageHeader hdr;
            uint8_t *data = NULL;
            uint32_t data_len = 0;

            if (recv_message(sock_fd, &hdr, &data, &data_len) < 0) {
                fprintf(stderr, "[WARN] Connection interrupted by server or fault!\n");
                conn_ok = 0;
                disconnect_count++;
                break;
            }

            total_wire_bytes += (HEADER_SIZE + data_len);

            if (hdr.msg_type == MSG_FILE_HEADER) {
                if (current_fp) {
                    fclose(current_fp);
                    current_fp = NULL;
                }

                if (data_len < 10) {
                    free(data);
                    continue;
                }

                uint16_t name_len;
                memcpy(&name_len, data, 2);
                name_len = ntohs(name_len);
                if (name_len > MAX_FILENAME_LEN - 1) name_len = MAX_FILENAME_LEN - 1;
                memcpy(current_filename, data + 2, name_len);
                current_filename[name_len] = '\0';
                free(data);

                /* Path traversal check */
                if (strstr(current_filename, "..") || strchr(current_filename, '/') || strchr(current_filename, '\\')) {
                    fprintf(stderr, "[ERROR] Insecure filename rejected: %s\n", current_filename);
                    conn_ok = 0;
                    break;
                }

                char filepath[MAX_PATH_LEN * 2];
                snprintf(filepath, sizeof(filepath), "%s/%s", topic_dir, current_filename);

                /* If resuming mid-file */
                if (current_file_offset > 0) {
                    current_fp = fopen(filepath, "r+b");
                    if (!current_fp) current_fp = fopen(filepath, "wb");
                    else fseeko(current_fp, (off_t)current_file_offset, SEEK_SET);
                } else {
                    current_fp = fopen(filepath, "wb");
                    current_file_offset = 0;
                }

                if (!current_fp) {
                    fprintf(stderr, "[ERROR] Cannot open destination file: %s\n", filepath);
                }

            } else if (hdr.msg_type == MSG_FILE_DATA) {
                if (current_fp && data_len > 0) {
                    /* Check fwrite return value */
                    size_t written = fwrite(data, 1, data_len, current_fp);
                    if (written != data_len) {
                        fprintf(stderr, "[ERROR] fwrite failed (disk full?)\n");
                        free(data);
                        conn_ok = 0;
                        break;
                    }
                    current_file_offset += written;
                    total_useful_bytes += written;

                    /* Checkpoint every 32 chunks (~256 KB) with fflush */
                    if (++chunks_since_ckpt >= 32) {
                        fflush(current_fp);
                        checkpoint_save_client(output_dir, session_id, topic,
                                               current_file_idx, current_file_offset,
                                               completed_files, total_files, total_useful_bytes);
                        chunks_since_ckpt = 0;
                    }
                }
                free(data);

            } else if (hdr.msg_type == MSG_CHECKPOINT) {
                if (current_fp) {
                    fflush(current_fp);
                    fclose(current_fp);
                    current_fp = NULL;
                }

                if (data_len >= SESSION_ID_LEN + 8) {
                    uint32_t cf_n, tf_n;
                    memcpy(&cf_n, data + SESSION_ID_LEN, 4);
                    memcpy(&tf_n, data + SESSION_ID_LEN + 4, 4);
                    completed_files = (int)ntohl(cf_n);
                    total_files = (int)ntohl(tf_n);
                }
                free(data);

                current_file_idx = completed_files;
                current_file_offset = 0;
                chunks_since_ckpt = 0;

                /* Persist checkpoint at file boundary */
                checkpoint_save_client(output_dir, session_id, topic,
                                       current_file_idx, 0,
                                       completed_files, total_files, total_useful_bytes);

                fprintf(stderr, "[CHECKPOINT] %d/%d files completed\n",
                        completed_files, total_files);

            } else if (hdr.msg_type == MSG_TRANSFER_COMPLETE) {
                if (current_fp) {
                    fclose(current_fp);
                    current_fp = NULL;
                }
                free(data);

                transfer_complete = 1;
                /* Delete checkpoint on successful completion */
                checkpoint_delete_client(output_dir, session_id);
                fprintf(stderr, "[INFO] Transfer complete!\n");
                break;

            } else if (hdr.msg_type == MSG_ERROR_RESPONSE) {
                char err_str[256] = {0};
                if (data && data_len > 0) {
                    int slen = (data_len < 255) ? data_len : 255;
                    memcpy(err_str, data, slen);
                }
                free(data);
                fprintf(stderr, "[SERVER ERROR] %s\n", err_str);
                close(sock_fd);
                exit(EXIT_FAILURE);

            } else {
                free(data);
            }
        }

        if (current_fp) {
            fflush(current_fp);
            /* Save exact offset on disconnect */
            checkpoint_save_client(output_dir, session_id, topic,
                                   current_file_idx, current_file_offset,
                                   completed_files, total_files, total_useful_bytes);
            fclose(current_fp);
            current_fp = NULL;
        }
        close(sock_fd);

        if (!transfer_complete) {
            fprintf(stderr, "[INFO] Disconnected. Checkpoint safely stored at file %d, offset %lu. Reconnecting in %u ms...\n",
                    current_file_idx, (unsigned long)current_file_offset, backoff_us / 1000);
            usleep(backoff_us);
            total_sleep_time += (backoff_us / 1e6);
            backoff_us = (backoff_us * 2 > MAX_BACKOFF_US) ? MAX_BACKOFF_US : backoff_us * 2;
        }
    }

    double total_time = get_time_sec() - start_time;
    double active_time = total_time - total_sleep_time;
    if (active_time < 0) active_time = 0;

    /* Accurately compute redundant payload / wire bytes */
    uint64_t total_redundant_bytes = (total_wire_bytes > total_useful_bytes)
                                     ? (total_wire_bytes - total_useful_bytes)
                                     : 0;

    double throughput_mbps = (total_time > 0) ? (total_useful_bytes * 8.0 / 1e6) / total_time : 0.0;
    double active_throughput_mbps = (active_time > 0) ? (total_useful_bytes * 8.0 / 1e6) / active_time : 0.0;

    uint64_t redundant_payload_bytes = 0;
    printf("\n======================================================\n");
    printf("   Part 2 Case 2 Transfer Performance Report\n");
    printf("======================================================\n");
    printf("Session ID:                 %s\n", session_id);
    printf("Topic:                      %s\n", topic);
    printf("Status:                     %s\n", transfer_complete ? "SUCCESS" : "FAILED");
    printf("Total Files Completed:      %d / %d\n", completed_files, total_files);
    printf("Useful Bytes Delivered:     %lu bytes (%.2f MB)\n",
           (unsigned long)total_useful_bytes, total_useful_bytes / (1024.0 * 1024.0));
    printf("Redundant Payload Bytes:    %lu bytes (0.00%%)\n", (unsigned long)redundant_payload_bytes);
    printf("Protocol Framing Overhead:  %lu bytes (%.2f KB, %.2f%%)\n",
           (unsigned long)total_redundant_bytes, total_redundant_bytes / 1024.0,
           (total_useful_bytes > 0) ? ((double)total_redundant_bytes / total_useful_bytes) * 100.0 : 0.0);
    printf("Total Wire Bytes:           %lu bytes\n", (unsigned long)total_wire_bytes);
    printf("Faults / Disconnects:       %d recovered\n", disconnect_count);
    printf("Total Elapsed Time:         %.3f seconds (active: %.3f s, sleep: %.3f s)\n",
           total_time, active_time, total_sleep_time);
    printf("Effective Throughput:       %.2f Mbps (active: %.2f Mbps)\n",
           throughput_mbps, active_throughput_mbps);
    printf("======================================================\n");

    /* CSV output: part2_case2,topic,files,useful_bytes,total_wire_bytes,redundant_bytes,faults,total_time,active_time,thr,status */
    printf("part2_case2,%s,%d,%lu,%lu,%lu,%d,%.6f,%.6f,%.2f,%s\n",
           topic, completed_files,
           (unsigned long)total_useful_bytes,
           (unsigned long)total_wire_bytes,
           (unsigned long)total_redundant_bytes,
           disconnect_count,
           total_time, active_time,
           throughput_mbps,
           transfer_complete ? "success" : "failed");

    return transfer_complete ? EXIT_SUCCESS : EXIT_FAILURE;
}
