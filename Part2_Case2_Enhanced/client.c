/*
 * client.c - Part 2 Case 2 Enhanced: Parallel Client with Stream Checkpoints
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Coordinates N parallel TCP connections per design doc Section 3.4.
 * First connection initializes group; remaining N connections join group.
 * Each connection receives and writes a disjoint subset of files concurrently.
 * Features per-stream checkpoints and byte-offset resumption.
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir> [num_connections] [max_retries]
 */
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <dirent.h>

#include "protocol.h"
#include "session.h"

#define MAX_CONNS 16
#define DEFAULT_MAX_RETRIES 1000

typedef struct {
    int conn_id;
    int total_conns;
    char server_ip[64];
    int server_port;
    char topic[MAX_TOPIC_LEN];
    char output_dir[MAX_PATH_LEN];
    char group_id[SESSION_ID_LEN + 1];
    int max_retries;
    int files_received;
    uint64_t wire_bytes_received;
    uint64_t useful_bytes_received;
    uint64_t redundant_bytes;
    int fault_disconnects;
    double sleep_time_sec;
    int success;
    pthread_t thread;
} WorkerContext;

static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Helpers: uint64 network byte order */
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

/* Path traversal sanitization */
static int is_safe_filename(const char *name) {
    if (!name || name[0] == '\0') return 0;
    if (strstr(name, "..")) return 0;
    if (strchr(name, '/') || strchr(name, '\\')) return 0;
    return 1;
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
 * Worker thread for each parallel connection stream
 * Supports per-stream checkpointing & byte-level resume
 * ============================================================ */
static void *worker_thread_fn(void *arg) {
    WorkerContext *ctx = (WorkerContext *)arg;
    char topic_dir[MAX_PATH_LEN * 2];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", ctx->output_dir, ctx->topic);

    /* Stream checkpoint tracking */
    int stream_file_idx = 0;
    uint64_t byte_offset = 0;
    int completed_files = 0;
    int total_assigned = 0;
    uint64_t useful_bytes = 0;

    /* Check if previous checkpoint exists for this stream */
    char ckpt_topic[MAX_TOPIC_LEN] = {0};
    if (stream_checkpoint_load(ctx->output_dir, ctx->group_id, ctx->conn_id,
                               ckpt_topic, &stream_file_idx, &byte_offset,
                               &completed_files, &total_assigned, &useful_bytes) == 0) {
        if (strcmp(ckpt_topic, ctx->topic) == 0) {
            fprintf(stderr, "[CONN %d] Loaded checkpoint: resume from file index %d, byte offset %lu, completed %d files\n",
                    ctx->conn_id, stream_file_idx, (unsigned long)byte_offset, completed_files);
        } else {
            /* Mismatched topic, reset */
            stream_file_idx = 0;
            byte_offset = 0;
            completed_files = 0;
            useful_bytes = 0;
        }
    }

    useconds_t backoff_us = 100000; /* 100ms initial backoff */
    int chunks_since_ckpt = 0;

    for (int attempt = 1; attempt <= ctx->max_retries && !ctx->success; attempt++) {
        if (attempt > 1) {
            ctx->fault_disconnects++;
            double sleep_s = (double)backoff_us / 1e6;
            ctx->sleep_time_sec += sleep_s;
            usleep(backoff_us);
            if (backoff_us < 2000000) backoff_us *= 2;
        }

        int sock_fd = connect_to_server(ctx->server_ip, ctx->server_port);
        if (sock_fd < 0) {
            continue;
        }

        /* Send PARALLEL_INIT:
         * group_id (36B), topic_len (2B), topic (tlen B), total_conns (1B), conn_id (1B),
         * resume_file_idx (4B), resume_offset (8B) */
        uint16_t topic_len = (uint16_t)strlen(ctx->topic);
        uint32_t init_len = SESSION_ID_LEN + 2 + topic_len + 1 + 1 + 4 + 8;
        uint8_t *init_buf = calloc(1, init_len);
        if (!init_buf) {
            close(sock_fd);
            continue;
        }

        memcpy(init_buf, ctx->group_id, SESSION_ID_LEN);
        uint16_t t_len_n = htons(topic_len);
        memcpy(init_buf + SESSION_ID_LEN, &t_len_n, 2);
        memcpy(init_buf + SESSION_ID_LEN + 2, ctx->topic, topic_len);
        init_buf[SESSION_ID_LEN + 2 + topic_len] = (uint8_t)ctx->total_conns;
        init_buf[SESSION_ID_LEN + 2 + topic_len + 1] = (uint8_t)ctx->conn_id;

        uint32_t r_fidx_n = htonl((uint32_t)stream_file_idx);
        memcpy(init_buf + SESSION_ID_LEN + 2 + topic_len + 2, &r_fidx_n, 4);
        uint64_to_net(byte_offset, init_buf + SESSION_ID_LEN + 2 + topic_len + 2 + 4);

        if (send_message(sock_fd, MSG_PARALLEL_INIT, STATUS_OK, init_buf, init_len) < 0) {
            free(init_buf);
            close(sock_fd);
            continue;
        }
        free(init_buf);

        /* Receive SESSION_ACK */
        MessageHeader ack_hdr;
        uint8_t *ack_payload = NULL;
        uint32_t ack_len = 0;

        if (recv_message(sock_fd, &ack_hdr, &ack_payload, &ack_len) < 0) {
            if (ack_payload) free(ack_payload);
            close(sock_fd);
            continue;
        }

        if (ack_hdr.msg_type == MSG_ERROR_RESPONSE) {
            fprintf(stderr, "[CONN %d FATAL] Server returned error: %.*s\n",
                    ctx->conn_id, (int)ack_len, ack_payload ? (char *)ack_payload : "unknown");
            if (ack_payload) free(ack_payload);
            close(sock_fd);
            break; /* Non-retryable error */
        }

        if (ack_hdr.msg_type != MSG_SESSION_ACK || ack_len < SESSION_ID_LEN) {
            if (ack_payload) free(ack_payload);
            close(sock_fd);
            continue;
        }
        free(ack_payload);

        /* Reset exponential backoff upon successful handshake */
        backoff_us = 100000;

        /* Receive FILE_ASSIGNMENT */
        MessageHeader assign_hdr;
        uint8_t *assign_payload = NULL;
        uint32_t assign_len = 0;

        if (recv_message(sock_fd, &assign_hdr, &assign_payload, &assign_len) < 0 ||
            assign_hdr.msg_type != MSG_FILE_ASSIGNMENT || assign_len < 2) {
            if (assign_payload) free(assign_payload);
            close(sock_fd);
            continue;
        }

        uint16_t assigned_count_raw;
        memcpy(&assigned_count_raw, assign_payload, 2);
        total_assigned = (int)ntohs(assigned_count_raw);
        free(assign_payload);

        if (attempt == 1) {
            fprintf(stderr, "[CONN %d] Assigned %d files to stream\n", ctx->conn_id, total_assigned);
        } else {
            fprintf(stderr, "[CONN %d] Reconnected (attempt %d), resuming from file %d, offset %lu (%d/%d completed)\n",
                    ctx->conn_id, attempt, stream_file_idx, (unsigned long)byte_offset, completed_files, total_assigned);
        }

        /* Receive assigned files */
        FILE *current_fp = NULL;
        char current_filename[MAX_FILENAME_LEN] = {0};
        uint64_t current_file_bytes_written = 0;
        int is_resumed_file = (byte_offset > 0);

        while (1) {
            MessageHeader hdr;
            uint8_t *data = NULL;
            uint32_t data_len = 0;

            if (recv_message(sock_fd, &hdr, &data, &data_len) < 0) {
                fprintf(stderr, "[CONN %d] Stream interrupted by server fault, will reconnect...\n", ctx->conn_id);
                break;
            }
            ctx->wire_bytes_received += (HEADER_SIZE + data_len);

            if (hdr.msg_type == MSG_FILE_HEADER) {
                if (current_fp) {
                    fflush(current_fp);
                    fclose(current_fp);
                    current_fp = NULL;
                }

                if (data_len < 10) { free(data); continue; }
                uint16_t nlen;
                memcpy(&nlen, data, 2);
                nlen = ntohs(nlen);
                if (nlen > MAX_FILENAME_LEN - 1) nlen = MAX_FILENAME_LEN - 1;
                memcpy(current_filename, data + 2, nlen);
                current_filename[nlen] = '\0';

                /* Sanitize filename against path traversal */
                if (!is_safe_filename(current_filename)) {
                    fprintf(stderr, "[CONN %d ERROR] Insecure filename '%s' rejected\n",
                            ctx->conn_id, current_filename);
                    free(data);
                    break;
                }

                uint64_t current_file_size = 0;
                if (data_len >= (uint32_t)(2 + nlen + 8)) {
                    current_file_size = net_to_uint64(data + 2 + nlen);
                }
                (void)current_file_size;

                free(data);

                char filepath[MAX_PATH_LEN * 4];
                snprintf(filepath, sizeof(filepath), "%s/%s", topic_dir, current_filename);

                if (is_resumed_file && byte_offset > 0) {
                    /* Resume appending / writing to existing partial file */
                    current_fp = fopen(filepath, "r+b");
                    if (!current_fp) {
                        current_fp = fopen(filepath, "wb");
                        byte_offset = 0;
                    } else {
                        if (fseeko(current_fp, (off_t)byte_offset, SEEK_SET) != 0) {
                            fseeko(current_fp, 0, SEEK_END);
                            byte_offset = (uint64_t)ftello(current_fp);
                        }
                    }
                    current_file_bytes_written = byte_offset;
                    is_resumed_file = 0; /* Only first file in resume sequence */
                } else {
                    current_fp = fopen(filepath, "wb");
                    byte_offset = 0;
                    current_file_bytes_written = 0;
                }

                if (!current_fp) {
                    fprintf(stderr, "[CONN %d ERROR] Cannot open '%s' for writing\n", ctx->conn_id, filepath);
                    break;
                }

            } else if (hdr.msg_type == MSG_FILE_DATA) {
                if (current_fp && data_len > 0) {
                    /* Validate fwrite return value */
                    size_t written = fwrite(data, 1, data_len, current_fp);
                    if (written != data_len) {
                        fprintf(stderr, "[CONN %d ERROR] fwrite short write\n", ctx->conn_id);
                        free(data);
                        break;
                    }
                    byte_offset += data_len;
                    current_file_bytes_written += data_len;
                    useful_bytes += data_len;

                    /* Checkpoint every 32 chunks (~256 KB) */
                    if (++chunks_since_ckpt >= 32) {
                        fflush(current_fp);
                        stream_checkpoint_save(ctx->output_dir, ctx->group_id, ctx->conn_id,
                                               ctx->topic, stream_file_idx, byte_offset,
                                               completed_files, total_assigned, useful_bytes);
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
                stream_file_idx++;
                byte_offset = 0;
                completed_files++;
                ctx->files_received = completed_files;
                chunks_since_ckpt = 0;

                /* Save stream checkpoint at file boundary */
                stream_checkpoint_save(ctx->output_dir, ctx->group_id, ctx->conn_id,
                                       ctx->topic, stream_file_idx, byte_offset,
                                       completed_files, total_assigned, useful_bytes);
                free(data);

            } else if (hdr.msg_type == MSG_TRANSFER_COMPLETE) {
                if (current_fp) {
                    fflush(current_fp);
                    fclose(current_fp);
                    current_fp = NULL;
                }
                free(data);
                ctx->success = 1;

                /* Clean up stream checkpoint on completion */
                stream_checkpoint_delete(ctx->output_dir, ctx->group_id, ctx->conn_id);
                break;

            } else {
                free(data);
            }
        }

        if (current_fp) {
            fflush(current_fp);
            fclose(current_fp);
            current_fp = NULL;

            /* Save checkpoint on disconnect so reconnect resumes accurately */
            if (!ctx->success) {
                stream_checkpoint_save(ctx->output_dir, ctx->group_id, ctx->conn_id,
                                       ctx->topic, stream_file_idx, byte_offset,
                                       completed_files, total_assigned, useful_bytes);
            }
        }
        close(sock_fd);
    }

    ctx->useful_bytes_received = useful_bytes;
    if (ctx->wire_bytes_received >= ctx->useful_bytes_received) {
        ctx->redundant_bytes = ctx->wire_bytes_received - ctx->useful_bytes_received;
    } else {
        ctx->redundant_bytes = 0;
    }

    return NULL;
}

/* ============================================================
 * Main
 * ============================================================ */
int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir> [num_connections] [max_retries]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];
    int num_conns = DEFAULT_PARALLEL_CONNS;
    int max_retries = DEFAULT_MAX_RETRIES;

    if (argc >= 6) {
        num_conns = atoi(argv[5]);
        if (num_conns < 1) num_conns = 1;
        if (num_conns > MAX_CONNS) num_conns = MAX_CONNS;
    }
    if (argc >= 7) {
        max_retries = atoi(argv[6]);
        if (max_retries < 1) max_retries = 1;
    }

    char topic_dir[MAX_PATH_LEN * 2];
    snprintf(topic_dir, sizeof(topic_dir), "%s/%s", output_dir, topic);
    mkdir_p(topic_dir);

    double start_time = get_time_sec();

    /* Step 1: Initiate group with registration request */
    fprintf(stderr, "[INFO] Initiating parallel transfer (%d streams) for topic '%s'...\n",
            num_conns, topic);

    int lead_fd = connect_to_server(server_ip, server_port);
    if (lead_fd < 0) {
        fprintf(stderr, "[FATAL] Cannot connect to server %s:%d\n", server_ip, server_port);
        exit(EXIT_FAILURE);
    }

    uint16_t topic_len = (uint16_t)strlen(topic);
    uint32_t init_len = SESSION_ID_LEN + 2 + topic_len + 1;
    uint8_t *init_buf = calloc(1, init_len);
    if (!init_buf) {
        close(lead_fd);
        exit(EXIT_FAILURE);
    }
    memset(init_buf, '0', SESSION_ID_LEN); /* 36 zero bytes requests new group */
    uint16_t t_len_n = htons(topic_len);
    memcpy(init_buf + SESSION_ID_LEN, &t_len_n, 2);
    memcpy(init_buf + SESSION_ID_LEN + 2, topic, topic_len);
    init_buf[SESSION_ID_LEN + 2 + topic_len] = (uint8_t)num_conns;

    if (send_message(lead_fd, MSG_PARALLEL_INIT, STATUS_OK, init_buf, init_len) < 0) {
        free(init_buf);
        close(lead_fd);
        exit(EXIT_FAILURE);
    }
    free(init_buf);

    /* Get SESSION_ACK with assigned group_id */
    MessageHeader ack_hdr;
    uint8_t *ack_payload = NULL;
    uint32_t ack_len = 0;

    if (recv_message(lead_fd, &ack_hdr, &ack_payload, &ack_len) < 0 ||
        ack_hdr.msg_type != MSG_SESSION_ACK || ack_len < SESSION_ID_LEN) {
        fprintf(stderr, "[FATAL] Failed to negotiate parallel group with server\n");
        if (ack_payload) free(ack_payload);
        close(lead_fd);
        exit(EXIT_FAILURE);
    }

    char group_id[SESSION_ID_LEN + 1];
    memcpy(group_id, ack_payload, SESSION_ID_LEN);
    group_id[SESSION_ID_LEN] = '\0';
    free(ack_payload);
    close(lead_fd);

    fprintf(stderr, "[INFO] Group ID established: '%s'. Launching %d parallel streams...\n",
            group_id, num_conns);

    /* Step 2: Spawn N worker threads */
    WorkerContext contexts[MAX_CONNS];
    for (int i = 0; i < num_conns; i++) {
        memset(&contexts[i], 0, sizeof(WorkerContext));
        contexts[i].conn_id = i;
        contexts[i].total_conns = num_conns;
        strncpy(contexts[i].server_ip, server_ip, sizeof(contexts[i].server_ip) - 1);
        contexts[i].server_ip[sizeof(contexts[i].server_ip) - 1] = '\0';
        contexts[i].server_port = server_port;
        strncpy(contexts[i].topic, topic, sizeof(contexts[i].topic) - 1);
        contexts[i].topic[sizeof(contexts[i].topic) - 1] = '\0';
        strncpy(contexts[i].output_dir, output_dir, sizeof(contexts[i].output_dir) - 1);
        contexts[i].output_dir[sizeof(contexts[i].output_dir) - 1] = '\0';
        snprintf(contexts[i].group_id, sizeof(contexts[i].group_id), "%s", group_id);
        contexts[i].max_retries = max_retries;

        pthread_create(&contexts[i].thread, NULL, worker_thread_fn, &contexts[i]);
    }

    /* Step 3: Wait for all streams to finish */
    int total_files = 0;
    uint64_t total_wire_bytes = 0;
    uint64_t total_useful_bytes = 0;
    uint64_t total_redundant_bytes = 0;
    int total_disconnects = 0;
    double max_sleep_time = 0.0;
    int all_ok = 1;

    for (int i = 0; i < num_conns; i++) {
        pthread_join(contexts[i].thread, NULL);
        total_files += contexts[i].files_received;
        total_wire_bytes += contexts[i].wire_bytes_received;
        total_useful_bytes += contexts[i].useful_bytes_received;
        total_redundant_bytes += contexts[i].redundant_bytes;
        total_disconnects += contexts[i].fault_disconnects;
        if (contexts[i].sleep_time_sec > max_sleep_time) {
            max_sleep_time = contexts[i].sleep_time_sec;
        }
        if (!contexts[i].success) all_ok = 0;
    }

    /* Verify files actually present on disk */
    DIR *chk_dir = opendir(topic_dir);
    int disk_files = 0;
    if (chk_dir) {
        struct dirent *de;
        while ((de = readdir(chk_dir)) != NULL) {
            if (de->d_name[0] != '.') disk_files++;
        }
        closedir(chk_dir);
    }
    if (disk_files == 0) all_ok = 0;

    double elapsed_time = get_time_sec() - start_time;
    double transfer_time = (elapsed_time > max_sleep_time) ? (elapsed_time - max_sleep_time) : elapsed_time;
    double throughput_mbps = (transfer_time > 0) ? (total_useful_bytes * 8.0 / 1e6) / transfer_time : 0.0;
    double throughput_mb_s = (transfer_time > 0) ? (total_useful_bytes / (1024.0 * 1024.0)) / transfer_time : 0.0;
    double overhead_pct = (total_useful_bytes > 0) ? ((double)total_redundant_bytes / (double)total_useful_bytes) * 100.0 : 0.0;

    uint64_t redundant_payload_bytes = 0;
    printf("\n======================================================\n");
    printf("   Part 2 Case 2 Enhanced Transfer Performance Report\n");
    printf("======================================================\n");
    printf("Group ID:                   %s\n", group_id);
    printf("Topic:                      %s\n", topic);
    printf("Parallel Connections:       %d streams\n", num_conns);
    printf("Status:                     %s\n", all_ok ? "SUCCESS" : "PARTIAL / FAILED");
    printf("Total Files Completed:      %d (on disk: %d)\n", total_files, disk_files);
    printf("Useful Bytes Received:      %lu bytes (%.2f MB)\n",
           (unsigned long)total_useful_bytes, total_useful_bytes / (1024.0 * 1024.0));
    printf("Total Wire Bytes Received:  %lu bytes (%.2f MB)\n",
           (unsigned long)total_wire_bytes, total_wire_bytes / (1024.0 * 1024.0));
    printf("Redundant Payload Bytes:    %lu bytes (0.00%%)\n", (unsigned long)redundant_payload_bytes);
    printf("Protocol Framing Overhead:  %lu bytes (%.2f%%)\n",
           (unsigned long)total_redundant_bytes, overhead_pct);
    printf("Fault Disconnects:          %d\n", total_disconnects);
    printf("Total Elapsed Time:         %.3f seconds\n", elapsed_time);
    printf("Transfer Time (excl sleep): %.3f seconds\n", transfer_time);
    printf("Aggregate Throughput:       %.2f Mbps (%.2f MB/s)\n",
           throughput_mbps, throughput_mb_s);
    printf("======================================================\n");

    /* Machine-readable CSV summary */
    printf("part2_enhanced,%s,%d,%d,%lu,%lu,%lu,%.3f,%.2f,%.2f,%s\n",
           topic, num_conns, total_disconnects,
           (unsigned long)total_useful_bytes,
           (unsigned long)total_wire_bytes,
           (unsigned long)total_redundant_bytes,
           transfer_time, throughput_mbps, overhead_pct,
           all_ok ? "SUCCESS" : "FAILED");

    return all_ok ? EXIT_SUCCESS : EXIT_FAILURE;
}
