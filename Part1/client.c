/*
 * client.c - Part 1: Basic Topic-Based File Distribution Client
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir>
 *
 * Connects to the server, sends a TOPIC_REQUEST for the specified topic,
 * receives all files, and writes them to output_dir/topic/
 */
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "topic_map.h"  /* For MAX_FILENAME_LEN etc. */

/* ============================================================
 * Get elapsed time in seconds using CLOCK_MONOTONIC
 * ============================================================ */
static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ============================================================
 * Create directory recursively (like mkdir -p)
 * ============================================================ */
static int mkdirs(const char *path) {
    char tmp[MAX_PATH_LEN * 4];
    snprintf(tmp, sizeof(tmp), "%s", path);
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
 * Receive all files for a topic from the server
 * Returns 0 on success, -1 on error
 * ============================================================ */
static int receive_all_files(int sockfd, const char *output_path,
                             uint64_t *bytes_received, int *files_received) {
    uint64_t total_bytes = 0;
    int total_files = 0;

    while (1) {
        MessageHeader hdr;
        uint8_t *payload = NULL;
        uint32_t payload_len = 0;

        if (recv_message(sockfd, &hdr, &payload, &payload_len) < 0) {
            fprintf(stderr, "[ERROR] Connection lost during transfer\n");
            return -1;
        }

        switch (hdr.msg_type) {
            case MSG_TRANSFER_COMPLETE: {
                fprintf(stderr, "[INFO] Transfer complete: %d files, %lu bytes\n",
                        total_files, (unsigned long)total_bytes);
                free(payload);
                *bytes_received = total_bytes;
                *files_received = total_files;
                return 0;
            }

            case MSG_ERROR_RESPONSE: {
                char err[256] = {0};
                if (payload && payload_len > 0) {
                    int len = (payload_len < 255) ? payload_len : 255;
                    memcpy(err, payload, len);
                }
                fprintf(stderr, "[ERROR] Server error: %s\n", err);
                free(payload);
                return -1;
            }

            case MSG_FILE_HEADER: {
                if (!payload || payload_len < 10) {
                    fprintf(stderr, "[ERROR] Invalid FILE_HEADER\n");
                    free(payload);
                    return -1;
                }

                /* Parse FILE_HEADER payload:
                 *   filename_length (2 bytes, uint16)
                 *   filename        (variable)
                 *   file_size       (8 bytes, uint64)
                 */
                uint16_t name_len;
                memcpy(&name_len, payload, 2);
                name_len = ntohs(name_len);

                if (payload_len < (uint32_t)(2 + name_len + 8)) {
                    fprintf(stderr, "[ERROR] FILE_HEADER too short\n");
                    free(payload);
                    return -1;
                }

                char filename[MAX_FILENAME_LEN];
                int fn_len = (name_len < MAX_FILENAME_LEN - 1) ? name_len : MAX_FILENAME_LEN - 1;
                memcpy(filename, payload + 2, fn_len);
                filename[fn_len] = '\0';

                uint64_t file_size;
                uint32_t hi, lo;
                memcpy(&hi, payload + 2 + name_len, 4);
                memcpy(&lo, payload + 2 + name_len + 4, 4);
                hi = ntohl(hi);
                lo = ntohl(lo);
                file_size = ((uint64_t)hi << 32) | lo;

                /* Path traversal check */
                if (strstr(filename, "..") || strchr(filename, '/') || strchr(filename, '\\')) {
                    fprintf(stderr, "[ERROR] Insecure filename rejected: %s\n", filename);
                    return -1;
                }

                free(payload);

                fprintf(stderr, "[INFO] Receiving file: %s (%lu bytes)\n",
                        filename, (unsigned long)file_size);

                /* Create file path */
                char filepath[MAX_PATH_LEN * 2];
                snprintf(filepath, sizeof(filepath), "%s/%s", output_path, filename);

                /* Open file for writing */
                FILE *fp = fopen(filepath, "wb");
                if (!fp) {
                    fprintf(stderr, "[ERROR] Cannot create file: %s\n", filepath);
                    return -1;
                }

                /* Receive FILE_DATA chunks until file is complete */
                uint64_t received = 0;
                while (received < file_size) {
                    MessageHeader data_hdr;
                    uint8_t *data_payload = NULL;
                    uint32_t data_len = 0;

                    if (recv_message(sockfd, &data_hdr, &data_payload, &data_len) < 0) {
                        fprintf(stderr, "[ERROR] Connection lost during file transfer\n");
                        fclose(fp);
                        return -1;
                    }

                    if (data_hdr.msg_type != MSG_FILE_DATA) {
                        fprintf(stderr, "[ERROR] Expected FILE_DATA (0x03), got 0x%02x\n",
                                data_hdr.msg_type);
                        free(data_payload);
                        fclose(fp);
                        return -1;
                    }

                    if (data_payload && data_len > 0) {
                        size_t written = fwrite(data_payload, 1, data_len, fp);
                        if (written != data_len) {
                            fprintf(stderr, "[ERROR] fwrite failed (disk full?)\n");
                            free(data_payload);
                            fclose(fp);
                            return -1;
                        }
                        received += data_len;
                        total_bytes += data_len;
                    }
                    free(data_payload);
                }

                fclose(fp);
                total_files++;
                fprintf(stderr, "[INFO] File saved: %s (%lu bytes)\n",
                        filename, (unsigned long)file_size);
                break;
            }

            default: {
                fprintf(stderr, "[WARN] Unexpected message type: 0x%02x\n", hdr.msg_type);
                free(payload);
                break;
            }
        }
    }
}

/* ============================================================
 * Print usage
 * ============================================================ */
static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir>\n", prog);
}

/* ============================================================
 * Main
 * ============================================================ */
int main(int argc, char *argv[]) {
    if (argc < 5) {
        print_usage(argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];

    fprintf(stderr, "==============================================\n");
    fprintf(stderr, "  Part 1: Topic-Based File Distribution Client\n");
    fprintf(stderr, "  Server: %s:%d\n", server_ip, server_port);
    fprintf(stderr, "  Topic:  %s\n", topic);
    fprintf(stderr, "  Output: %s\n", output_dir);
    fprintf(stderr, "==============================================\n");

    signal(SIGPIPE, SIG_IGN);

    /* Create output directory: output_dir/topic/ */
    char output_path[MAX_PATH_LEN];
    snprintf(output_path, sizeof(output_path), "%s/%s", output_dir, topic);
    mkdirs(output_path);
    fprintf(stderr, "[INFO] Output directory: %s\n", output_path);

    /* Connect to server */
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        perror("socket");
        return 1;
    }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "[ERROR] Invalid server IP: %s\n", server_ip);
        close(sockfd);
        return 1;
    }

    fprintf(stderr, "[INFO] Connecting to %s:%d...\n", server_ip, server_port);
    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("connect");
        close(sockfd);
        return 1;
    }
    set_tcp_nodelay(sockfd);
    fprintf(stderr, "[INFO] Connected to server\n");

    double start_time = get_time_sec();

    /* Send TOPIC_REQUEST */
    if (send_message(sockfd, MSG_TOPIC_REQUEST, STATUS_OK,
                     topic, strlen(topic)) < 0) {
        fprintf(stderr, "[ERROR] Failed to send TOPIC_REQUEST\n");
        close(sockfd);
        return 1;
    }
    fprintf(stderr, "[INFO] Sent TOPIC_REQUEST for '%s'\n", topic);

    /* Receive all files */
    uint64_t bytes_received = 0;
    int files_received = 0;
    int ret = receive_all_files(sockfd, output_path, &bytes_received, &files_received);

    double elapsed = get_time_sec() - start_time;

    /* Print summary */
    fprintf(stderr, "\n==============================================\n");
    fprintf(stderr, "  Transfer Summary\n");
    fprintf(stderr, "  Topic:          %s\n", topic);
    fprintf(stderr, "  Files received: %d\n", files_received);
    fprintf(stderr, "  Bytes received: %lu\n", (unsigned long)bytes_received);
    fprintf(stderr, "  Time elapsed:   %.3f sec\n", elapsed);
    if (elapsed > 0) {
        fprintf(stderr, "  Throughput:     %.2f MB/s\n",
                bytes_received / elapsed / 1024.0 / 1024.0);
    }
    fprintf(stderr, "  Status:         %s\n", (ret == 0) ? "SUCCESS" : "FAILED");
    fprintf(stderr, "==============================================\n");

    /* Output CSV-format metrics to stdout for data collection */
    printf("part1,%s,%d,%lu,%.6f,%.2f,%s\n",
           topic, files_received, (unsigned long)bytes_received,
           elapsed,
           (elapsed > 0) ? (bytes_received / elapsed / 1024.0 / 1024.0) : 0.0,
           (ret == 0) ? "success" : "failed");

    close(sockfd);
    return (ret == 0) ? 0 : 1;
}
