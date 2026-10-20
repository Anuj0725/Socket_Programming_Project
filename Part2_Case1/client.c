/*
 * client.c - Part 2 Case 1: Fault-Tolerant Client (No Session Management)
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * On disconnect, the client reconnects and restarts the entire transfer
 * from scratch. Retries up to max_retries times (configurable, default 1000).
 *
 * Usage: ./client <server_ip> <server_port> <topic> <output_dir> [max_retries]
 */
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <dirent.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "topic_map.h"

#define DEFAULT_MAX_RETRIES 1000  /* large default so large topics can finish */
#define INITIAL_BACKOFF_MS  50
#define MAX_BACKOFF_MS      800

static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

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

/* Clear any partially received files from a failed attempt */
static void clear_output_dir(const char *output_path) {
    DIR *dir = opendir(output_path);
    if (!dir) return;

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char filepath[MAX_PATH_LEN * 2];
        snprintf(filepath, sizeof(filepath), "%s/%s", output_path, entry->d_name);
        struct stat st;
        if (stat(filepath, &st) == 0 && S_ISREG(st.st_mode)) {
            unlink(filepath);
        }
    }
    closedir(dir);
}

/* Attempt to connect to the server */
static int connect_to_server(const char *server_ip, int server_port) {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) { perror("socket"); return -1; }

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_port = htons(server_port);
    if (inet_pton(AF_INET, server_ip, &server_addr.sin_addr) <= 0) {
        fprintf(stderr, "[ERROR] Invalid server IP: %s\n", server_ip);
        close(sockfd);
        return -1;
    }

    if (connect(sockfd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        close(sockfd);
        return -1;
    }
    set_tcp_nodelay(sockfd);
    return sockfd;
}

/*
 * Receive all files
 * Returns:
 *   0 on success
 *  -1 on connection drop / transient error (should retry)
 *  -2 on fatal error (e.g. MSG_ERROR_RESPONSE - do not retry)
 */
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
            *bytes_received = total_bytes;
            *files_received = total_files;
            return -1;
        }

        switch (hdr.msg_type) {
            case MSG_TRANSFER_COMPLETE: {
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
                fprintf(stderr, "[FATAL] Server error: %s\n", err);
                free(payload);
                *bytes_received = total_bytes;
                *files_received = total_files;
                return -2; /* Fatal error -> exit immediately */
            }

            case MSG_FILE_HEADER: {
                if (!payload || payload_len < 10) {
                    fprintf(stderr, "[ERROR] Invalid FILE_HEADER\n");
                    free(payload);
                    return -1;
                }

                uint16_t name_len;
                memcpy(&name_len, payload, 2);
                name_len = ntohs(name_len);

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
                free(payload);

                /* Reject path traversal */
                if (strstr(filename, "..") || strchr(filename, '/') || strchr(filename, '\\')) {
                    fprintf(stderr, "[ERROR] Path traversal attempt in filename rejected: %s\n", filename);
                    return -1;
                }

                char filepath[MAX_PATH_LEN * 2];
                snprintf(filepath, sizeof(filepath), "%s/%s", output_path, filename);

                FILE *fp = fopen(filepath, "wb");
                if (!fp) {
                    fprintf(stderr, "[ERROR] Cannot create file: %s\n", filepath);
                    return -1;
                }

                uint64_t received = 0;
                while (received < file_size) {
                    MessageHeader data_hdr;
                    uint8_t *data_payload = NULL;
                    uint32_t data_len = 0;

                    if (recv_message(sockfd, &data_hdr, &data_payload, &data_len) < 0) {
                        fclose(fp);
                        *bytes_received = total_bytes;
                        *files_received = total_files;
                        return -1;
                    }

                    if (data_hdr.msg_type != MSG_FILE_DATA) {
                        fprintf(stderr, "[ERROR] Expected FILE_DATA, got 0x%02x\n",
                                data_hdr.msg_type);
                        free(data_payload);
                        fclose(fp);
                        return -1;
                    }

                    if (data_payload && data_len > 0) {
                        /* Check fwrite return value */
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
                break;
            }

            default:
                free(payload);
                break;
        }
    }
}

/* ============================================================
 * Main - retry loop
 * ============================================================ */
int main(int argc, char *argv[]) {
    if (argc < 5) {
        fprintf(stderr, "Usage: %s <server_ip> <server_port> <topic> <output_dir> [max_retries]\n", argv[0]);
        return 1;
    }

    const char *server_ip = argv[1];
    int server_port = atoi(argv[2]);
    const char *topic = argv[3];
    const char *output_dir = argv[4];
    int max_retries = DEFAULT_MAX_RETRIES;

    if (argc >= 6) {
        max_retries = atoi(argv[5]);
        if (max_retries < 1) max_retries = 1;
    }

    fprintf(stderr, "==============================================\n");
    fprintf(stderr, "  Part 2 Case 1: No Session Management Client\n");
    fprintf(stderr, "  Server:      %s:%d\n", server_ip, server_port);
    fprintf(stderr, "  Topic:       %s\n", topic);
    fprintf(stderr, "  Output:      %s\n", output_dir);
    fprintf(stderr, "  Max Retries: %d\n", max_retries);
    fprintf(stderr, "==============================================\n");

    signal(SIGPIPE, SIG_IGN);

    char output_path[MAX_PATH_LEN];
    snprintf(output_path, sizeof(output_path), "%s/%s", output_dir, topic);
    mkdirs(output_path);

    double overall_start = get_time_sec();
    double total_sleep_sec = 0.0;
    uint64_t total_bytes_all_attempts = 0;
    uint64_t useful_bytes = 0;
    int attempt = 0;
    int success = 0;
    int backoff_ms = INITIAL_BACKOFF_MS;

    while (attempt < max_retries && !success) {
        attempt++;

        /* Clear any partial files from previous failed attempts */
        clear_output_dir(output_path);

        fprintf(stderr, "\n[INFO] Attempt %d/%d: connecting to %s:%d...\n",
                attempt, max_retries, server_ip, server_port);

        int sockfd = connect_to_server(server_ip, server_port);
        if (sockfd < 0) {
            /* Exponential backoff */
            fprintf(stderr, "[INFO] Retrying in %d ms...\n", backoff_ms);
            usleep(backoff_ms * 1000);
            total_sleep_sec += (backoff_ms / 1000.0);
            backoff_ms = (backoff_ms * 2 > MAX_BACKOFF_MS) ? MAX_BACKOFF_MS : backoff_ms * 2;
            continue;
        }

        /* Connected OK: reset backoff */
        backoff_ms = INITIAL_BACKOFF_MS;

        /* Send TOPIC_REQUEST */
        if (send_message(sockfd, MSG_TOPIC_REQUEST, STATUS_OK,
                         topic, strlen(topic)) < 0) {
            fprintf(stderr, "[ERROR] Failed to send TOPIC_REQUEST\n");
            close(sockfd);
            continue;
        }

        uint64_t bytes_received = 0;
        int files_received = 0;
        int ret = receive_all_files(sockfd, output_path, &bytes_received, &files_received);

        total_bytes_all_attempts += bytes_received;
        close(sockfd);

        if (ret == 0) {
            success = 1;
            useful_bytes = bytes_received;
            fprintf(stderr, "[INFO] Transfer completed successfully on attempt %d\n", attempt);
        } else if (ret == -2) {
            /* Fatal server error (e.g. topic not found), abort immediately */
            fprintf(stderr, "[FATAL] Aborting transfer due to fatal server error.\n");
            break;
        } else {
            fprintf(stderr, "[WARN] Transfer failed on attempt %d (%lu bytes received before failure)\n",
                    attempt, (unsigned long)bytes_received);
            /* Backoff before retry */
            fprintf(stderr, "[INFO] Retrying in %d ms...\n", backoff_ms);
            usleep(backoff_ms * 1000);
            total_sleep_sec += (backoff_ms / 1000.0);
            backoff_ms = (backoff_ms * 2 > MAX_BACKOFF_MS) ? MAX_BACKOFF_MS : backoff_ms * 2;
        }
    }

    double overall_elapsed = get_time_sec() - overall_start;
    double active_time = overall_elapsed - total_sleep_sec;
    if (active_time < 0) active_time = 0;

    uint64_t redundant_bytes = (success && total_bytes_all_attempts >= useful_bytes)
                               ? (total_bytes_all_attempts - useful_bytes)
                               : total_bytes_all_attempts;

    fprintf(stderr, "\n==============================================\n");
    fprintf(stderr, "  Transfer Summary (Case 1 - No Session Mgmt)\n");
    fprintf(stderr, "  Topic:              %s\n", topic);
    fprintf(stderr, "  Total attempts:     %d\n", attempt);
    fprintf(stderr, "  Status:             %s\n", success ? "SUCCESS" : "FAILED");
    fprintf(stderr, "  Useful bytes:       %lu\n", (unsigned long)useful_bytes);
    fprintf(stderr, "  Total wire bytes:   %lu\n", (unsigned long)total_bytes_all_attempts);
    fprintf(stderr, "  Redundant bytes:    %lu\n", (unsigned long)redundant_bytes);
    fprintf(stderr, "  Total elapsed time: %.3f sec\n", overall_elapsed);
    fprintf(stderr, "  Total sleep time:   %.3f sec\n", total_sleep_sec);
    if (success && overall_elapsed > 0) {
        fprintf(stderr, "  Throughput:         %.2f MB/s (active: %.2f MB/s)\n",
                useful_bytes / overall_elapsed / 1024.0 / 1024.0,
                active_time > 0 ? (useful_bytes / active_time / 1024.0 / 1024.0) : 0.0);
        fprintf(stderr, "  Overhead ratio:     %.2f%%\n",
                (useful_bytes > 0) ? (redundant_bytes * 100.0 / useful_bytes) : 0.0);
    } else {
        fprintf(stderr, "  Throughput:         N/A (Failed to complete)\n");
        fprintf(stderr, "  Overhead ratio:     N/A (100%% wasted)\n");
    }
    fprintf(stderr, "==============================================\n");

    /* CSV output: part,topic,attempts,useful_bytes,total_bytes,redundant_bytes,time,active_time,throughput,overhead,status */
    printf("part2_case1,%s,%d,%lu,%lu,%lu,%.6f,%.6f,%.2f,%.2f,%s\n",
           topic, attempt,
           (unsigned long)useful_bytes,
           (unsigned long)total_bytes_all_attempts,
           (unsigned long)redundant_bytes,
           overall_elapsed,
           active_time,
           (success && overall_elapsed > 0) ? (useful_bytes / overall_elapsed / 1024.0 / 1024.0) : 0.0,
           (success && useful_bytes > 0) ? (redundant_bytes * 100.0 / useful_bytes) : 100.0,
           success ? "success" : "failed");

    return success ? 0 : 1;
}
