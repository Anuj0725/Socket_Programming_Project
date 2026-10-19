/*
 * server.c - Part 2 Case 1: Fault-Tolerant Server (No Session Management)
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Multi-threaded server with fault injection. When a fault occurs,
 * the connection is terminated. Since no session state is maintained,
 * the client must restart the entire transfer from scratch.
 *
 * Usage: ./server <port> <topics_root_dir> <failure_probability>
 */
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <errno.h>
#include <time.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "topic_map.h"

/* ============================================================
 * Global state
 * ============================================================ */
static TopicMap g_topic_map;
static volatile int g_running = 1;
static double g_failure_probability = 0.0;

/* Performance metrics */
typedef struct {
    uint64_t total_bytes_sent;
    uint64_t total_files_sent;
    uint64_t total_faults_injected;
    int      clients_served;
    int      transfers_completed;
    double   total_time_seconds;
    pthread_mutex_t lock;
} ServerMetrics;

static ServerMetrics g_metrics = { 0, 0, 0, 0, 0, 0.0, PTHREAD_MUTEX_INITIALIZER };

/* Active worker thread tracking for graceful shutdown */
static int g_active_threads = 0;
static pthread_mutex_t g_thread_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_thread_cond = PTHREAD_COND_INITIALIZER;

typedef struct {
    int client_fd;
    TopicMap *topic_map;
    struct sockaddr_in client_addr;
} ClientArgs;

/* ============================================================
 * Signal handler
 * ============================================================ */
static void signal_handler(int sig) {
    (void)sig;
    fprintf(stderr, "\n[INFO] Received signal %d, shutting down...\n", sig);
    g_running = 0;
}

static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ============================================================
 * Fault injection: check if we should terminate this connection
 * Uses thread-local RNG (drand48_r) per design document Section 4
 * ============================================================ */
typedef struct {
    struct drand48_data rng_buf;
} ThreadRNG;

static void init_thread_rng(ThreadRNG *rng) {
    long seed = (long)time(NULL) ^ (long)pthread_self();
    srand48_r(seed, &rng->rng_buf);
}

static int maybe_inject_fault(ThreadRNG *rng, double fail_prob) {
    if (fail_prob <= 0.0) return 0;
    double r;
    drand48_r(&rng->rng_buf, &r);
    return (r < fail_prob) ? 1 : 0;
}

/* ============================================================
 * Send all files with fault injection
 * ============================================================ */
static int send_all_files_with_fault(int client_fd, const char *topic,
                                     const char *dir_path, double fail_prob,
                                     ThreadRNG *rng,
                                     uint64_t *bytes_out, int *files_out) {
    int file_count = 0;
    char **files = list_files_in_dir(dir_path, &file_count);
    if (!files || file_count == 0) {
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR,
                     "No files in topic", 17);
        return -1;
    }

    uint64_t total_bytes = 0;

    for (int i = 0; i < file_count; i++) {
        char filepath[MAX_PATH_LEN];
        snprintf(filepath, sizeof(filepath), "%s/%s", dir_path, files[i]);

        /* Check file stats and open file BEFORE sending FILE_HEADER */
        struct stat st;
        if (stat(filepath, &st) != 0) {
            fprintf(stderr, "[WARN] Cannot stat file: %s\n", filepath);
            continue;
        }
        uint64_t file_size = (uint64_t)st.st_size;

        FILE *fp = fopen(filepath, "rb");
        if (!fp) {
            fprintf(stderr, "[WARN] Cannot open file: %s\n", filepath);
            continue;
        }

        /* Build and send FILE_HEADER */
        uint16_t name_len = (uint16_t)strlen(files[i]);
        uint32_t hdr_payload_len = 2 + name_len + 8;
        uint8_t *hdr_payload = malloc(hdr_payload_len);
        if (!hdr_payload) {
            fclose(fp);
            continue;
        }

        uint16_t name_len_n = htons(name_len);
        memcpy(hdr_payload, &name_len_n, 2);
        memcpy(hdr_payload + 2, files[i], name_len);

        uint64_t file_size_n;
        uint32_t hi = htonl((uint32_t)(file_size >> 32));
        uint32_t lo = htonl((uint32_t)(file_size & 0xFFFFFFFF));
        memcpy(&file_size_n, &hi, 4);
        memcpy(((uint8_t *)&file_size_n) + 4, &lo, 4);
        memcpy(hdr_payload + 2 + name_len, &file_size_n, 8);

        if (send_message(client_fd, MSG_FILE_HEADER, STATUS_OK,
                         hdr_payload, hdr_payload_len) < 0) {
            free(hdr_payload);
            fclose(fp);
            free_file_list(files, file_count);
            return -1;
        }
        free(hdr_payload);

        /* Send FILE_DATA chunks with fault injection */

        uint8_t buf[CHUNK_SIZE];
        size_t n;
        while ((n = fread(buf, 1, CHUNK_SIZE, fp)) > 0) {
            if (send_message(client_fd, MSG_FILE_DATA, STATUS_OK,
                             buf, (uint32_t)n) < 0) {
                fclose(fp);
                free_file_list(files, file_count);
                return -1;
            }
            total_bytes += n;

            /* --- Fault injection check after each chunk --- */
            if (maybe_inject_fault(rng, fail_prob)) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                fprintf(stderr, "[FAULT] [%ld.%09ld] Injected fault: topic='%s', "
                        "file='%s' (%d/%d), offset=%lu\n",
                        ts.tv_sec, ts.tv_nsec,
                        topic, files[i], i + 1, file_count,
                        (unsigned long)total_bytes);

                pthread_mutex_lock(&g_metrics.lock);
                g_metrics.total_faults_injected++;
                pthread_mutex_unlock(&g_metrics.lock);

                fclose(fp);
                free_file_list(files, file_count);
                return -2;  /* -2 indicates fault injection */
            }
        }
        fclose(fp);

        fprintf(stderr, "[INFO] Sent file %d/%d: %s (%lu bytes)\n",
                i + 1, file_count, files[i], (unsigned long)file_size);
    }

    /* Send TRANSFER_COMPLETE */
    send_message(client_fd, MSG_TRANSFER_COMPLETE, STATUS_END_OF_TRANSFER, NULL, 0);

    *bytes_out = total_bytes;
    *files_out = file_count;
    free_file_list(files, file_count);
    return 0;
}

/* ============================================================
 * Handle a single client connection
 * ============================================================ */
static void handle_client(int client_fd, TopicMap *topic_map,
                          struct sockaddr_in *client_addr) {
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr->sin_addr, client_ip, sizeof(client_ip));
    fprintf(stderr, "[INFO] Client connected: %s:%d\n", client_ip,
            ntohs(client_addr->sin_port));

    /* Initialize thread-local RNG for fault injection */
    ThreadRNG rng;
    init_thread_rng(&rng);

    double start_time = get_time_sec();

    /* Receive TOPIC_REQUEST */
    MessageHeader hdr;
    uint8_t *payload = NULL;
    uint32_t payload_len = 0;

    if (recv_message(client_fd, &hdr, &payload, &payload_len) < 0) {
        fprintf(stderr, "[ERROR] Failed to receive message from %s\n", client_ip);
        return;
    }

    if (hdr.msg_type != MSG_TOPIC_REQUEST) {
        fprintf(stderr, "[ERROR] Expected TOPIC_REQUEST, got 0x%02x\n", hdr.msg_type);
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR,
                     "Invalid request", 15);
        free(payload);
        return;
    }

    char topic[MAX_TOPIC_LEN];
    int topic_len = (payload_len < MAX_TOPIC_LEN - 1) ? payload_len : MAX_TOPIC_LEN - 1;
    memcpy(topic, payload, topic_len);
    topic[topic_len] = '\0';
    free(payload);

    fprintf(stderr, "[INFO] Client %s requested topic: '%s'\n", client_ip, topic);

    TopicEntry *entry = lookup_topic(topic_map, topic);
    if (!entry) {
        char err_msg[128];
        snprintf(err_msg, sizeof(err_msg), "Topic not found: %s", topic);
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR,
                     err_msg, strlen(err_msg));
        fprintf(stderr, "[ERROR] Topic '%s' not found\n", topic);
        return;
    }

    uint64_t bytes_sent = 0;
    int files_sent = 0;
    int ret = send_all_files_with_fault(client_fd, topic, entry->dir_path,
                                        g_failure_probability, &rng,
                                        &bytes_sent, &files_sent);

    double elapsed = get_time_sec() - start_time;

    pthread_mutex_lock(&g_metrics.lock);
    g_metrics.total_bytes_sent += bytes_sent;
    g_metrics.clients_served++;
    if (ret == 0) {
        g_metrics.total_files_sent += files_sent;
        g_metrics.transfers_completed++;
    }
    g_metrics.total_time_seconds += elapsed;
    pthread_mutex_unlock(&g_metrics.lock);

    if (ret == -2) {
        fprintf(stderr, "[INFO] Connection terminated by fault injection for %s\n", client_ip);
    } else if (ret == 0) {
        double throughput = (elapsed > 0) ? (bytes_sent / elapsed / 1024.0 / 1024.0) : 0;
        fprintf(stderr, "[INFO] Transfer complete for %s: %d files, %lu bytes, %.3f sec, "
                "%.2f MB/s\n", client_ip, files_sent, (unsigned long)bytes_sent,
                elapsed, throughput);
    } else {
        fprintf(stderr, "[ERROR] Transfer failed for %s\n", client_ip);
    }
}

/* ============================================================
 * Thread function
 * ============================================================ */
static void *client_thread_fn(void *arg) {
    ClientArgs *a = (ClientArgs *)arg;
    handle_client(a->client_fd, a->topic_map, &a->client_addr);
    close(a->client_fd);
    free(a);

    pthread_mutex_lock(&g_thread_lock);
    g_active_threads--;
    pthread_cond_broadcast(&g_thread_cond);
    pthread_mutex_unlock(&g_thread_lock);

    return NULL;
}

/* ============================================================
 * Main
 * ============================================================ */
int main(int argc, char *argv[]) {
    if (argc < 4) {
        fprintf(stderr, "Usage: %s <port> <topics_root_dir> <failure_probability>\n", argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    const char *topics_root_dir = argv[2];
    g_failure_probability = atof(argv[3]);

    if (g_failure_probability < 0.0 || g_failure_probability > 1.0) {
        fprintf(stderr, "[ERROR] failure_probability must be in [0.0, 1.0]\n");
        return 1;
    }

    fprintf(stderr, "==============================================\n");
    fprintf(stderr, "  Part 2 Case 1: No Session Management Server\n");
    fprintf(stderr, "  Port: %d\n", port);
    fprintf(stderr, "  Topics dir: %s\n", topics_root_dir);
    fprintf(stderr, "  Failure probability: %.4f\n", g_failure_probability);
    fprintf(stderr, "==============================================\n");

    /* Install signal handlers without SA_RESTART so accept() unblocks */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    if (init_topic_map(&g_topic_map, topics_root_dir) < 0) {
        fprintf(stderr, "[FATAL] Failed to initialize topic map\n");
        return 1;
    }
    print_topic_map(&g_topic_map);

    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) { perror("socket"); return 1; }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind"); close(listen_fd); return 1;
    }
    if (listen(listen_fd, 128) < 0) {
        perror("listen"); close(listen_fd); return 1;
    }

    fprintf(stderr, "[INFO] Server listening on port %d\n", port);

    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR || !g_running) break;
            perror("accept");
            continue;
        }
        set_tcp_nodelay(client_fd);

        ClientArgs *args = malloc(sizeof(ClientArgs));
        if (!args) { close(client_fd); continue; }
        args->client_fd = client_fd;
        args->topic_map = &g_topic_map;
        args->client_addr = client_addr;

        pthread_mutex_lock(&g_thread_lock);
        g_active_threads++;
        pthread_mutex_unlock(&g_thread_lock);

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread_fn, args) != 0) {
            perror("pthread_create");
            pthread_mutex_lock(&g_thread_lock);
            g_active_threads--;
            pthread_mutex_unlock(&g_thread_lock);
            close(client_fd);
            free(args);
            continue;
        }
        pthread_detach(tid);
    }

    close(listen_fd);

    /* Graceful shutdown: wait up to 10 seconds for active worker threads */
    struct timespec deadline;
    clock_gettime(CLOCK_REALTIME, &deadline);
    deadline.tv_sec += 10;
    pthread_mutex_lock(&g_thread_lock);
    while (g_active_threads > 0) {
        if (pthread_cond_timedwait(&g_thread_cond, &g_thread_lock, &deadline) != 0) {
            fprintf(stderr, "[INFO] Shutdown timeout reached with %d threads active\n", g_active_threads);
            break;
        }
    }
    pthread_mutex_unlock(&g_thread_lock);

    /* Print final metrics under mutex lock */
    pthread_mutex_lock(&g_metrics.lock);
    fprintf(stderr, "\n==============================================\n");
    fprintf(stderr, "  Server Metrics Summary (Case 1 - No Session Mgmt)\n");
    fprintf(stderr, "  Clients served:      %d\n", g_metrics.clients_served);
    fprintf(stderr, "  Transfers completed: %d\n", g_metrics.transfers_completed);
    fprintf(stderr, "  Faults injected:     %lu\n", (unsigned long)g_metrics.total_faults_injected);
    fprintf(stderr, "  Total payload sent:  %lu bytes\n", (unsigned long)g_metrics.total_bytes_sent);
    fprintf(stderr, "  Total time:          %.3f sec\n", g_metrics.total_time_seconds);
    fprintf(stderr, "==============================================\n");
    pthread_mutex_unlock(&g_metrics.lock);

    return 0;
}
