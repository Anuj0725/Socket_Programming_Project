/*
 * server.c - Part 1: Basic Topic-Based File Distribution Server
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Implements both single-threaded and multi-threaded modes.
 * Usage: ./server <port> <topics_root_dir>
 *
 * The server mode is determined by a compile-time flag:
 *   -DMULTI_THREADED  -> multi-threaded (default)
 *   Without flag      -> single-threaded
 * Both modes are compiled by the Makefile into a single binary that
 * defaults to multi-threaded. Pass --single-threaded flag at runtime
 * to use single-threaded mode.
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
static int g_single_threaded = 0;  /* 0 = multi-threaded (default) */

/* Performance metrics (protected by mutex in multi-threaded mode) */
typedef struct {
    uint64_t total_bytes_sent;
    uint64_t total_files_sent;
    int      clients_served;
    double   total_time_seconds;
    pthread_mutex_t lock;
} ServerMetrics;

static ServerMetrics g_metrics = { 0, 0, 0, 0.0, PTHREAD_MUTEX_INITIALIZER };

/* Active worker thread tracking for graceful shutdown */
static int g_active_threads = 0;
static pthread_mutex_t g_thread_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_thread_cond = PTHREAD_COND_INITIALIZER;

/* Arguments passed to client handler thread */
typedef struct {
    int client_fd;
    TopicMap *topic_map;
    struct sockaddr_in client_addr;
} ClientArgs;

/* ============================================================
 * Signal handler for graceful shutdown
 * ============================================================ */
static void signal_handler(int sig) {
    (void)sig;
    fprintf(stderr, "\n[INFO] Received signal %d, shutting down...\n", sig);
    g_running = 0;
}

/* ============================================================
 * Get elapsed time in seconds using CLOCK_MONOTONIC
 * ============================================================ */
static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ============================================================
 * Send all files for a given topic to the client
 * Protocol flow: (FILE_HEADER + FILE_DATA chunks)* + TRANSFER_COMPLETE
 * ============================================================ */
static int send_all_files(int client_fd, const char *topic, const char *dir_path,
                          uint64_t *bytes_out, int *files_out) {
    int file_count = 0;
    char **files = list_files_in_dir(dir_path, &file_count);
    if (!files || file_count == 0) {
        fprintf(stderr, "[WARN] No files found for topic '%s'\n", topic);
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

        /* Build FILE_HEADER payload:
         *   filename_length (2 bytes, uint16, network order)
         *   filename        (variable)
         *   file_size       (8 bytes, uint64, network order)
         */
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

        /* Convert file_size to network byte order (big-endian) */
        uint64_t file_size_n;
        uint32_t hi = htonl((uint32_t)(file_size >> 32));
        uint32_t lo = htonl((uint32_t)(file_size & 0xFFFFFFFF));
        memcpy(&file_size_n, &hi, 4);
        memcpy(((uint8_t *)&file_size_n) + 4, &lo, 4);
        memcpy(hdr_payload + 2 + name_len, &file_size_n, 8);

        /* Send FILE_HEADER */
        if (send_message(client_fd, MSG_FILE_HEADER, STATUS_OK,
                         hdr_payload, hdr_payload_len) < 0) {
            free(hdr_payload);
            fclose(fp);
            free_file_list(files, file_count);
            return -1;
        }
        free(hdr_payload);

        /* Send FILE_DATA chunks */
        uint8_t buf[CHUNK_SIZE];
        size_t n;
        while ((n = fread(buf, 1, CHUNK_SIZE, fp)) > 0) {
            if (send_message(client_fd, MSG_FILE_DATA, STATUS_OK, buf, (uint32_t)n) < 0) {
                fclose(fp);
                free_file_list(files, file_count);
                return -1;
            }
            total_bytes += n;
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
 * Protocol flow: receive TOPIC_REQUEST -> send files -> close
 * ============================================================ */
static void handle_client(int client_fd, TopicMap *topic_map,
                          struct sockaddr_in *client_addr) {
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr->sin_addr, client_ip, sizeof(client_ip));
    fprintf(stderr, "[INFO] Client connected: %s:%d\n", client_ip,
            ntohs(client_addr->sin_port));

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
        fprintf(stderr, "[ERROR] Expected TOPIC_REQUEST, got 0x%02x from %s\n",
                hdr.msg_type, client_ip);
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR,
                     "Invalid request", 15);
        free(payload);
        return;
    }

    /* Extract topic name */
    char topic[MAX_TOPIC_LEN];
    int topic_len = (payload_len < MAX_TOPIC_LEN - 1) ? payload_len : MAX_TOPIC_LEN - 1;
    memcpy(topic, payload, topic_len);
    topic[topic_len] = '\0';
    free(payload);

    fprintf(stderr, "[INFO] Client %s requested topic: '%s'\n", client_ip, topic);

    /* Look up topic */
    TopicEntry *entry = lookup_topic(topic_map, topic);
    if (!entry) {
        char err_msg[128];
        snprintf(err_msg, sizeof(err_msg), "Topic not found: %s", topic);
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR,
                     err_msg, strlen(err_msg));
        fprintf(stderr, "[ERROR] Topic '%s' not found\n", topic);
        return;
    }

    /* Send all files for the topic */
    uint64_t bytes_sent = 0;
    int files_sent = 0;
    if (send_all_files(client_fd, topic, entry->dir_path,
                       &bytes_sent, &files_sent) < 0) {
        fprintf(stderr, "[ERROR] Transfer failed for client %s topic '%s'\n",
                client_ip, topic);
        return;
    }

    double elapsed = get_time_sec() - start_time;

    /* Update metrics */
    pthread_mutex_lock(&g_metrics.lock);
    g_metrics.total_bytes_sent += bytes_sent;
    g_metrics.total_files_sent += files_sent;
    g_metrics.clients_served++;
    g_metrics.total_time_seconds += elapsed;
    pthread_mutex_unlock(&g_metrics.lock);

    double throughput = (elapsed > 0) ? (bytes_sent / elapsed / 1024.0 / 1024.0) : 0;
    fprintf(stderr, "[INFO] Transfer complete for %s: %d files, %lu bytes, %.3f sec, "
            "%.2f MB/s\n", client_ip, files_sent, (unsigned long)bytes_sent,
            elapsed, throughput);
}

/* ============================================================
 * Thread function for multi-threaded mode
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
 * Print usage
 * ============================================================ */
static void print_usage(const char *prog) {
    fprintf(stderr, "Usage: %s <port> <topics_root_dir> [--single-threaded]\n", prog);
    fprintf(stderr, "  <port>             Port number to listen on\n");
    fprintf(stderr, "  <topics_root_dir>  Directory containing topic subdirectories\n");
    fprintf(stderr, "  --single-threaded  Use single-threaded mode (default: multi-threaded)\n");
}

/* ============================================================
 * Main
 * ============================================================ */
int main(int argc, char *argv[]) {
    if (argc < 3) {
        print_usage(argv[0]);
        return 1;
    }

    int port = atoi(argv[1]);
    const char *topics_root_dir = argv[2];

    /* Check for --single-threaded flag */
    for (int i = 3; i < argc; i++) {
        if (strcmp(argv[i], "--single-threaded") == 0) {
            g_single_threaded = 1;
        }
    }

    fprintf(stderr, "==============================================\n");
    fprintf(stderr, "  Part 1: Topic-Based File Distribution Server\n");
    fprintf(stderr, "  Mode: %s\n", g_single_threaded ? "SINGLE-THREADED" : "MULTI-THREADED");
    fprintf(stderr, "  Port: %d\n", port);
    fprintf(stderr, "  Topics dir: %s\n", topics_root_dir);
    fprintf(stderr, "==============================================\n");

    /* Install signal handlers without SA_RESTART so accept() unblocks */
    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);  /* Ignore broken pipe */

    /* Initialize topic map */
    if (init_topic_map(&g_topic_map, topics_root_dir) < 0) {
        fprintf(stderr, "[FATAL] Failed to initialize topic map\n");
        return 1;
    }
    print_topic_map(&g_topic_map);

    /* Create listening socket */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("socket");
        return 1;
    }

    /* Allow port reuse */
    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("bind");
        close(listen_fd);
        return 1;
    }

    if (listen(listen_fd, 128) < 0) {
        perror("listen");
        close(listen_fd);
        return 1;
    }

    fprintf(stderr, "[INFO] Server listening on port %d\n", port);

    /* Accept loop */
    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR || !g_running) break;  /* Interrupted by signal -> clean exit */
            perror("accept");
            continue;
        }
        set_tcp_nodelay(client_fd);

        if (g_single_threaded) {
            /* Single-threaded: handle client directly, blocking */
            handle_client(client_fd, &g_topic_map, &client_addr);
            close(client_fd);
        } else {
            /* Multi-threaded: spawn a new detached thread */
            ClientArgs *args = malloc(sizeof(ClientArgs));
            if (!args) {
                fprintf(stderr, "[ERROR] malloc failed for ClientArgs\n");
                close(client_fd);
                continue;
            }
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
    }

    /* Cleanup */
    close(listen_fd);

    /* Graceful shutdown: wait up to 10 seconds for active worker threads */
    if (!g_single_threaded) {
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
    }

    /* Print final metrics under mutex lock */
    pthread_mutex_lock(&g_metrics.lock);
    fprintf(stderr, "\n==============================================\n");
    fprintf(stderr, "  Server Metrics Summary\n");
    fprintf(stderr, "  Mode: %s\n", g_single_threaded ? "SINGLE-THREADED" : "MULTI-THREADED");
    fprintf(stderr, "  Clients served:    %d\n", g_metrics.clients_served);
    fprintf(stderr, "  Files sent:        %lu\n", (unsigned long)g_metrics.total_files_sent);
    fprintf(stderr, "  Total payload sent:%lu bytes\n", (unsigned long)g_metrics.total_bytes_sent);
    fprintf(stderr, "  Total time:        %.3f sec\n", g_metrics.total_time_seconds);
    if (g_metrics.total_time_seconds > 0) {
        fprintf(stderr, "  Avg throughput:    %.2f MB/s\n",
                g_metrics.total_bytes_sent / g_metrics.total_time_seconds / 1024.0 / 1024.0);
    }
    fprintf(stderr, "==============================================\n");
    pthread_mutex_unlock(&g_metrics.lock);

    return 0;
}
