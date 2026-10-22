/*
 * server.c - Part 2 Case 2: Fault-Tolerant Server with Session Management
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Implements checkpoint-based session management per design doc sections 3.3, 4, 5.
 * On disconnect, client sends RESUME_REQUEST with session_id, last_file_idx, byte_offset.
 * Server validates checkpoint and resumes transfer without re-sending completed files.
 *
 * Usage: ./server <port> <topics_root_dir> <failure_probability>
 */
#define _FILE_OFFSET_BITS 64

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
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
#include "session.h"

/* ============================================================
 * Global state
 * ============================================================ */
static TopicMap g_topic_map;
static SessionTable g_session_table;
static volatile int g_running = 1;
static double g_failure_probability = 0.0;
static char g_topics_dir[MAX_PATH_LEN];

/* Performance metrics */
typedef struct {
    uint64_t total_bytes_sent;
    uint64_t total_files_sent;
    uint64_t total_faults_injected;
    int      clients_served;
    int      resumes_handled;
    int      transfers_completed;
    double   total_time_seconds;
    pthread_mutex_t lock;
} ServerMetrics;

static ServerMetrics g_metrics = { 0, 0, 0, 0, 0, 0, 0.0, PTHREAD_MUTEX_INITIALIZER };

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

static void signal_handler(int sig) {
    (void)sig;
    g_running = 0;
}

static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* ============================================================
 * Fault injection using thread-local RNG (drand48_r)
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
 * Send files with session checkpointing and fault injection
 * ============================================================ */
static int send_files_session(int client_fd, SessionState *session,
                              const char *dir_path, int start_file_idx,
                              uint64_t start_offset, double fail_prob,
                              ThreadRNG *rng, uint64_t *bytes_out) {
    uint64_t session_bytes = session->total_bytes_sent;
    uint64_t bytes_this_attempt = 0; /* accurate local byte tracking */
    int total_files = session->total_files;

    for (int i = start_file_idx; i < total_files; i++) {
        char filepath[MAX_PATH_LEN];
        snprintf(filepath, sizeof(filepath), "%s/%s", dir_path, session->file_list[i]);

        /* Stat and open file BEFORE sending FILE_HEADER */
        struct stat st;
        if (stat(filepath, &st) != 0) {
            fprintf(stderr, "[WARN] Cannot stat file: %s\n", filepath);
            continue;
        }
        uint64_t file_size = (uint64_t)st.st_size;

        /* Validate start_offset <= file_size on resume */
        if (i == start_file_idx && start_offset > file_size) {
            fprintf(stderr, "[WARN] Resumed offset %lu exceeds file size %lu on %s\n",
                    (unsigned long)start_offset, (unsigned long)file_size, session->file_list[i]);
            start_offset = 0;
        }

        FILE *fp = fopen(filepath, "rb");
        if (!fp) {
            fprintf(stderr, "[WARN] Cannot open file: %s\n", filepath);
            continue;
        }

        uint64_t cur_offset = 0;
        /* If resuming this specific file mid-way */
        if (i == start_file_idx && start_offset > 0) {
            /* Treat fseek failure as error */
            if (fseeko(fp, (off_t)start_offset, SEEK_SET) != 0) {
                fprintf(stderr, "[ERROR] fseek to offset %lu failed on %s\n",
                        (unsigned long)start_offset, filepath);
                fclose(fp);
                send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "fseek failed", 12);
                *bytes_out = bytes_this_attempt;
                return -1;
            }
            cur_offset = start_offset;
            fprintf(stderr, "[RESUME] Resuming file '%s' from byte offset %lu/%lu\n",
                    session->file_list[i], (unsigned long)start_offset, (unsigned long)file_size);
        }

        /* Build and send FILE_HEADER */
        uint16_t name_len = (uint16_t)strlen(session->file_list[i]);
        uint32_t hdr_len = 2 + name_len + 8;
        uint8_t *hdr_buf = malloc(hdr_len);
        if (!hdr_buf) {
            fclose(fp);
            continue;
        }

        uint16_t name_len_n = htons(name_len);
        memcpy(hdr_buf, &name_len_n, 2);
        memcpy(hdr_buf + 2, session->file_list[i], name_len);
        uint64_to_net(file_size, hdr_buf + 2 + name_len);

        if (send_message(client_fd, MSG_FILE_HEADER, STATUS_OK, hdr_buf, hdr_len) < 0) {
            free(hdr_buf);
            fclose(fp);
            *bytes_out = bytes_this_attempt;
            return -1;
        }
        free(hdr_buf);

        uint8_t buf[CHUNK_SIZE];
        size_t n;
        while ((n = fread(buf, 1, CHUNK_SIZE, fp)) > 0) {
            if (send_message(client_fd, MSG_FILE_DATA, STATUS_OK, buf, (uint32_t)n) < 0) {
                fclose(fp);
                *bytes_out = bytes_this_attempt;
                return -1;
            }
            cur_offset += n;
            session_bytes += n;
            bytes_this_attempt += n;

            /* Check fault injection */
            if (maybe_inject_fault(rng, fail_prob)) {
                struct timespec ts;
                clock_gettime(CLOCK_REALTIME, &ts);
                fprintf(stderr, "[FAULT] [%ld.%09ld] Injected fault: session='%s', topic='%s', "
                        "file='%s' (%d/%d), offset=%lu/%lu\n",
                        ts.tv_sec, ts.tv_nsec,
                        session->session_id, session->topic,
                        session->file_list[i], i + 1, total_files,
                        (unsigned long)cur_offset, (unsigned long)file_size);

                /* Update and persist session state before disconnecting (isolated dir) */
                session_update(&g_session_table, session, i, cur_offset, session_bytes);
                session_persist_server(session, g_topics_dir);

                pthread_mutex_lock(&g_metrics.lock);
                g_metrics.total_faults_injected++;
                pthread_mutex_unlock(&g_metrics.lock);

                fclose(fp);
                *bytes_out = bytes_this_attempt;
                return -2; /* Fault injected */
            }
        }
        fclose(fp);

        /* File completed: send CHECKPOINT message to client */
        uint32_t ckpt_len = SESSION_ID_LEN + 4 + 4 + 2 + name_len + 8;
        uint8_t *ckpt_buf = malloc(ckpt_len);
        if (ckpt_buf) {
            memcpy(ckpt_buf, session->session_id, SESSION_ID_LEN);
            uint32_t comp_n = htonl(i + 1);
            uint32_t tot_n = htonl(total_files);
            uint16_t nlen_n = htons(name_len);
            memcpy(ckpt_buf + SESSION_ID_LEN, &comp_n, 4);
            memcpy(ckpt_buf + SESSION_ID_LEN + 4, &tot_n, 4);
            memcpy(ckpt_buf + SESSION_ID_LEN + 8, &nlen_n, 2);
            memcpy(ckpt_buf + SESSION_ID_LEN + 10, session->file_list[i], name_len);
            uint64_to_net(session_bytes, ckpt_buf + SESSION_ID_LEN + 10 + name_len);

            send_message(client_fd, MSG_CHECKPOINT, STATUS_OK, ckpt_buf, ckpt_len);
            free(ckpt_buf);
        }

        /* Update server session */
        session_update(&g_session_table, session, i + 1, 0, session_bytes);
        session_persist_server(session, g_topics_dir);

        fprintf(stderr, "[INFO] Completed file %d/%d: %s (%lu bytes)\n",
                i + 1, total_files, session->file_list[i], (unsigned long)file_size);
    }

    /* All files completed! Send TRANSFER_COMPLETE */
    send_message(client_fd, MSG_TRANSFER_COMPLETE, STATUS_END_OF_TRANSFER, NULL, 0);

    *bytes_out = bytes_this_attempt;
    return 0;
}

/* ============================================================
 * Handle a single client connection
 * ============================================================ */
static void handle_client(int client_fd, TopicMap *topic_map,
                          struct sockaddr_in *client_addr) {
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr->sin_addr, client_ip, sizeof(client_ip));

    ThreadRNG rng;
    init_thread_rng(&rng);

    double start_time = get_time_sec();

    MessageHeader hdr;
    uint8_t *payload = NULL;
    uint32_t payload_len = 0;

    if (recv_message(client_fd, &hdr, &payload, &payload_len) < 0) {
        fprintf(stderr, "[ERROR] Failed to receive request from %s\n", client_ip);
        return;
    }

    SessionState *session = NULL;
    int start_file_idx = 0;
    uint64_t start_offset = 0;
    char topic[MAX_TOPIC_LEN] = {0};

    if (hdr.msg_type == MSG_SESSION_INIT) {
        if (payload_len < SESSION_ID_LEN + 2) {
            fprintf(stderr, "[ERROR] Malformed SESSION_INIT payload\n");
            free(payload);
            return;
        }

        char req_session_id[SESSION_ID_LEN + 1];
        memcpy(req_session_id, payload, SESSION_ID_LEN);
        req_session_id[SESSION_ID_LEN] = '\0';

        uint16_t topic_len;
        memcpy(&topic_len, payload + SESSION_ID_LEN, 2);
        topic_len = ntohs(topic_len);

        /* Bounds check topic length against payload_len */
        if ((uint32_t)SESSION_ID_LEN + 2 + topic_len > payload_len) {
            fprintf(stderr, "[ERROR] Malformed SESSION_INIT: topic_len exceeds payload\n");
            free(payload);
            return;
        }

        if (topic_len > MAX_TOPIC_LEN - 1) topic_len = MAX_TOPIC_LEN - 1;
        memcpy(topic, payload + SESSION_ID_LEN + 2, topic_len);
        topic[topic_len] = '\0';
        free(payload);

        int is_all_zeros = 1;
        for (int k = 0; k < SESSION_ID_LEN; k++) {
            if (req_session_id[k] != '0' && req_session_id[k] != '\0') {
                is_all_zeros = 0;
                break;
            }
        }

        /* Check if reattaching existing session */
        if (!is_all_zeros && is_valid_uuid(req_session_id)) {
            session = session_lookup(&g_session_table, req_session_id);
            if (!session) {
                /* Try loading from disk */
                SessionState loaded;
                if (session_load_server(&loaded, g_topics_dir, req_session_id) == 0) {
                    /* Rebuild file_list from topic directory */
                    TopicEntry *entry = lookup_topic(topic_map, loaded.topic);
                    if (entry) {
                        int count = 0;
                        char **files = list_files_in_dir(entry->dir_path, &count);
                        if (files && count == loaded.total_files) {
                            session = session_create(&g_session_table, loaded.topic, files, count);
                            if (session) {
                                snprintf(session->session_id, sizeof(session->session_id), "%s", req_session_id);
                                session->completed_files = loaded.completed_files;
                                session->current_file_idx = loaded.current_file_idx;
                                session->byte_offset = loaded.byte_offset;
                                session->total_bytes_sent = loaded.total_bytes_sent;
                            }
                        } else if (files) {
                            free_file_list(files, count);
                        }
                    }
                }
            }
        }

        if (!session) {
            /* Create new session */
            TopicEntry *entry = lookup_topic(topic_map, topic);
            if (!entry) {
                char err_msg[128];
                snprintf(err_msg, sizeof(err_msg), "Topic not found: %s", topic);
                send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, err_msg, strlen(err_msg));
                return;
            }

            int count = 0;
            char **files = list_files_in_dir(entry->dir_path, &count);
            if (!files || count == 0) {
                send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "No files in topic", 17);
                return;
            }

            session = session_create(&g_session_table, topic, files, count);
            if (!session) {
                send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Server busy", 11);
                free_file_list(files, count);
                return;
            }
            session_persist_server(session, g_topics_dir);
            start_file_idx = 0;
            start_offset = 0;
            fprintf(stderr, "[INFO] Created new session '%s' for topic '%s' (%d files)\n",
                    session->session_id, topic, count);
        } else {
            start_file_idx = session->current_file_idx;
            start_offset = session->byte_offset;
            fprintf(stderr, "[INFO] Reattached session '%s' at file %d, offset %lu\n",
                    session->session_id, start_file_idx, (unsigned long)start_offset);
        }

        /* Send SESSION_ACK */
        uint8_t ack_payload[SESSION_ID_LEN + 4 + 8];
        memcpy(ack_payload, session->session_id, SESSION_ID_LEN);
        uint32_t f_idx_n = htonl((uint32_t)start_file_idx);
        memcpy(ack_payload + SESSION_ID_LEN, &f_idx_n, 4);
        uint64_to_net(start_offset, ack_payload + SESSION_ID_LEN + 4);

        if (send_message(client_fd, MSG_SESSION_ACK, STATUS_OK, ack_payload, sizeof(ack_payload)) < 0) {
            return;
        }

    } else if (hdr.msg_type == MSG_RESUME_REQUEST) {
        if (payload_len < SESSION_ID_LEN + 4 + 8) {
            fprintf(stderr, "[ERROR] Malformed RESUME_REQUEST\n");
            free(payload);
            return;
        }

        char req_session_id[SESSION_ID_LEN + 1];
        memcpy(req_session_id, payload, SESSION_ID_LEN);
        req_session_id[SESSION_ID_LEN] = '\0';

        uint32_t req_file_idx_n;
        memcpy(&req_file_idx_n, payload + SESSION_ID_LEN, 4);
        uint32_t req_file_idx = ntohl(req_file_idx_n);
        uint64_t req_offset = net_to_uint64(payload + SESSION_ID_LEN + 4);
        free(payload);

        /* Validate session id format */
        if (!is_valid_uuid(req_session_id)) {
            fprintf(stderr, "[WARN] Invalid UUID in RESUME_REQUEST\n");
            send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Invalid session ID", 18);
            return;
        }

        session = session_lookup(&g_session_table, req_session_id);
        if (!session) {
            SessionState loaded;
            if (session_load_server(&loaded, g_topics_dir, req_session_id) == 0) {
                /* Rebuild file_list from topic directory with NULL checks */
                TopicEntry *entry = lookup_topic(topic_map, loaded.topic);
                if (entry) {
                    int count = 0;
                    char **files = list_files_in_dir(entry->dir_path, &count);
                    if (files && count == loaded.total_files) {
                        session = session_create(&g_session_table, loaded.topic, files, count);
                        if (session) {
                            snprintf(session->session_id, sizeof(session->session_id), "%s", req_session_id);
                            session->completed_files = loaded.completed_files;
                            session->current_file_idx = loaded.current_file_idx;
                            session->byte_offset = loaded.byte_offset;
                            session->total_bytes_sent = loaded.total_bytes_sent;
                        }
                    } else if (files) {
                        free_file_list(files, count);
                    }
                }
            }
        }

        if (!session) {
            fprintf(stderr, "[WARN] Session '%s' not found for resume\n", req_session_id);
            send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Session not found", 17);
            return;
        }

        /* Validate checkpoint bounds against session */
        snprintf(topic, sizeof(topic), "%s", session->topic);
        if ((int)req_file_idx < session->total_files) {
            start_file_idx = (int)req_file_idx;
            start_offset = req_offset;
        } else {
            start_file_idx = session->current_file_idx;
            start_offset = session->byte_offset;
        }

        pthread_mutex_lock(&g_metrics.lock);
        g_metrics.resumes_handled++;
        pthread_mutex_unlock(&g_metrics.lock);

        fprintf(stderr, "[INFO] Resuming session '%s' at file %d, offset %lu\n",
                session->session_id, start_file_idx, (unsigned long)start_offset);

        /* Send SESSION_ACK */
        uint8_t ack_payload[SESSION_ID_LEN + 4 + 8];
        memcpy(ack_payload, session->session_id, SESSION_ID_LEN);
        uint32_t f_idx_n = htonl((uint32_t)start_file_idx);
        memcpy(ack_payload + SESSION_ID_LEN, &f_idx_n, 4);
        uint64_to_net(start_offset, ack_payload + SESSION_ID_LEN + 4);

        if (send_message(client_fd, MSG_SESSION_ACK, STATUS_OK, ack_payload, sizeof(ack_payload)) < 0) {
            return;
        }

    } else if (hdr.msg_type == MSG_TOPIC_REQUEST) {
        int tlen = (payload_len < MAX_TOPIC_LEN - 1) ? payload_len : MAX_TOPIC_LEN - 1;
        memcpy(topic, payload, tlen);
        topic[tlen] = '\0';
        free(payload);

        TopicEntry *entry = lookup_topic(topic_map, topic);
        if (!entry) {
            send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Topic not found", 15);
            return;
        }

        int count = 0;
        char **files = list_files_in_dir(entry->dir_path, &count);
        if (!files || count == 0) {
            send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "No files in topic", 17);
            return;
        }

        session = session_create(&g_session_table, topic, files, count);
        if (!session) {
            send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Server busy", 11);
            free_file_list(files, count);
            return;
        }
        start_file_idx = 0;
        start_offset = 0;
    } else {
        fprintf(stderr, "[ERROR] Unexpected message type: 0x%02x\n", hdr.msg_type);
        free(payload);
        return;
    }

    TopicEntry *entry = lookup_topic(topic_map, session->topic);
    if (!entry) return;

    /* Copy session_id locally before transfer starts (avoids use-after-remove) */
    char saved_session_id[SESSION_ID_LEN + 1];
    snprintf(saved_session_id, sizeof(saved_session_id), "%s", session->session_id);

    uint64_t bytes_transferred = 0;
    int ret = send_files_session(client_fd, session, entry->dir_path,
                                 start_file_idx, start_offset,
                                 g_failure_probability, &rng,
                                 &bytes_transferred);

    double elapsed = get_time_sec() - start_time;

    /* Update metrics under lock */
    pthread_mutex_lock(&g_metrics.lock);
    g_metrics.total_bytes_sent += bytes_transferred;
    g_metrics.clients_served++;
    if (ret == 0) {
        g_metrics.transfers_completed++;
        g_metrics.total_files_sent += session->total_files;
        /* Session completed successfully: remove and clean up disk state */
        session_remove(&g_session_table, saved_session_id);
        session_delete_server(saved_session_id);
    }
    g_metrics.total_time_seconds += elapsed;
    pthread_mutex_unlock(&g_metrics.lock);

    if (ret == -2) {
        fprintf(stderr, "[INFO] Fault injected. Session '%s' state persisted for resume.\n",
                saved_session_id);
    } else if (ret == 0) {
        fprintf(stderr, "[INFO] Transfer complete for session '%s' in %.2fs\n",
                saved_session_id, elapsed);
    }
}

/* ============================================================
 * Thread entry point
 * ============================================================ */
static void *client_thread_fn(void *arg) {
    ClientArgs *args = (ClientArgs *)arg;
    handle_client(args->client_fd, args->topic_map, &args->client_addr);
    close(args->client_fd);
    free(args);

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
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <port> <topics_root_dir> [failure_probability]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int port = atoi(argv[1]);
    const char *topics_root_dir = argv[2];
    if (argc >= 4) {
        g_failure_probability = atof(argv[3]);
        if (g_failure_probability < 0.0) g_failure_probability = 0.0;
        if (g_failure_probability > 1.0) g_failure_probability = 1.0;
    }

    strncpy(g_topics_dir, topics_root_dir, sizeof(g_topics_dir) - 1);

    struct sigaction sa;
    memset(&sa, 0, sizeof(sa));
    sa.sa_handler = signal_handler;
    sigaction(SIGINT, &sa, NULL);
    sigaction(SIGTERM, &sa, NULL);
    signal(SIGPIPE, SIG_IGN);

    /* Initialize TopicMap */
    if (init_topic_map(&g_topic_map, topics_root_dir) < 0) {
        fprintf(stderr, "[FATAL] Failed to initialize topic map from '%s'\n", topics_root_dir);
        exit(EXIT_FAILURE);
    }
    fprintf(stderr, "[INFO] Loaded %d topics from '%s'\n", g_topic_map.count, topics_root_dir);

    /* Initialize SessionTable */
    session_table_init(&g_session_table);

    /* Socket setup */
    int listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (listen_fd < 0) {
        perror("[FATAL] socket failed");
        exit(EXIT_FAILURE);
    }

    int opt = 1;
    setsockopt(listen_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in server_addr;
    memset(&server_addr, 0, sizeof(server_addr));
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    if (bind(listen_fd, (struct sockaddr *)&server_addr, sizeof(server_addr)) < 0) {
        perror("[FATAL] bind failed");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    if (listen(listen_fd, 64) < 0) {
        perror("[FATAL] listen failed");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    fprintf(stderr, "[INFO] Part 2 Case 2 Server listening on port %d (fail_prob=%.4f)\n",
            port, g_failure_probability);

    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR || !g_running) break;
            perror("[WARN] accept failed");
            continue;
        }
        set_tcp_nodelay(client_fd);

        ClientArgs *args = malloc(sizeof(ClientArgs));
        if (!args) {
            close(client_fd);
            continue;
        }
        args->client_fd = client_fd;
        args->topic_map = &g_topic_map;
        memcpy(&args->client_addr, &client_addr, sizeof(client_addr));

        pthread_mutex_lock(&g_thread_lock);
        g_active_threads++;
        pthread_mutex_unlock(&g_thread_lock);

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread_fn, args) != 0) {
            perror("[WARN] pthread_create failed");
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

    pthread_mutex_lock(&g_metrics.lock);
    fprintf(stderr, "\n[INFO] Server shutdown complete.\n");
    fprintf(stderr, "[METRICS] Total payload sent: %lu bytes\n", (unsigned long)g_metrics.total_bytes_sent);
    fprintf(stderr, "[METRICS] Faults injected:    %lu\n", (unsigned long)g_metrics.total_faults_injected);
    fprintf(stderr, "[METRICS] Resumes handled:    %d\n", g_metrics.resumes_handled);
    fprintf(stderr, "[METRICS] Transfers completed:%d\n", g_metrics.transfers_completed);
    pthread_mutex_unlock(&g_metrics.lock);

    return 0;
}
