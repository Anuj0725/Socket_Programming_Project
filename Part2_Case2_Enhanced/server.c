/*
 * server.c - Part 2 Case 2 Enhanced: Parallel Connections + Fault Tolerance
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Implements parallel TCP transfer with round-robin file assignment,
 * non-blocking poll-driven I/O, per-stream checkpoint resume,
 * and fault injection per design doc Section 3.4 & 6.
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
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <netinet/in.h>
#include <arpa/inet.h>

#include "protocol.h"
#include "topic_map.h"
#include "session.h"

#define MAX_PARALLEL_GROUPS 64
#define GROUP_TIMEOUT_SEC   300

/* Parallel transfer group coordination */
typedef struct {
    char group_id[SESSION_ID_LEN + 1];
    char topic[MAX_TOPIC_LEN];
    char **file_list;
    int total_files;
    int num_connections;
    int connections_joined;
    int connections_finished;
    time_t last_activity;
    int active;
    pthread_mutex_t lock;
} ParallelGroup;

static ParallelGroup g_groups[MAX_PARALLEL_GROUPS];
static pthread_mutex_t g_group_table_lock = PTHREAD_MUTEX_INITIALIZER;

static TopicMap g_topic_map;
static volatile int g_running = 1;
static double g_failure_probability = 0.0;
static char g_topics_dir[MAX_PATH_LEN];

/* Global metrics */
typedef struct {
    uint64_t total_bytes_sent;
    uint64_t total_files_sent;
    uint64_t total_faults_injected;
    int      clients_served;
    int      parallel_transfers_completed;
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
    struct sockaddr_in client_addr;
} ClientArgs;

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

static void signal_handler(int sig) {
    (void)sig;
    g_running = 0;
}

static double get_time_sec(void) {
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec + ts.tv_nsec / 1e9;
}

/* Thread-local RNG for fault injection */
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
 * Non-blocking send helper using poll() (single buffer send)
 * ============================================================ */
static int send_all_poll(int fd, const void *buf, size_t n, int timeout_ms) {
    size_t total_sent = 0;
    const uint8_t *p = (const uint8_t *)buf;

    while (total_sent < n) {
        struct pollfd pfd;
        pfd.fd = fd;
        pfd.events = POLLOUT;
        pfd.revents = 0;

        int ret = poll(&pfd, 1, timeout_ms);
        if (ret < 0) {
            if (errno == EINTR) continue;
            return -1;
        } else if (ret == 0) {
            return -1; /* Timeout */
        }

        if (pfd.revents & (POLLERR | POLLHUP | POLLNVAL)) {
            return -1;
        }

        if (pfd.revents & POLLOUT) {
            ssize_t sent = send(fd, p + total_sent, n - total_sent, MSG_NOSIGNAL);
            if (sent <= 0) {
                if (errno == EAGAIN || errno == EWOULDBLOCK) continue;
                return -1;
            }
            total_sent += (size_t)sent;
        }
    }
    return 0;
}

/* Single poll + send per message */
static int send_message_poll(int fd, uint8_t msg_type, uint8_t status,
                             const void *payload, uint32_t payload_len) {
    MessageHeader hdr;
    memset(&hdr, 0, sizeof(hdr));
    hdr.msg_type = msg_type;
    hdr.payload_length = htonl(payload_len);
    hdr.status_code = status;

    if (payload_len == 0 || payload == NULL) {
        return send_all_poll(fd, &hdr, HEADER_SIZE, 5000);
    }

    size_t total_len = HEADER_SIZE + (size_t)payload_len;
    if (total_len <= 65536 + HEADER_SIZE) {
        uint8_t buf[HEADER_SIZE + 65536];
        memcpy(buf, &hdr, HEADER_SIZE);
        memcpy(buf + HEADER_SIZE, payload, payload_len);
        return send_all_poll(fd, buf, total_len, 5000);
    } else {
        uint8_t *buf = (uint8_t *)malloc(total_len);
        if (!buf) return -1;
        memcpy(buf, &hdr, HEADER_SIZE);
        memcpy(buf + HEADER_SIZE, payload, payload_len);
        int ret = send_all_poll(fd, buf, total_len, 5000);
        free(buf);
        return ret;
    }
}

/* ============================================================
 * Group coordination lookup / creation (proper error handling & expiry)
 * ============================================================ */
static ParallelGroup *find_or_create_group(const char *group_id, const char *topic,
                                           int num_conns, int req_conn_id, int *conn_id_out) {
    pthread_mutex_lock(&g_group_table_lock);
    time_t now = time(NULL);

    /* Sweep expired groups */
    for (int i = 0; i < MAX_PARALLEL_GROUPS; i++) {
        if (g_groups[i].active && (now - g_groups[i].last_activity > GROUP_TIMEOUT_SEC)) {
            if (g_groups[i].file_list) {
                free_file_list(g_groups[i].file_list, g_groups[i].total_files);
                g_groups[i].file_list = NULL;
            }
            g_groups[i].active = 0;
        }
    }

    int is_zero = 1;
    for (int i = 0; i < SESSION_ID_LEN; i++) {
        if (group_id[i] != '0' && group_id[i] != '\0') {
            is_zero = 0;
            break;
        }
    }

    if (!is_zero) {
        /* Existing group lookup - do not create ghost group if unknown */
        if (!is_valid_uuid(group_id)) {
            pthread_mutex_unlock(&g_group_table_lock);
            return NULL;
        }

        for (int i = 0; i < MAX_PARALLEL_GROUPS; i++) {
            if (g_groups[i].active && strcmp(g_groups[i].group_id, group_id) == 0) {
                pthread_mutex_lock(&g_groups[i].lock);
                int conn_id;
                if (req_conn_id >= 0) {
                    conn_id = req_conn_id;
                } else {
                    conn_id = g_groups[i].connections_joined++;
                }
                g_groups[i].last_activity = now;
                pthread_mutex_unlock(&g_groups[i].lock);

                *conn_id_out = conn_id;
                pthread_mutex_unlock(&g_group_table_lock);
                return &g_groups[i];
            }
        }

        /* Not found */
        pthread_mutex_unlock(&g_group_table_lock);
        return NULL;
    }

    /* Request for new group (is_zero == 1) */
    TopicEntry *entry = lookup_topic(&g_topic_map, topic);
    if (!entry) {
        pthread_mutex_unlock(&g_group_table_lock);
        return NULL;
    }

    int count = 0;
    char **files = list_files_in_dir(entry->dir_path, &count);
    if (!files || count == 0) {
        pthread_mutex_unlock(&g_group_table_lock);
        return NULL;
    }

    /* Find empty group slot */
    ParallelGroup *grp = NULL;
    for (int i = 0; i < MAX_PARALLEL_GROUPS; i++) {
        if (!g_groups[i].active) {
            grp = &g_groups[i];
            break;
        }
    }

    if (!grp) {
        free_file_list(files, count);
        pthread_mutex_unlock(&g_group_table_lock);
        return NULL;
    }

    memset(grp, 0, sizeof(ParallelGroup));
    pthread_mutex_init(&grp->lock, NULL);
    generate_uuid(grp->group_id);
    size_t tlen = strlen(topic);
    if (tlen >= sizeof(grp->topic)) tlen = sizeof(grp->topic) - 1;
    memcpy(grp->topic, topic, tlen);
    grp->topic[tlen] = '\0';
    grp->num_connections = (num_conns > 0) ? num_conns : DEFAULT_PARALLEL_CONNS;
    grp->file_list = files;
    grp->total_files = count;
    grp->connections_joined = 0;
    grp->connections_finished = 0;
    grp->last_activity = now;
    grp->active = 1;
    *conn_id_out = -1;

    pthread_mutex_unlock(&g_group_table_lock);
    return grp;
}

/* ============================================================
 * Handle a single parallel connection
 * ============================================================ */
static void handle_client(int client_fd, struct sockaddr_in *client_addr) {
    char client_ip[INET_ADDRSTRLEN];
    inet_ntop(AF_INET, &client_addr->sin_addr, client_ip, sizeof(client_ip));

    ThreadRNG rng;
    init_thread_rng(&rng);

    double start_time = get_time_sec();

    MessageHeader hdr;
    uint8_t *payload = NULL;
    uint32_t payload_len = 0;

    if (recv_message(client_fd, &hdr, &payload, &payload_len) < 0) {
        return;
    }

    char group_id_req[SESSION_ID_LEN + 1] = {0};
    char topic[MAX_TOPIC_LEN] = {0};
    int num_conns = DEFAULT_PARALLEL_CONNS;
    int req_conn_id = -1;
    uint32_t resume_file_idx = 0;
    uint64_t resume_offset = 0;

    if (hdr.msg_type == MSG_PARALLEL_INIT) {
        if (payload_len < SESSION_ID_LEN + 3) {
            free(payload);
            return;
        }

        memcpy(group_id_req, payload, SESSION_ID_LEN);
        group_id_req[SESSION_ID_LEN] = '\0';

        uint16_t t_len;
        memcpy(&t_len, payload + SESSION_ID_LEN, 2);
        t_len = ntohs(t_len);

        /* Bounds check topic length against payload_len */
        if ((uint32_t)SESSION_ID_LEN + 2 + t_len > payload_len) {
            fprintf(stderr, "[ERROR] PARALLEL_INIT payload too short for topic\n");
            free(payload);
            return;
        }

        if (t_len > MAX_TOPIC_LEN - 1) t_len = MAX_TOPIC_LEN - 1;
        memcpy(topic, payload + SESSION_ID_LEN + 2, t_len);
        topic[t_len] = '\0';

        if (payload_len >= (uint32_t)(SESSION_ID_LEN + 2 + t_len + 1)) {
            num_conns = payload[SESSION_ID_LEN + 2 + t_len];
        }
        if (payload_len >= (uint32_t)(SESSION_ID_LEN + 2 + t_len + 2)) {
            req_conn_id = (int8_t)payload[SESSION_ID_LEN + 2 + t_len + 1];
        }

        /* Parse resume file index and byte offset per stream */
        if (payload_len >= (uint32_t)(SESSION_ID_LEN + 2 + t_len + 2 + 4)) {
            uint32_t r_fidx_n;
            memcpy(&r_fidx_n, payload + SESSION_ID_LEN + 2 + t_len + 2, 4);
            resume_file_idx = ntohl(r_fidx_n);
        }
        if (payload_len >= (uint32_t)(SESSION_ID_LEN + 2 + t_len + 2 + 4 + 8)) {
            resume_offset = net_to_uint64(payload + SESSION_ID_LEN + 2 + t_len + 2 + 4);
        }

        free(payload);

    } else if (hdr.msg_type == MSG_TOPIC_REQUEST || hdr.msg_type == MSG_SESSION_INIT) {
        if (hdr.msg_type == MSG_TOPIC_REQUEST) {
            int tlen = (payload_len < MAX_TOPIC_LEN - 1) ? payload_len : MAX_TOPIC_LEN - 1;
            memcpy(topic, payload, tlen);
            topic[tlen] = '\0';
        } else {
            uint16_t tlen;
            memcpy(&tlen, payload + SESSION_ID_LEN, 2);
            tlen = ntohs(tlen);
            if ((uint32_t)SESSION_ID_LEN + 2 + tlen > payload_len) {
                free(payload);
                return;
            }
            if (tlen > MAX_TOPIC_LEN - 1) tlen = MAX_TOPIC_LEN - 1;
            memcpy(topic, payload + SESSION_ID_LEN + 2, tlen);
            topic[tlen] = '\0';
        }
        free(payload);
        num_conns = 1;
        req_conn_id = 0;
    } else {
        free(payload);
        return;
    }

    int conn_id = 0;
    ParallelGroup *grp = find_or_create_group(group_id_req, topic, num_conns, req_conn_id, &conn_id);
    if (!grp) {
        /* Return error for unknown/expired group IDs */
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Group not found or expired", 26);
        return;
    }

    /* Send SESSION_ACK acknowledging the group and resume point */
    uint8_t ack_payload[SESSION_ID_LEN + 4 + 8];
    memcpy(ack_payload, grp->group_id, SESSION_ID_LEN);
    uint32_t f_idx = htonl((conn_id < 0) ? (uint32_t)-1 : resume_file_idx);
    memcpy(ack_payload + SESSION_ID_LEN, &f_idx, 4);
    uint64_to_net(resume_offset, ack_payload + SESSION_ID_LEN + 4);
    send_message(client_fd, MSG_SESSION_ACK, STATUS_OK, ack_payload, sizeof(ack_payload));

    if (conn_id < 0) {
        /* Group registration connection complete; worker streams will connect next */
        return;
    }

    /*
     * Round-robin file assignment:
     * Assign indices where (idx % grp->num_connections) == conn_id
     * Dynamically allocate assigned_indices to prevent stack overflow!
     */
    int *assigned_indices = (int *)malloc(sizeof(int) * (size_t)(grp->total_files + 1));
    if (!assigned_indices) {
        send_message(client_fd, MSG_ERROR_RESPONSE, STATUS_ERROR, "Server memory error", 19);
        return;
    }

    int assigned_count = 0;
    for (int i = 0; i < grp->total_files; i++) {
        if ((i % grp->num_connections) == conn_id) {
            assigned_indices[assigned_count++] = i;
        }
    }

    /* Send FILE_ASSIGNMENT message (0x21): assigned_count (2B) + file_indices (4B each) */
    uint32_t assign_payload_len = 2 + (uint32_t)assigned_count * 4;
    uint8_t *assign_buf = malloc(assign_payload_len);
    if (!assign_buf) {
        free(assigned_indices);
        return;
    }

    uint16_t fc_n = htons((uint16_t)assigned_count);
    memcpy(assign_buf, &fc_n, 2);
    for (int k = 0; k < assigned_count; k++) {
        uint32_t idx_n = htonl((uint32_t)assigned_indices[k]);
        memcpy(assign_buf + 2 + k * 4, &idx_n, 4);
    }

    send_message(client_fd, MSG_FILE_ASSIGNMENT, STATUS_OK, assign_buf, assign_payload_len);
    free(assign_buf);

    fprintf(stderr, "[ENHANCED] Conn %d/%d assigned %d files (group '%s', resume from file %u, offset %lu)\n",
            conn_id + 1, grp->num_connections, assigned_count, grp->group_id,
            resume_file_idx, (unsigned long)resume_offset);

    /* Set socket to non-blocking for poll-driven send */
    int flags = fcntl(client_fd, F_GETFL, 0);
    fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

    TopicEntry *entry = lookup_topic(&g_topic_map, grp->topic);
    if (!entry) {
        free(assigned_indices);
        return;
    }

    uint64_t bytes_transferred = 0;
    int start_k = (resume_file_idx < (uint32_t)assigned_count) ? (int)resume_file_idx : 0;

    for (int k = start_k; k < assigned_count; k++) {
        int file_idx = assigned_indices[k];
        char filepath[MAX_PATH_LEN * 2];
        snprintf(filepath, sizeof(filepath), "%s/%s", entry->dir_path, grp->file_list[file_idx]);

        /* Stat and open file BEFORE sending FILE_HEADER */
        struct stat st;
        if (stat(filepath, &st) != 0) continue;
        uint64_t file_size = (uint64_t)st.st_size;

        uint64_t cur_offset = 0;
        if (k == start_k && resume_offset > 0) {
            if (resume_offset <= file_size) {
                cur_offset = resume_offset;
            }
        }

        FILE *fp = fopen(filepath, "rb");
        if (!fp) continue;

        /* Seek to offset on resume */
        if (cur_offset > 0) {
            if (fseeko(fp, (off_t)cur_offset, SEEK_SET) != 0) {
                fprintf(stderr, "[ERROR] fseek to %lu failed on %s\n", (unsigned long)cur_offset, filepath);
                fclose(fp);
                free(assigned_indices);
                return;
            }
            fprintf(stderr, "[ENHANCED RESUME] Stream %d resuming '%s' from byte offset %lu/%lu\n",
                    conn_id, grp->file_list[file_idx], (unsigned long)cur_offset, (unsigned long)file_size);
        }

        /* Send FILE_HEADER */
        uint16_t name_len = (uint16_t)strlen(grp->file_list[file_idx]);
        uint32_t hdr_len = 2 + name_len + 8;
        uint8_t *hdr_buf = malloc(hdr_len);
        if (!hdr_buf) {
            fclose(fp);
            continue;
        }

        uint16_t nlen_n = htons(name_len);
        memcpy(hdr_buf, &nlen_n, 2);
        memcpy(hdr_buf + 2, grp->file_list[file_idx], name_len);
        uint64_to_net(file_size, hdr_buf + 2 + name_len);

        if (send_message_poll(client_fd, MSG_FILE_HEADER, STATUS_OK, hdr_buf, hdr_len) < 0) {
            free(hdr_buf);
            fclose(fp);
            free(assigned_indices);
            return;
        }
        free(hdr_buf);

        uint8_t buf[CHUNK_SIZE];
        size_t n;
        while ((n = fread(buf, 1, CHUNK_SIZE, fp)) > 0) {
            if (send_message_poll(client_fd, MSG_FILE_DATA, STATUS_OK, buf, (uint32_t)n) < 0) {
                fclose(fp);
                free(assigned_indices);
                return;
            }
            bytes_transferred += n;
            cur_offset += n;

            if (maybe_inject_fault(&rng, g_failure_probability)) {
                fprintf(stderr, "[FAULT] Injected fault on parallel conn %d, file '%s' (offset %lu/%lu)\n",
                        conn_id, grp->file_list[file_idx], (unsigned long)cur_offset, (unsigned long)file_size);
                pthread_mutex_lock(&g_metrics.lock);
                g_metrics.total_faults_injected++;
                pthread_mutex_unlock(&g_metrics.lock);
                fclose(fp);
                free(assigned_indices);
                return; /* Clean exit on fault path */
            }
        }
        fclose(fp);

        /* Send CHECKPOINT notification to client */
        uint32_t ckpt_len = SESSION_ID_LEN + 4 + 4 + 2 + name_len + 8;
        uint8_t *ckpt_buf = malloc(ckpt_len);
        if (ckpt_buf) {
            memcpy(ckpt_buf, grp->group_id, SESSION_ID_LEN);
            uint32_t comp_n = htonl(k + 1);
            uint32_t tot_n = htonl(assigned_count);
            uint16_t n_len_n = htons(name_len);
            memcpy(ckpt_buf + SESSION_ID_LEN, &comp_n, 4);
            memcpy(ckpt_buf + SESSION_ID_LEN + 4, &tot_n, 4);
            memcpy(ckpt_buf + SESSION_ID_LEN + 8, &n_len_n, 2);
            memcpy(ckpt_buf + SESSION_ID_LEN + 10, grp->file_list[file_idx], name_len);
            uint64_to_net(bytes_transferred, ckpt_buf + SESSION_ID_LEN + 10 + name_len);

            send_message_poll(client_fd, MSG_CHECKPOINT, STATUS_OK, ckpt_buf, ckpt_len);
            free(ckpt_buf);
        }
    }

    /* Sub-connection completed all assigned files */
    send_message_poll(client_fd, MSG_TRANSFER_COMPLETE, STATUS_END_OF_TRANSFER, NULL, 0);

    /* Free assigned indices */
    free(assigned_indices);

    double elapsed = get_time_sec() - start_time;

    pthread_mutex_lock(&g_metrics.lock);
    g_metrics.total_bytes_sent += bytes_transferred;
    g_metrics.clients_served++;
    g_metrics.total_time_seconds += elapsed;
    pthread_mutex_unlock(&g_metrics.lock);

    pthread_mutex_lock(&grp->lock);
    grp->connections_finished++;
    if (grp->connections_finished >= grp->num_connections) {
        pthread_mutex_lock(&g_metrics.lock);
        g_metrics.parallel_transfers_completed++;
        pthread_mutex_unlock(&g_metrics.lock);
        fprintf(stderr, "[INFO] All %d parallel streams finished for group '%s'!\n",
                grp->num_connections, grp->group_id);
        if (grp->file_list) {
            free_file_list(grp->file_list, grp->total_files);
            grp->file_list = NULL;
        }
        grp->active = 0;
    }
    pthread_mutex_unlock(&grp->lock);
}

static void *client_thread_fn(void *arg) {
    ClientArgs *args = (ClientArgs *)arg;
    handle_client(args->client_fd, &args->client_addr);
    close(args->client_fd);
    free(args);

    pthread_mutex_lock(&g_thread_lock);
    g_active_threads--;
    pthread_cond_broadcast(&g_thread_cond);
    pthread_mutex_unlock(&g_thread_lock);

    return NULL;
}

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

    if (init_topic_map(&g_topic_map, topics_root_dir) < 0) {
        fprintf(stderr, "[FATAL] Failed to load topics from '%s'\n", topics_root_dir);
        exit(EXIT_FAILURE);
    }
    fprintf(stderr, "[INFO] Loaded %d topics from '%s'\n", g_topic_map.count, topics_root_dir);

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

    if (listen(listen_fd, 128) < 0) {
        perror("[FATAL] listen failed");
        close(listen_fd);
        exit(EXIT_FAILURE);
    }

    fprintf(stderr, "[INFO] Part 2 Case 2 Enhanced Server listening on port %d (fail_prob=%.4f, non-blocking poll enabled)\n",
            port, g_failure_probability);

    while (g_running) {
        struct sockaddr_in client_addr;
        socklen_t addr_len = sizeof(client_addr);
        int client_fd = accept(listen_fd, (struct sockaddr *)&client_addr, &addr_len);
        if (client_fd < 0) {
            if (errno == EINTR || !g_running) break;
            continue;
        }
        set_tcp_nodelay(client_fd);

        ClientArgs *args = malloc(sizeof(ClientArgs));
        if (!args) {
            close(client_fd);
            continue;
        }
        args->client_fd = client_fd;
        memcpy(&args->client_addr, &client_addr, sizeof(client_addr));

        pthread_mutex_lock(&g_thread_lock);
        g_active_threads++;
        pthread_mutex_unlock(&g_thread_lock);

        pthread_t tid;
        if (pthread_create(&tid, NULL, client_thread_fn, args) != 0) {
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
    fprintf(stderr, "\n[INFO] Enhanced Server shutdown complete.\n");
    fprintf(stderr, "[METRICS] Total payload sent: %lu bytes\n", (unsigned long)g_metrics.total_bytes_sent);
    fprintf(stderr, "[METRICS] Faults injected:    %lu\n", (unsigned long)g_metrics.total_faults_injected);
    fprintf(stderr, "[METRICS] Parallel transfers completed: %d\n", g_metrics.parallel_transfers_completed);
    pthread_mutex_unlock(&g_metrics.lock);

    return 0;
}
