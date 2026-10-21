/*
 * session.c - Session management implementation
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Implements JSON-based checkpoint persistence for both server and client.
 * Server: /tmp/cn_sessions/<session_id>.json (isolated from dataset)
 * Client: <output_dir>/.checkpoint_<session_id>.json (atomic write)
 */
#define _FILE_OFFSET_BITS 64

#include "session.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <ctype.h>
#include <time.h>
#include <sys/stat.h>
#include <dirent.h>
#include <unistd.h>

/* ============================================================
 * Session table initialization
 * ============================================================ */
void session_table_init(SessionTable *table) {
    memset(table, 0, sizeof(SessionTable));
    pthread_mutex_init(&table->lock, NULL);
}

/* ============================================================
 * Validate UUID string format
 * Exactly 36 characters: 8-4-4-4-12 hex digits with dashes
 * ============================================================ */
int is_valid_uuid(const char *buf) {
    if (!buf || strlen(buf) != SESSION_ID_LEN) return 0;
    for (int i = 0; i < SESSION_ID_LEN; i++) {
        if (i == 8 || i == 13 || i == 18 || i == 23) {
            if (buf[i] != '-') return 0;
        } else {
            if (!isxdigit((unsigned char)buf[i])) return 0;
        }
    }
    return 1;
}

/* ============================================================
 * UUID v4 generation using /dev/urandom
 * Thread-safe, non-racy, cryptographically strong RFC 4122 v4
 * ============================================================ */
void generate_uuid(char *buf) {
    uint8_t b[16];
    FILE *f = fopen("/dev/urandom", "rb");
    if (f) {
        if (fread(b, 1, 16, f) != 16) {
            for (int i = 0; i < 16; i++) b[i] = (uint8_t)(rand() & 0xFF);
        }
        fclose(f);
    } else {
        for (int i = 0; i < 16; i++) b[i] = (uint8_t)(rand() & 0xFF);
    }

    b[6] = (b[6] & 0x0F) | 0x40; /* Version 4 */
    b[8] = (b[8] & 0x3F) | 0x80; /* Variant RFC 4122 */

    snprintf(buf, SESSION_ID_LEN + 1,
             "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x",
             b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
             b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]);
}

/* ============================================================
 * Create a new session with automatic timeout / eviction
 * ============================================================ */
SessionState *session_create(SessionTable *table, const char *topic,
                             char **file_list, int total_files) {
    pthread_mutex_lock(&table->lock);

    time_t now = time(NULL);

    /* Sweep expired sessions */
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (table->sessions[i].active && (now - table->sessions[i].last_update > SESSION_TIMEOUT_SEC)) {
            if (table->sessions[i].file_list) {
                for (int f = 0; f < table->sessions[i].total_files; f++) {
                    free(table->sessions[i].file_list[f]);
                }
                free(table->sessions[i].file_list);
                table->sessions[i].file_list = NULL;
            }
            session_delete_server(table->sessions[i].session_id);
            table->sessions[i].active = 0;
            table->count--;
        }
    }

    /* If table still full, evict oldest session */
    if (table->count >= MAX_SESSIONS) {
        int oldest_idx = -1;
        time_t oldest_time = now;
        for (int i = 0; i < MAX_SESSIONS; i++) {
            if (table->sessions[i].active && (oldest_idx == -1 || table->sessions[i].last_update < oldest_time)) {
                oldest_idx = i;
                oldest_time = table->sessions[i].last_update;
            }
        }
        if (oldest_idx >= 0) {
            if (table->sessions[oldest_idx].file_list) {
                for (int f = 0; f < table->sessions[oldest_idx].total_files; f++) {
                    free(table->sessions[oldest_idx].file_list[f]);
                }
                free(table->sessions[oldest_idx].file_list);
                table->sessions[oldest_idx].file_list = NULL;
            }
            session_delete_server(table->sessions[oldest_idx].session_id);
            table->sessions[oldest_idx].active = 0;
            table->count--;
        }
    }

    /* Find free slot */
    SessionState *session = NULL;
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (!table->sessions[i].active) {
            session = &table->sessions[i];
            break;
        }
    }

    if (!session) {
        pthread_mutex_unlock(&table->lock);
        return NULL;
    }

    memset(session, 0, sizeof(SessionState));
    generate_uuid(session->session_id);
    snprintf(session->topic, sizeof(session->topic), "%s", topic);
    session->file_list = file_list;
    session->total_files = total_files;
    session->completed_files = 0;
    session->current_file_idx = 0;
    session->byte_offset = 0;
    session->total_bytes_sent = 0;
    session->last_update = now;
    session->active = 1;
    table->count++;

    pthread_mutex_unlock(&table->lock);
    return session;
}

/* ============================================================
 * Look up session by ID
 * ============================================================ */
SessionState *session_lookup(SessionTable *table, const char *session_id) {
    if (!table || !session_id || !is_valid_uuid(session_id)) return NULL;

    pthread_mutex_lock(&table->lock);

    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (table->sessions[i].active &&
            strcmp(table->sessions[i].session_id, session_id) == 0) {
            pthread_mutex_unlock(&table->lock);
            return &table->sessions[i];
        }
    }

    pthread_mutex_unlock(&table->lock);
    return NULL;
}

/* ============================================================
 * Update session checkpoint
 * ============================================================ */
void session_update(SessionTable *table, SessionState *session,
                    int file_idx, uint64_t byte_offset, uint64_t total_bytes) {
    if (!session) return;
    pthread_mutex_lock(&table->lock);
    session->current_file_idx = file_idx;
    session->byte_offset = byte_offset;
    session->total_bytes_sent = total_bytes;
    session->last_update = time(NULL);
    if (byte_offset == 0 && file_idx > 0) {
        session->completed_files = file_idx;
    }
    pthread_mutex_unlock(&table->lock);
}

/* ============================================================
 * Persist session to JSON on server side
 * Written into /tmp/cn_sessions/<session_id>.json
 * Uses temporary file and atomic rename.
 * ============================================================ */
int session_persist_server(const SessionState *session, const char *topics_root_dir) {
    (void)topics_root_dir;
    if (!session || !is_valid_uuid(session->session_id)) return -1;

    mkdir(SERVER_SESSION_DIR, 0777);

    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/%s.json", SERVER_SESSION_DIR, session->session_id);

    char tmppath[MAX_PATH_LEN * 2];
    snprintf(tmppath, sizeof(tmppath), "%s/%s.tmp", SERVER_SESSION_DIR, session->session_id);

    FILE *fp = fopen(tmppath, "w");
    if (!fp) return -1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"session_id\": \"%s\",\n", session->session_id);
    fprintf(fp, "  \"topic\": \"%s\",\n", session->topic);
    fprintf(fp, "  \"total_files\": %d,\n", session->total_files);
    fprintf(fp, "  \"completed_files\": %d,\n", session->completed_files);
    fprintf(fp, "  \"current_file_idx\": %d,\n", session->current_file_idx);
    fprintf(fp, "  \"byte_offset\": %lu,\n", (unsigned long)session->byte_offset);
    fprintf(fp, "  \"total_bytes_sent\": %lu,\n", (unsigned long)session->total_bytes_sent);
    fprintf(fp, "  \"last_update\": %ld\n", (long)session->last_update);
    fprintf(fp, "}\n");

    fflush(fp);
    fclose(fp);

    return rename(tmppath, filepath);
}

/* ============================================================
 * Delete server session file on completion
 * ============================================================ */
void session_delete_server(const char *session_id) {
    if (!session_id || !is_valid_uuid(session_id)) return;
    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/%s.json", SERVER_SESSION_DIR, session_id);
    unlink(filepath);
}

/* ============================================================
 * Load session from disk (server side)
 * ============================================================ */
int session_load_server(SessionState *session, const char *topics_root_dir,
                        const char *session_id) {
    if (!session || !session_id || !is_valid_uuid(session_id)) return -1;

    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/%s.json", SERVER_SESSION_DIR, session_id);

    FILE *fp = fopen(filepath, "r");
    if (!fp && topics_root_dir) {
        /* Fallback check in dataset/.sessions if migrated */
        snprintf(filepath, sizeof(filepath), "%s/.sessions/%s.json", topics_root_dir, session_id);
        fp = fopen(filepath, "r");
    }
    if (!fp) return -1;

    memset(session, 0, sizeof(SessionState));

    char line[512];
    while (fgets(line, sizeof(line), fp)) {
        char *key, *val;
        if ((key = strstr(line, "\"session_id\""))) {
            val = strchr(key + 12, '"');
            if (val) {
                val++;
                char *end = strchr(val, '"');
                if (end) {
                    int len = end - val;
                    if (len > SESSION_ID_LEN) len = SESSION_ID_LEN;
                    memcpy(session->session_id, val, len);
                    session->session_id[len] = '\0';
                }
            }
        } else if ((key = strstr(line, "\"topic\""))) {
            val = strchr(key + 7, '"');
            if (val) {
                val++;
                char *end = strchr(val, '"');
                if (end) {
                    int len = end - val;
                    if (len > MAX_TOPIC_LEN - 1) len = MAX_TOPIC_LEN - 1;
                    memcpy(session->topic, val, len);
                    session->topic[len] = '\0';
                }
            }
        } else if ((key = strstr(line, "\"total_files\""))) {
            val = strchr(key + 13, ':');
            if (val) session->total_files = atoi(val + 1);
        } else if ((key = strstr(line, "\"completed_files\""))) {
            val = strchr(key + 17, ':');
            if (val) session->completed_files = atoi(val + 1);
        } else if ((key = strstr(line, "\"current_file_idx\""))) {
            val = strchr(key + 18, ':');
            if (val) session->current_file_idx = atoi(val + 1);
        } else if ((key = strstr(line, "\"byte_offset\""))) {
            val = strchr(key + 13, ':');
            if (val) session->byte_offset = strtoull(val + 1, NULL, 10);
        } else if ((key = strstr(line, "\"total_bytes_sent\""))) {
            val = strchr(key + 18, ':');
            if (val) session->total_bytes_sent = strtoull(val + 1, NULL, 10);
        } else if ((key = strstr(line, "\"last_update\""))) {
            val = strchr(key + 13, ':');
            if (val) session->last_update = atol(val + 1);
        }
    }

    fclose(fp);
    session->active = 1;
    return 0;
}

/* ============================================================
 * Remove session
 * ============================================================ */
void session_remove(SessionTable *table, const char *session_id) {
    if (!table || !session_id) return;
    pthread_mutex_lock(&table->lock);
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (table->sessions[i].active &&
            strcmp(table->sessions[i].session_id, session_id) == 0) {
            /* Free the file list to prevent memory leak */
            if (table->sessions[i].file_list) {
                for (int f = 0; f < table->sessions[i].total_files; f++) {
                    free(table->sessions[i].file_list[f]);
                }
                free(table->sessions[i].file_list);
                table->sessions[i].file_list = NULL;
            }
            table->sessions[i].active = 0;
            table->count--;
            break;
        }
    }
    pthread_mutex_unlock(&table->lock);
}

/* ============================================================
 * Client-side checkpoint save (atomic write via tmp file)
 * Written to <output_dir>/.checkpoint_<session_id>.json
 * ============================================================ */
int checkpoint_save_client(const char *output_dir, const char *session_id,
                           const char *topic, int file_idx, uint64_t byte_offset,
                           int completed_files, int total_files, uint64_t total_bytes) {
    if (!output_dir || !session_id || !is_valid_uuid(session_id)) return -1;

    char filepath[MAX_PATH_LEN * 4];
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s.json", output_dir, session_id);

    char tmppath[MAX_PATH_LEN * 4];
    snprintf(tmppath, sizeof(tmppath), "%s/.checkpoint_%s.tmp", output_dir, session_id);

    FILE *fp = fopen(tmppath, "w");
    if (!fp) return -1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"session_id\": \"%s\",\n", session_id);
    fprintf(fp, "  \"topic\": \"%s\",\n", topic ? topic : "");
    fprintf(fp, "  \"completed_files\": %d,\n", completed_files);
    fprintf(fp, "  \"total_files\": %d,\n", total_files);
    fprintf(fp, "  \"current_file_idx\": %d,\n", file_idx);
    fprintf(fp, "  \"byte_offset\": %lu,\n", (unsigned long)byte_offset);
    fprintf(fp, "  \"total_bytes_received\": %lu\n", (unsigned long)total_bytes);
    fprintf(fp, "}\n");

    fflush(fp);
    fclose(fp);

    return rename(tmppath, filepath);
}

/* ============================================================
 * Client-side checkpoint load
 * ============================================================ */
int checkpoint_load_client(const char *output_dir, const char *session_id,
                           char *topic, int *file_idx, uint64_t *byte_offset,
                           int *completed_files, int *total_files, uint64_t *total_bytes) {
    if (!output_dir || !session_id || !is_valid_uuid(session_id)) return -1;

    char filepath[MAX_PATH_LEN * 4];
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s.json", output_dir, session_id);

    FILE *fp = fopen(filepath, "r");
    if (!fp) return -1;

    char line[512];
    while (fgets(line, sizeof(line), fp)) {
        char *key, *val;
        if ((key = strstr(line, "\"topic\""))) {
            val = strchr(key + 7, '"');
            if (val) {
                val++;
                char *end = strchr(val, '"');
                if (end && topic) {
                    int len = end - val;
                    if (len > MAX_TOPIC_LEN - 1) len = MAX_TOPIC_LEN - 1;
                    memcpy(topic, val, len);
                    topic[len] = '\0';
                }
            }
        } else if ((key = strstr(line, "\"current_file_idx\""))) {
            val = strchr(key + 18, ':');
            if (val && file_idx) *file_idx = atoi(val + 1);
        } else if ((key = strstr(line, "\"byte_offset\""))) {
            val = strchr(key + 13, ':');
            if (val && byte_offset) *byte_offset = strtoull(val + 1, NULL, 10);
        } else if ((key = strstr(line, "\"completed_files\""))) {
            val = strchr(key + 17, ':');
            if (val && completed_files) *completed_files = atoi(val + 1);
        } else if ((key = strstr(line, "\"total_files\""))) {
            val = strchr(key + 13, ':');
            if (val && total_files) *total_files = atoi(val + 1);
        } else if ((key = strstr(line, "\"total_bytes_received\""))) {
            val = strchr(key + 21, ':');
            if (val && total_bytes) *total_bytes = strtoull(val + 1, NULL, 10);
        }
    }

    fclose(fp);
    return 0;
}

/* ============================================================
 * Find checkpoint for a topic (scan for .checkpoint_*.json files)
 * ============================================================ */
int checkpoint_find_for_topic(const char *output_dir, const char *topic,
                              char *session_id_out) {
    DIR *dir = opendir(output_dir);
    if (!dir) return -1;

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (strncmp(entry->d_name, ".checkpoint_", 12) == 0 &&
            strstr(entry->d_name, ".json") != NULL) {
            /* Extract session ID from filename */
            const char *start = entry->d_name + 12;
            const char *end = strstr(start, ".json");
            if (end && (end - start) == SESSION_ID_LEN) {
                char sid[SESSION_ID_LEN + 1];
                memcpy(sid, start, SESSION_ID_LEN);
                sid[SESSION_ID_LEN] = '\0';

                if (is_valid_uuid(sid)) {
                    char found_topic[MAX_TOPIC_LEN] = {0};
                    int fi = 0;
                    uint64_t bo = 0;
                    int cf = 0, tf = 0;
                    uint64_t tb = 0;
                    if (checkpoint_load_client(output_dir, sid, found_topic,
                                               &fi, &bo, &cf, &tf, &tb) == 0) {
                        if (strcmp(found_topic, topic) == 0) {
                            strcpy(session_id_out, sid);
                            closedir(dir);
                            return 0;
                        }
                    }
                }
            }
        }
    }
    closedir(dir);
    return -1;
}

/* ============================================================
 * Delete client checkpoint
 * ============================================================ */
void checkpoint_delete_client(const char *output_dir, const char *session_id) {
    if (!output_dir || !session_id || !is_valid_uuid(session_id)) return;
    char filepath[MAX_PATH_LEN * 4];
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s.json", output_dir, session_id);
    unlink(filepath);
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s.tmp", output_dir, session_id);
    unlink(filepath);
}
