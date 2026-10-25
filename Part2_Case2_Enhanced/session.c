/*
 * session.c - Session and stream checkpoint implementation (Enhanced)
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
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

void session_table_init(SessionTable *table) {
    memset(table, 0, sizeof(SessionTable));
    pthread_mutex_init(&table->lock, NULL);
}

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

SessionState *session_create(SessionTable *table, const char *topic,
                             char **file_list, int total_files) {
    pthread_mutex_lock(&table->lock);
    time_t now = time(NULL);

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

void session_delete_server(const char *session_id) {
    if (!session_id || !is_valid_uuid(session_id)) return;
    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/%s.json", SERVER_SESSION_DIR, session_id);
    unlink(filepath);
}

int session_load_server(SessionState *session, const char *topics_root_dir,
                        const char *session_id) {
    if (!session || !session_id || !is_valid_uuid(session_id)) return -1;

    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/%s.json", SERVER_SESSION_DIR, session_id);

    FILE *fp = fopen(filepath, "r");
    if (!fp && topics_root_dir) {
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

void session_remove(SessionTable *table, const char *session_id) {
    if (!table || !session_id) return;
    pthread_mutex_lock(&table->lock);
    for (int i = 0; i < MAX_SESSIONS; i++) {
        if (table->sessions[i].active &&
            strcmp(table->sessions[i].session_id, session_id) == 0) {
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
 * Client stream checkpoint persistence
 * ============================================================ */
int stream_checkpoint_save(const char *output_dir, const char *group_id, int conn_id,
                           const char *topic, int stream_file_idx, uint64_t byte_offset,
                           int completed_files, int total_assigned, uint64_t useful_bytes) {
    if (!output_dir || !group_id || !is_valid_uuid(group_id)) return -1;

    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s_conn%d.json", output_dir, group_id, conn_id);

    char tmppath[MAX_PATH_LEN * 2];
    snprintf(tmppath, sizeof(tmppath), "%s/.checkpoint_%s_conn%d.tmp", output_dir, group_id, conn_id);

    FILE *fp = fopen(tmppath, "w");
    if (!fp) return -1;

    fprintf(fp, "{\n");
    fprintf(fp, "  \"group_id\": \"%s\",\n", group_id);
    fprintf(fp, "  \"conn_id\": %d,\n", conn_id);
    fprintf(fp, "  \"topic\": \"%s\",\n", topic ? topic : "");
    fprintf(fp, "  \"stream_file_idx\": %d,\n", stream_file_idx);
    fprintf(fp, "  \"byte_offset\": %lu,\n", (unsigned long)byte_offset);
    fprintf(fp, "  \"completed_files\": %d,\n", completed_files);
    fprintf(fp, "  \"total_assigned\": %d,\n", total_assigned);
    fprintf(fp, "  \"useful_bytes\": %lu\n", (unsigned long)useful_bytes);
    fprintf(fp, "}\n");

    fflush(fp);
    fclose(fp);

    return rename(tmppath, filepath);
}

int stream_checkpoint_load(const char *output_dir, const char *group_id, int conn_id,
                           char *topic, int *stream_file_idx, uint64_t *byte_offset,
                           int *completed_files, int *total_assigned, uint64_t *useful_bytes) {
    if (!output_dir || !group_id || !is_valid_uuid(group_id)) return -1;

    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s_conn%d.json", output_dir, group_id, conn_id);

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
        } else if ((key = strstr(line, "\"stream_file_idx\""))) {
            val = strchr(key + 17, ':');
            if (val && stream_file_idx) *stream_file_idx = atoi(val + 1);
        } else if ((key = strstr(line, "\"byte_offset\""))) {
            val = strchr(key + 13, ':');
            if (val && byte_offset) *byte_offset = strtoull(val + 1, NULL, 10);
        } else if ((key = strstr(line, "\"completed_files\""))) {
            val = strchr(key + 17, ':');
            if (val && completed_files) *completed_files = atoi(val + 1);
        } else if ((key = strstr(line, "\"total_assigned\""))) {
            val = strchr(key + 16, ':');
            if (val && total_assigned) *total_assigned = atoi(val + 1);
        } else if ((key = strstr(line, "\"useful_bytes\""))) {
            val = strchr(key + 14, ':');
            if (val && useful_bytes) *useful_bytes = strtoull(val + 1, NULL, 10);
        }
    }
    fclose(fp);
    return 0;
}

void stream_checkpoint_delete(const char *output_dir, const char *group_id, int conn_id) {
    if (!output_dir || !group_id || !is_valid_uuid(group_id)) return;
    char filepath[MAX_PATH_LEN * 2];
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s_conn%d.json", output_dir, group_id, conn_id);
    unlink(filepath);
    snprintf(filepath, sizeof(filepath), "%s/.checkpoint_%s_conn%d.tmp", output_dir, group_id, conn_id);
    unlink(filepath);
}
