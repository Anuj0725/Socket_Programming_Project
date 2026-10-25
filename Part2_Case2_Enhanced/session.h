/*
 * session.h - Session and stream checkpoint management (Enhanced)
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 */

#ifndef SESSION_H
#define SESSION_H

#define _FILE_OFFSET_BITS 64

#include "protocol.h"
#include <stdint.h>
#include <time.h>
#include <pthread.h>

#define MAX_SESSIONS 256
#define SERVER_SESSION_DIR "/tmp/cn_sessions"
#define SESSION_TIMEOUT_SEC 300

/* Session state structure */
typedef struct {
    char     session_id[SESSION_ID_LEN + 1];
    char     topic[MAX_TOPIC_LEN];
    char   **file_list;
    int      total_files;
    int      completed_files;
    int      current_file_idx;
    uint64_t byte_offset;
    uint64_t total_bytes_sent;
    time_t   last_update;
    int      active;
} SessionState;

typedef struct {
    SessionState sessions[MAX_SESSIONS];
    int count;
    pthread_mutex_t lock;
} SessionTable;

void session_table_init(SessionTable *table);
int is_valid_uuid(const char *buf);
void generate_uuid(char *buf);

SessionState *session_create(SessionTable *table, const char *topic,
                             char **file_list, int total_files);
SessionState *session_lookup(SessionTable *table, const char *session_id);
void session_update(SessionTable *table, SessionState *session,
                    int file_idx, uint64_t byte_offset, uint64_t total_bytes);
int session_persist_server(const SessionState *session, const char *topics_root_dir);
int session_load_server(SessionState *session, const char *topics_root_dir,
                        const char *session_id);
void session_delete_server(const char *session_id);
void session_remove(SessionTable *table, const char *session_id);

/* Client stream checkpoint persistence */
int stream_checkpoint_save(const char *output_dir, const char *group_id, int conn_id,
                           const char *topic, int stream_file_idx, uint64_t byte_offset,
                           int completed_files, int total_assigned, uint64_t useful_bytes);

int stream_checkpoint_load(const char *output_dir, const char *group_id, int conn_id,
                           char *topic, int *stream_file_idx, uint64_t *byte_offset,
                           int *completed_files, int *total_assigned, uint64_t *useful_bytes);

void stream_checkpoint_delete(const char *output_dir, const char *group_id, int conn_id);

#endif /* SESSION_H */
