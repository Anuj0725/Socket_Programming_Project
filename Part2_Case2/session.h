/*
 * session.h - Session management for checkpoint-based resume
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Per design document sections 5.1-5.3
 */

#ifndef SESSION_H
#define SESSION_H

#define _FILE_OFFSET_BITS 64

#include "protocol.h"
#include <stdint.h>
#include <time.h>
#include <pthread.h>

#define MAX_SESSIONS 256
#define SERVER_SESSION_DIR "/tmp/cn_sessions"  /* Move out of dataset */
#define SESSION_TIMEOUT_SEC 300                 /* 5-minute session timeout */

/* Session state structure (design doc section 5.1) */
typedef struct {
    char     session_id[SESSION_ID_LEN + 1]; /* UUID v4 string           */
    char     topic[MAX_TOPIC_LEN];           /* requested topic          */
    char   **file_list;                      /* ordered filenames         */
    int      total_files;                    /* total files in topic      */
    int      completed_files;               /* files fully transferred   */
    int      current_file_idx;              /* file being transferred    */
    uint64_t byte_offset;                   /* offset within current file */
    uint64_t total_bytes_sent;              /* cumulative bytes sent      */
    time_t   last_update;                   /* last checkpoint time       */
    int      active;                        /* 1 if session is active     */
} SessionState;

/* Session table with mutex protection */
typedef struct {
    SessionState sessions[MAX_SESSIONS];
    int count;
    pthread_mutex_t lock;
} SessionTable;

/* Initialize session table */
void session_table_init(SessionTable *table);

/* Validate UUID characters and length */
int is_valid_uuid(const char *buf);

/* Generate a UUID v4 string (urandom) */
void generate_uuid(char *buf);

/* Create a new session, returns pointer to session (or NULL on failure) */
SessionState *session_create(SessionTable *table, const char *topic,
                             char **file_list, int total_files);

/* Look up a session by ID, returns pointer or NULL */
SessionState *session_lookup(SessionTable *table, const char *session_id);

/* Update session checkpoint */
void session_update(SessionTable *table, SessionState *session,
                    int file_idx, uint64_t byte_offset, uint64_t total_bytes);

/* Persist session state to disk as JSON */
int session_persist_server(const SessionState *session, const char *topics_root_dir);

/* Load session state from disk */
int session_load_server(SessionState *session, const char *topics_root_dir,
                        const char *session_id);

/* Delete server session file on completion */
void session_delete_server(const char *session_id);

/* Remove session (transfer completed) */
void session_remove(SessionTable *table, const char *session_id);

/* Client-side checkpoint persistence (atomic via tmp file + rename) */
int checkpoint_save_client(const char *output_dir, const char *session_id,
                           const char *topic, int file_idx, uint64_t byte_offset,
                           int completed_files, int total_files, uint64_t total_bytes);

/* Client-side checkpoint loading */
int checkpoint_load_client(const char *output_dir, const char *session_id,
                           char *topic, int *file_idx, uint64_t *byte_offset,
                           int *completed_files, int *total_files, uint64_t *total_bytes);

/* Check if a checkpoint file exists for the given topic */
int checkpoint_find_for_topic(const char *output_dir, const char *topic,
                              char *session_id_out);

/* Delete client checkpoint file */
void checkpoint_delete_client(const char *output_dir, const char *session_id);

#endif /* SESSION_H */
