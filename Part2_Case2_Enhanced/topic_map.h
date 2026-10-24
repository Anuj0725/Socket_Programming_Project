/*
 * topic_map.h - Topic-to-directory mapping data structure
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 *
 * Hash map with open addressing and linear probing for O(1) topic lookup.
 * Populated at startup from topics_root_dir, read-only thereafter.
 */

#ifndef TOPIC_MAP_H
#define TOPIC_MAP_H

#include "protocol.h"

/* Each topic entry in the hash map */
typedef struct {
    char key[MAX_KEY_LEN];          /* topic name, e.g. "cane"         */
    char dir_path[MAX_PATH_LEN];    /* absolute path to topic dir      */
    int  file_count;                /* number of files in directory     */
    int  occupied;                  /* 1 if this slot is in use         */
} TopicEntry;

/* The complete topic map */
typedef struct {
    TopicEntry entries[MAX_TOPICS];
    int count;                      /* number of topics loaded          */
} TopicMap;

/*
 * Initialize the topic map by scanning topics_root_dir.
 * Each subdirectory becomes a topic entry.
 * Returns 0 on success, -1 on error.
 */
int init_topic_map(TopicMap *map, const char *topics_root_dir);

/*
 * Look up a topic by key.
 * Returns pointer to TopicEntry if found, NULL otherwise.
 */
TopicEntry *lookup_topic(TopicMap *map, const char *key);

/*
 * Print all loaded topics to stdout (for logging/debugging).
 */
void print_topic_map(const TopicMap *map);

/*
 * Count regular files in a directory.
 * Returns file count, or -1 on error.
 */
int count_files_in_dir(const char *dir_path);

/*
 * Get sorted list of filenames in a directory.
 * Returns allocated array of strings (caller frees), sets *count.
 * Returns NULL on error.
 */
char **list_files_in_dir(const char *dir_path, int *count);

/*
 * Free a list of filenames returned by list_files_in_dir.
 */
void free_file_list(char **files, int count);

#endif /* TOPIC_MAP_H */
