/*
 * topic_map.c - Topic-to-directory mapping implementation
 * IT305 - Computer Networks Lab Project
 * Author: Anuj Madhani (202401017)
 */

#include "topic_map.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <ctype.h>
#include <dirent.h>
#include <sys/stat.h>

/* Case-insensitive hash function (djb2 by Dan Bernstein) */
static unsigned int hash_string(const char *str) {
    unsigned int hash = 5381;
    int c;
    while ((c = *str++))
        hash = ((hash << 5) + hash) + tolower((unsigned char)c);
    return hash;
}

int init_topic_map(TopicMap *map, const char *topics_root_dir) {
    memset(map, 0, sizeof(TopicMap));

    DIR *dir = opendir(topics_root_dir);
    if (!dir) {
        perror("opendir");
        fprintf(stderr, "[ERROR] Cannot open topics root directory: %s\n", topics_root_dir);
        return -1;
    }

    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        /* Skip . and .. and hidden files */
        if (entry->d_name[0] == '.') continue;

        /* Build full path */
        char full_path[MAX_PATH_LEN];
        snprintf(full_path, sizeof(full_path), "%s/%s", topics_root_dir, entry->d_name);

        /* Check if it's a directory */
        struct stat st;
        if (stat(full_path, &st) != 0 || !S_ISDIR(st.st_mode)) continue;

        if (map->count >= MAX_TOPICS) {
            fprintf(stderr, "[WARN] Maximum topic limit (%d) reached\n", MAX_TOPICS);
            break;
        }

        /* Find slot using linear probing */
        unsigned int idx = hash_string(entry->d_name) % MAX_TOPICS;
        while (map->entries[idx].occupied) {
            idx = (idx + 1) % MAX_TOPICS;
        }

        size_t klen = strlen(entry->d_name);
        if (klen >= sizeof(map->entries[idx].key)) klen = sizeof(map->entries[idx].key) - 1;
        memcpy(map->entries[idx].key, entry->d_name, klen);
        map->entries[idx].key[klen] = '\0';

        size_t plen = strlen(full_path);
        if (plen >= sizeof(map->entries[idx].dir_path)) plen = sizeof(map->entries[idx].dir_path) - 1;
        memcpy(map->entries[idx].dir_path, full_path, plen);
        map->entries[idx].dir_path[plen] = '\0';
        map->entries[idx].file_count = count_files_in_dir(full_path);
        map->entries[idx].occupied = 1;
        map->count++;
    }

    closedir(dir);
    fprintf(stderr, "[INFO] Loaded %d topics from %s\n", map->count, topics_root_dir);
    return 0;
}

TopicEntry *lookup_topic(TopicMap *map, const char *key) {
    if (!map || !key) return NULL;
    unsigned int idx = hash_string(key) % MAX_TOPICS;
    int checked = 0;
    while (checked < MAX_TOPICS) {
        if (!map->entries[idx].occupied) return NULL;
        /* Case-insensitive comparison */
        if (strcasecmp(map->entries[idx].key, key) == 0) {
            return &map->entries[idx];
        }
        idx = (idx + 1) % MAX_TOPICS;
        checked++;
    }
    return NULL;
}

void print_topic_map(const TopicMap *map) {
    if (!map) return;
    fprintf(stderr, "[INFO] Topic Map (%d topics):\n", map->count);
    for (int i = 0; i < MAX_TOPICS; i++) {
        if (map->entries[i].occupied) {
            fprintf(stderr, "  [%d] %-20s -> %s (%d files)\n",
                    i, map->entries[i].key,
                    map->entries[i].dir_path,
                    map->entries[i].file_count);
        }
    }
}

int count_files_in_dir(const char *dir_path) {
    DIR *dir = opendir(dir_path);
    if (!dir) return -1;

    int count = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char full_path[MAX_PATH_LEN];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
        struct stat st;
        if (stat(full_path, &st) == 0 && S_ISREG(st.st_mode)) {
            count++;
        }
    }
    closedir(dir);
    return count;
}

/* Comparison function for qsort on strings */
static int cmp_strings(const void *a, const void *b) {
    return strcmp(*(const char **)a, *(const char **)b);
}

char **list_files_in_dir(const char *dir_path, int *count) {
    if (!count) return NULL;
    *count = 0;

    DIR *dir = opendir(dir_path);
    if (!dir) return NULL;

    /* First pass: count files */
    int n = 0;
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_name[0] == '.') continue;
        char full_path[MAX_PATH_LEN];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
        struct stat st;
        if (stat(full_path, &st) == 0 && S_ISREG(st.st_mode)) {
            n++;
        }
    }
    rewinddir(dir);

    if (n == 0) {
        closedir(dir);
        *count = 0;
        return NULL;
    }

    /* Allocate array dynamically */
    char **files = (char **)malloc((size_t)n * sizeof(char *));
    if (!files) {
        closedir(dir);
        return NULL;
    }

    /* Second pass: collect names */
    int i = 0;
    while ((entry = readdir(dir)) != NULL && i < n) {
        if (entry->d_name[0] == '.') continue;
        char full_path[MAX_PATH_LEN];
        snprintf(full_path, sizeof(full_path), "%s/%s", dir_path, entry->d_name);
        struct stat st;
        if (stat(full_path, &st) == 0 && S_ISREG(st.st_mode)) {
            files[i] = strdup(entry->d_name);
            i++;
        }
    }
    closedir(dir);

    /* Sort for deterministic ordering */
    qsort(files, (size_t)i, sizeof(char *), cmp_strings);

    *count = i;
    return files;
}

void free_file_list(char **files, int count) {
    if (!files) return;
    for (int i = 0; i < count; i++) {
        if (files[i]) free(files[i]);
    }
    free(files);
}
