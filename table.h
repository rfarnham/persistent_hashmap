#ifndef phm_table_h
#define phm_table_h

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

typedef struct phm_table phm_table;

phm_table* phm_create_table(const char* path,
                            int table_size,
                            int max_assoc_bytes);

phm_table* phm_open_table(const char* path);

void phm_close_table(phm_table* table);

int phm_get_table_size(phm_table* table);

int phm_get_max_assoc_bytes(phm_table* table);

int phm_put(phm_table* table,
            size_t hash, const uint8_t* key, int key_size,
            const uint8_t* value, int value_size,
            time_t expiry, time_t now);

int phm_get(phm_table* table,
            size_t hash, const uint8_t* key, int key_size,
            const uint8_t** value,
            time_t new_expiry);

#endif
