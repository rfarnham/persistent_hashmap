#ifndef phm_iterator_h
#define phm_iterator_h

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>
#include <time.h>

#include "table.h"

typedef void* phm_iterator;

phm_iterator phm_iterator_begin(phm_table* table);

phm_iterator phm_iterator_end(phm_table* table);

phm_iterator phm_iterator_advance(phm_table* table, phm_iterator iterator);

size_t         phm_iterator_hash         (phm_table* table, phm_iterator iterator);
time_t         phm_iterator_expiry       (phm_table* table, phm_iterator iterator);
size_t         phm_iterator_assoc_offset (phm_table* table, phm_iterator iterator);
int            phm_iterator_key_size     (phm_table* table, phm_iterator iterator);
int            phm_iterator_value_size   (phm_table* table, phm_iterator iterator);
const uint8_t* phm_iterator_key          (phm_table* table, phm_iterator iterator);
const uint8_t* phm_iterator_value        (phm_table* table, phm_iterator iterator);

#endif
