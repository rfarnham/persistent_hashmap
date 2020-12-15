#ifndef phm_internal_h
#define phm_internal_h

#include <assert.h>
#include <stddef.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#ifdef DEBUG

#define LOG(fmt, ...) fprintf(stderr, fmt, ##__VA_ARGS__)

#else

#define LOG(fmt, ...)

#endif

typedef struct {
  int table_size;
  int max_assoc_bytes;
  size_t next_free_assoc;
} phm_header;

typedef struct {
  size_t hash;
  time_t expiry;
  size_t assoc_offset;
} phm_index;

typedef struct {
  int key_size;
  int value_size;
  uint8_t bytes[];
} phm_assoc;

// It is important that all alignments be multiples of 8 bytes.
static_assert(sizeof(phm_header) == 16, "phm_header assumed to be 16 bytes");
static_assert(sizeof(phm_index) == 24, "phm_index assumed to be 24 bytes");
static_assert(sizeof(phm_assoc) == 8, "phm_assoc assumed to be 8 bytes");

struct phm_table {
  phm_header* header;
  phm_index* index;
  uint8_t* assoc;
  int fd;
};

static inline phm_assoc* get_assoc_by_index(phm_table* table, phm_index* index) {
  return (phm_assoc*) (table->assoc + index->assoc_offset);
}

static inline uint8_t* get_key(phm_assoc* assoc) {
  return assoc->bytes;
}

static inline uint8_t* get_value(phm_assoc* assoc) {
  return assoc->bytes + assoc->key_size;
}

#endif
