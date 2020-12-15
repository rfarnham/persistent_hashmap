#include <assert.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <sys/mman.h>
#include <stddef.h>
#include <time.h>
#include <unistd.h>
#include <stdlib.h>
#include <string.h>

#include "table.h"
#include "internal.h"

static void init_header(phm_header* header,  int table_size, int max_assoc_bytes) {
  header->table_size = table_size;
  header->max_assoc_bytes = max_assoc_bytes;
  header->next_free_assoc = 0;
}

static void init_table(phm_table* table, void* addr, int fd) {
  table->header = (phm_header*) addr;
  table->index = (phm_index*) ((uint8_t*) addr + sizeof(phm_header));
  table->assoc = (void*) (table->index + table->header->table_size);
  table->fd = fd;
}

static size_t calculate_len(int table_size, int max_assoc_bytes) {
  size_t header_size = sizeof(phm_header);
  size_t index_size = sizeof(phm_index) * table_size;
  size_t assoc_size = (sizeof(phm_assoc) + max_assoc_bytes) * table_size;
  
  return header_size + index_size + assoc_size;
}

static size_t calculate_file_len(int fd) {
  off_t len = lseek(fd, 0, SEEK_END);
  lseek(fd, 0, SEEK_SET);
  return len;
}

static phm_table* open_or_create_and_lock_table_file(const char* path, int table_size, int max_assoc_bytes) {
  if (((table_size > 0) ^ (max_assoc_bytes > 0)) || table_size < 0 || max_assoc_bytes < 0) {
    fprintf(stderr, "Invalid arguments: table_size = %d, max_assoc_bytes = %d\n", table_size, max_assoc_bytes);
    return NULL;
  }
  if (max_assoc_bytes % 8 != 0) {
    int incr = 8 - max_assoc_bytes % 8;
    fprintf(stderr, "Rounding up max_assoc_bytes from %d to %d\n", max_assoc_bytes, max_assoc_bytes + incr);
    max_assoc_bytes += incr;
  }

  bool create = table_size > 0;
  const char* err = create 
    ? "Could not create table \"%s\" [%s]: %s\n"
    : "Could not open table \"%s\" [%s]: %s\n";

  int flags = create ? O_RDWR | O_CREAT | O_EXCL : O_RDWR;
  int mode = 0660;
  int fd = open(path, flags, mode);
  
  if (fd == -1){ 
    fprintf(stderr, err, path, "open", strerror(errno));
    return NULL;
  }
  
  if (flock(fd, LOCK_EX) == -1) {
    fprintf(stderr, err, path, "flock", strerror(errno));
    close(fd);
    return NULL;
  }

  size_t len = create ? calculate_len(table_size, max_assoc_bytes) : calculate_file_len(fd);

  if (create) {
    if (ftruncate(fd, len) != 0) {
      fprintf(stderr, err, path, "ftruncate", strerror(errno));
      close(fd);
      remove(path);
      return NULL;
    }
  }

  void* addr = mmap(NULL, len, PROT_READ | PROT_WRITE, MAP_SHARED | MAP_FILE, fd, 0);
  if (addr == MAP_FAILED) {
    fprintf(stderr, err, path, "mmap", strerror(errno));
    close(fd);
    if (create) {
      remove(path);
    }
    return NULL;
  }

  if (create) {
    phm_header* header = (phm_header*) addr;
    init_header(header, table_size, max_assoc_bytes);
  }

  phm_table* table = (phm_table*) malloc(sizeof(phm_table));
  if (table == NULL) {
    fprintf(stderr, err, path, "malloc", strerror(errno));
    close(fd);
    return NULL;
  }

  init_table(table, addr, fd);
  return table;
}

phm_table* phm_create_table(const char* path, int table_size, int max_assoc_bytes) {
  return open_or_create_and_lock_table_file(path, table_size, max_assoc_bytes);
}

phm_table* phm_open_table(const char* path) {
  return open_or_create_and_lock_table_file(path, 0, 0);
}

void phm_close_table(phm_table* table) {
  void* addr = (void*) table->header;
  size_t len = calculate_len(table->header->table_size, table->header->max_assoc_bytes);
  if (munmap(addr, len) == -1) {
    fprintf(stderr, "Could not close table: %s\n", strerror(errno));
  }
  if (close(table->fd)  == -1) {
    fprintf(stderr, "Could not close table: %s\n", strerror(errno));
  }

  free(table);
}

static void write_assoc(phm_table* table,
                        phm_index* index,
                        size_t hash, time_t expiry,
                        const uint8_t* key, int key_size,
                        const uint8_t* value, int value_size) {
  phm_assoc* assoc = get_assoc_by_index(table, index);
  index->hash = hash;
  index->expiry = expiry;
  assoc->key_size = key_size;
  assoc->value_size = value_size;
  memcpy(assoc->bytes, key, key_size);
  memcpy(assoc->bytes + key_size, value, value_size);
}

static void update_assoc(phm_table* table,
                         phm_index* index,
                         time_t expiry,
                         const uint8_t* value, int value_size) {
  phm_assoc* assoc = get_assoc_by_index(table, index);
  index->expiry = expiry;
  assoc->value_size = value_size;
  memcpy(assoc->bytes + assoc->key_size, value, value_size);
}

static void append_assoc(phm_table* table,
                         phm_index* index,
                         size_t hash, time_t expiry,
                         const uint8_t* key, int key_size,
                         const uint8_t* value, int value_size) {
  phm_header* header = table->header;
  size_t offset = header->next_free_assoc;
  header->next_free_assoc += header->max_assoc_bytes;
  index->assoc_offset = offset;
  write_assoc(table, index, hash, expiry,  key, key_size, value, value_size);
}

static bool match_key(phm_table* table, phm_index* index, size_t hash, const uint8_t* key, int key_size) {
  phm_assoc* assoc = get_assoc_by_index(table, index);
  return index->hash == hash && assoc->key_size == key_size && memcmp(get_key(assoc), key, key_size) == 0;
}

static bool is_empty(phm_index* index) {
  return index->expiry == 0 && index->hash == 0;
}

static void expire(phm_index* index) {
  index->expiry = 0;
}

static void update_expiration(phm_index* index, time_t expiry) {
  if (expiry < 0) {
    return;
  } else if (expiry == 0) {
    expire(index);
  } else {
    index->expiry = expiry;
  }
}

static void find_indices(phm_table* table,
                         size_t hash, const uint8_t* key, int key_size,
                         phm_index** expired_p, phm_index** lru_p, phm_index** needle_p, phm_index** last_p,
                         time_t now) {
  int table_size = table->header->table_size;
  phm_index* first = table->index + hash % table_size;
  phm_index* sentinel = table->index + table_size;
  
  phm_index* expired = NULL;
  phm_index* lru = NULL;
  phm_index* needle = NULL;
  phm_index* last = first;

  do {
    if (match_key(table, last, hash, key, key_size)) {
      needle = last;
      break;
    }
    if (is_empty(last)) {
      break;
    }
    if (expired == NULL && last->expiry < now) {
      expired = last;
    }
    if (lru == NULL || last->expiry < lru->expiry) {
      lru = last;
    }

    last++;
    if (last == sentinel) {
      last = table->index;
    }
  } while (last != first);

  *expired_p = expired;
  *lru_p = lru;
  *needle_p = needle;
  *last_p = last;

  assert(expired == NULL || expired != needle);
  assert(lru == NULL || lru != needle);
  assert(needle == NULL || !is_empty(needle));
  assert(expired == NULL || !is_empty(expired));
  assert(lru == NULL || !is_empty(lru));
}


int phm_put(phm_table* table,
            size_t hash, const uint8_t* key, int key_size,
            const uint8_t* value, int value_size,
            time_t expiry, time_t now) {
  if (hash == 0) {
    // hash of 0 is reserved for denoting free entries.
    hash = hash + 1;
  }
  if (key_size + value_size > table->header->max_assoc_bytes) {
    fprintf(stderr, "Combined key size %d and value size %d greater than max assoc bytes %d.\n",
            key_size, value_size, table->header->max_assoc_bytes);
    return -1;
  }

  phm_index* expired;
  phm_index* lru;
  phm_index* needle;
  phm_index* last;
  find_indices(table, hash, key, key_size, &expired, &lru, &needle, &last, now);

  #define OFFSET(x) ((x) == NULL ? -1 : (x) - table->index)
  LOG("needle = %zd, expired = %zd, lru = %zd, last = %zd, ",
    OFFSET(needle), OFFSET(expired), OFFSET(lru), OFFSET(last));
  #undef OFFSET

  if (needle == NULL) {
    if (expired != NULL) {
      LOG(" [write expired (%zu)]\n", expired->hash);
      write_assoc(table, expired, hash, expiry, key, key_size, value, value_size);
    } else if (is_empty(last)) {
      LOG(" [append]\n");
      append_assoc(table, last, hash, expiry, key, key_size, value, value_size);
    } else {
      LOG(" [write lru (%zu)]\n", lru->hash);
      write_assoc(table, lru, hash, expiry, key, key_size, value, value_size);
    }
    return 0;
  } else {
    if (expired != NULL) {
      LOG(" [write expired compact (%zu)]\n", expired->hash);
      expire(needle);
      write_assoc(table, expired, hash, expiry, key, key_size, value, value_size);
    } else {
      LOG(" [update]\n");
      update_assoc(table, needle, expiry, value, value_size);
    }
    return 1;
  }
}

int phm_get(phm_table* table,
            size_t hash, const uint8_t* key, int key_size,
            const uint8_t** value,
            time_t new_expiry) {
  if (hash == 0) {
    hash = hash + 1;
  }
  if (key_size > table->header->max_assoc_bytes) {
    fprintf(stderr, "Key size %d greater than max assoc bytes %d.\n",
          key_size, table->header->max_assoc_bytes);
      *value = NULL;
      return -1;
  }

  phm_index* expired;
  phm_index* lru;
  phm_index* needle;
  phm_index* last;
  find_indices(table, hash, key, key_size, &expired, &lru, &needle,  &last, 0);

  if (needle == NULL) {
    *value = NULL;
    return -1;
  }

  update_expiration(needle, new_expiry);

  phm_assoc* assoc = get_assoc_by_index(table, needle);
  *value = get_value(assoc);
  return assoc->value_size;
}

int phm_get_table_size(phm_table* table) {
  return table->header->table_size;
}

int phm_get_max_assoc_bytes(phm_table* table) {
  return table->header->max_assoc_bytes;
}
