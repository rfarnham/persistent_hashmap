#include <assert.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "table.h"
#include "iterator.h"

static char PATH[128];

static void test_invalid_table_size() {
        phm_table* table = phm_create_table(PATH, -1, 100);
        assert(table == NULL);
}

static void test_invalid_max_assoc_bytes() {
        phm_table* table = phm_create_table(PATH, 10, -1);
        assert(table == NULL);
}

static void test_header() {
    phm_table* table = phm_create_table(PATH, 10, 100);
    assert(table != NULL);
    assert(phm_get_table_size(table) == 10);
    assert(phm_get_max_assoc_bytes(table) == 104);
    phm_close_table(table);
    remove(PATH);
}

static void insert(phm_table* table, size_t hash, const char* key, 
                   const char* value, time_t expiry, time_t now) {
  phm_put(table, hash, (uint8_t*) key, strlen(key), (uint8_t*) value, strlen(value), expiry, now);
}

static void check_iterator(phm_table* table, phm_iterator* itp, size_t hash,
                           time_t expiry, size_t assoc_offset,
                           const char* key, const char* value) {
  phm_iterator it = *itp;
  assert(it != phm_iterator_end(table));
  assert(phm_iterator_hash(table, it) == hash);
  assert(phm_iterator_expiry(table, it) == expiry);
  assert(phm_iterator_assoc_offset(table, it) == assoc_offset);
  assert(phm_iterator_key_size(table, it) == (int) strlen(key));
  assert(phm_iterator_value_size(table, it) == (int) strlen(value));
  assert(memcmp(phm_iterator_key(table, it), key, strlen(key)) == 0);
  assert(memcmp(phm_iterator_value(table, it), value, strlen(value)) == 0);
  *itp = phm_iterator_advance(table, it);
}

static void check_get(phm_table* table, size_t hash,
                      const char* key, const char* value, time_t new_expiry) {
  const uint8_t* value_out;                
  int value_size = phm_get(table, hash, (uint8_t*) key, strlen(key), &value_out, new_expiry);
  if (value == NULL) {
      assert(value_size == -1);
      assert(value_out == NULL);
  } else {
    assert(value_size == (int) strlen(value));
    assert(memcmp(value_out, value, value_size) == 0);
  }
}

static void test_insert() {
    phm_table* table = phm_create_table(PATH, 10, 100);
    assert(table != NULL);
    size_t assoc_size = phm_get_max_assoc_bytes(table);

    insert(table, 3, "3a", "v3a", 4, 1);
    insert(table, 14, "14", "v14", 2, 1);
    insert(table, 23, "23", "v23", 3, 1);

    insert(table, 18, "18", "v18", 3, 2);
    insert(table, 19, "19", "v19", 3, 2);
    insert(table, 28, "28", "v28", 3, 2);

    insert(table, 3, "3b", "v3b", 5, 3);

    // expected layout of table:
    // hash  = [28, x, x, 3a, 3b, 23, x, x, 18, 19]
    // assoc = [v3a, v3b, v23, v18, v19, v28]
    phm_iterator it = phm_iterator_begin(table);
    check_iterator(table, &it, 28, 3, assoc_size * 5, "28", "v28");
    check_iterator(table, &it, 3, 4, assoc_size * 0, "3a", "v3a");
    check_iterator(table, &it, 3, 5, assoc_size * 1, "3b", "v3b");
    check_iterator(table, &it, 23, 3, assoc_size * 2, "23", "v23");
    check_iterator(table, &it, 18, 3, assoc_size * 3, "18", "v18");
    check_iterator(table, &it, 19, 3, assoc_size * 4, "19", "v19");
    assert(it == phm_iterator_end(table));

    check_get(table, 23, "23", "v23", 0); // update expiration to 0
    insert(table, 4, "4", "v4", 10, 4);   // should overwrite 23
    check_get(table, 23, "23", NULL, -1); // 23 is no longer accessible
    check_get(table, 4,  "4", "v4", -1);  // will not update expiration
    insert(table, 24, "24", "v24", 10, 5);
    check_get(table, 4, "4", "v4", -1);
    check_get(table, 24, "24", "v24", -1);

    phm_close_table(table);
    remove(PATH);
}

static void test_stress() {
    phm_table* table = phm_create_table(PATH, 10000, 1000);
    assert(table != NULL);

    char key[1024];
    char value[1024];
    long long hash = 0;
    for (int i = 0; i < 8000; i++) {
        hash = (hash * 63 + ~i) ^ ~hash;
        snprintf(key, sizeof(key), "key=%lld", hash);
        snprintf(value, sizeof(value), "value=%lld", hash);
        int ret = phm_put(table, (size_t) hash, (uint8_t*) key, strlen(key), (uint8_t*) value, strlen(value), 10, 5);
        assert(ret == 0);
    }

    hash = 0;
    for (int i = 0;  i < 8000; i++) {
        hash = (hash * 63 + ~i) ^ ~hash;
        const uint8_t* value_out;
        snprintf(key, sizeof(key), "key=%lld", hash);
        snprintf(value, sizeof(value), "value=%lld", hash);
        int value_size = phm_get(table, (size_t) hash, (uint8_t*) key, strlen(key), &value_out, -1);
        assert(value_size == (int) strlen(value));
        assert(memcmp(value, value_out, value_size) == 0);
    }

    phm_close_table(table);
    table = phm_open_table(PATH);
    assert(table != NULL);
    hash = 0;
    for (int i = 0;  i < 8000; i++) {
        hash = (hash * 63 + ~i) ^ ~hash;
        const uint8_t* value_out;
        snprintf(key, sizeof(key), "key=%lld", hash);
        snprintf(value, sizeof(value), "value=%lld", hash);
        int value_size = phm_get(table, (size_t) hash, (uint8_t*) key, strlen(key), &value_out, -1);
        assert(value_size == (int) strlen(value));
        assert(memcmp(value, value_out, value_size) == 0);
    }

    phm_close_table(table);
    remove(PATH);
}

#define TEST(fn) do {             \
    printf("TESTING " # fn "\n"); \
    test_ ## fn ();               \
} while (0)

int main() {
    strncpy(PATH, "/tmp/test_table.XXXX", sizeof(PATH));
    assert(mktemp(PATH));
    printf("\n(test table located in %s)\n\n", PATH);
    TEST(invalid_table_size);
    TEST(invalid_max_assoc_bytes);
    TEST(header);
    TEST(insert);
    TEST(stress);

    printf("\n\nAll tests passed!\n\n");
    return 0;
}
