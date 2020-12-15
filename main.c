#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "table.h"
#include "iterator.h"

static void print_value(phm_table* table, phm_iterator iterator) {
    printf("hash = %zu, expiry = %ld, assoc_offset = %zu, "
           "key_size = %d, value_size = %d, "
           "key = %.*s, value = %.*s\n",
           phm_iterator_hash(table, iterator), phm_iterator_expiry(table, iterator), phm_iterator_assoc_offset(table, iterator),
           phm_iterator_key_size(table, iterator), phm_iterator_value_size(table, iterator),
           phm_iterator_key_size(table, iterator), phm_iterator_key(table, iterator), phm_iterator_value_size(table, iterator), phm_iterator_value(table, iterator));
}

int main(int argc, char* argv[]) {
    if (argc != 4) {
        printf("Usage: %s table_name table_size max_assoc_bytes\n", argv[0]);
        exit(1);
    }
    const char* table_name = argv[1];
    int table_size = atoi(argv[2]);
    int max_assoc_bytes = atoi(argv[3]);

    phm_table* table = phm_create_table(table_name, table_size, max_assoc_bytes);
    if (table == NULL) {
        exit(1);
    }

    // print_header(table);
    // char key[20];
    // char value[20];
    // for (int j = 0; j < 1000; j++) {
    //     for (int i = 0; i < 3000; i++) {
    //         snprintf(key, sizeof(key), "%d", i);
    //         snprintf(value, sizeof(value), "%d", i*i);
    //         size_t hash = i * 1000 + i;
    //         phm_put(table, hash,
    //                 (uint8_t*) key, strlen(key),
    //                 (uint8_t*) value, strlen(value),
    //                 i + 100, j * i + 55);

    //         // printf("%d: ", i);
    //         // print_header(table);
    //     }
    // }

    phm_put(table, 5,
        (uint8_t*) "firstkey", strlen("firstkey"),
        (uint8_t*) "firstvalue", strlen("firstvalue"),
        9, 0);

    phm_put(table, 5,
        (uint8_t*) "expiredkey", strlen("expiredkey"),
        (uint8_t*) "expiredvalue", strlen("expiredvalue"),
        2, 0);


    phm_put(table, 5,
        (uint8_t*) "secondkey", strlen("secondkey"),
        (uint8_t*) "secondvalue", strlen("secondvalue"),
        9, 0);

    phm_put(table, 5,
        (uint8_t*) "expiredkey2", strlen("expiredkey2"),
        (uint8_t*) "expiredvalue2", strlen("expiredvalue2"),
        3, 0);

    phm_put(table, 5,
        (uint8_t*) "mykey2", strlen("mykey2"),
        (uint8_t*) "myvalue2", strlen("myvalue2"),
        2, 0);


    phm_put(table, 5,
        (uint8_t*) "mykey2", strlen("mykey2"),
        (uint8_t*) "myvalue2redux", strlen("myvalue2redux"),
        4, 3);


    for (phm_iterator iterator = phm_iterator_begin(table);
         iterator != phm_iterator_end(table);
         iterator = phm_iterator_advance(table, iterator)) {
      print_value(table, iterator);
    }

    phm_close_table(table);
    return 0;
}
