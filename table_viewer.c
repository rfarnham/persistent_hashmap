#include <stdio.h>
#include <stdlib.h>

#include "table.h"
#include "iterator.h"
#include "internal.h"

static void print_header(phm_table* table) {
  phm_header* header = table->header;

  printf("HEADER: table size = %d, max assoc bytes = %d, next free assoc = %zu\n",
    header->table_size, header->max_assoc_bytes, header->next_free_assoc);
}

static void print_value(phm_table* table, phm_iterator iterator) {
    printf("hash = %zu, expiry = %ld, assoc_offset = %zu, "
           "key_size = %d, value_size = %d, "
           "key = %.*s, value = %.*s\n",
           phm_iterator_hash(table, iterator), phm_iterator_expiry(table, iterator), phm_iterator_assoc_offset(table, iterator),
           phm_iterator_key_size(table, iterator), phm_iterator_value_size(table, iterator),
           phm_iterator_key_size(table, iterator), phm_iterator_key(table, iterator), phm_iterator_value_size(table, iterator), phm_iterator_value(table, iterator));
}

static void print_entries(phm_table* table) {
    for (phm_iterator iterator = phm_iterator_begin(table);
         iterator != phm_iterator_end(table);
         iterator = phm_iterator_advance(table, iterator)) {
      print_value(table, iterator);
    }
}


int main(int argc, char* argv[]) {
    if (argc != 2) {
        printf("Usage: %s table_path\n", argv[0]);
        exit(1);
    }
    const char* table_path = argv[1];

    phm_table* table = phm_open_table(table_path);
    if (table == NULL) {
        exit(1);
    }

    print_header(table);
    print_entries(table);

    phm_close_table(table);

    return 0;
}
