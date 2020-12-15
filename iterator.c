#include "iterator.h"
#include "internal.h"

phm_iterator phm_iterator_begin(phm_table* table) {
    phm_index* iter = table->index;
    phm_index* end = (phm_index*) phm_iterator_end(table);
    while (iter != end && iter->expiry == 0) {
        iter++;
    }
    return iter;
}

phm_iterator phm_iterator_end(phm_table* table) {
    return table->assoc;
}

phm_iterator phm_iterator_advance(phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    phm_index* end = (phm_index*) phm_iterator_end(table);
    do {
        iter++;
    } while (iter != end && iter->expiry == 0);

    return iter;
}

size_t phm_iterator_hash(__attribute__((unused)) phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    return iter->hash;
}

time_t phm_iterator_expiry(__attribute__((unused)) phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    return iter->expiry;
}

size_t phm_iterator_assoc_offset(__attribute__((unused)) phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    return iter->assoc_offset;
}

int phm_iterator_key_size(phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    phm_assoc* assoc = get_assoc_by_index(table, iter);
    return assoc->key_size;
}

int phm_iterator_value_size(phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    phm_assoc* assoc = get_assoc_by_index(table, iter);
    return assoc->value_size;
}

const uint8_t* phm_iterator_key(phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    phm_assoc* assoc = get_assoc_by_index(table, iter);
    return get_key(assoc);
}

const uint8_t* phm_iterator_value(phm_table* table, phm_iterator iterator) {
    phm_index* iter = (phm_index*) iterator;
    phm_assoc* assoc = get_assoc_by_index(table, iter);
    return get_value(assoc);
}
