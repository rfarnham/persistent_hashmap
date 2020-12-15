#ifndef phm_stats_h
#define phm_stats_h

#include <stddef.h>

typedef struct {
    size_t cache_hit;
    size_t cache_miss;
    size_t expiration;
    size_t eviction;
} phm_stats;



#endif