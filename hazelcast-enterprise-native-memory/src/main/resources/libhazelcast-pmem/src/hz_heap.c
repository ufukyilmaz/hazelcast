#include "hz_heap.h"
#include <memkind.h>
#include <libpmem.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libgen.h>

int hz_create_pmem_heap(const char *file_path, long size, struct hz_heap **heap, char *errmsg) {
    // make sure we return NULL if we encounter an error
    *heap = NULL;

    if ((size_t) size < MEMKIND_PMEM_MIN_SIZE) {
        sprintf(errmsg, "Heap size must be greater than %d", MEMKIND_PMEM_MIN_SIZE);
        return HEAP_ERROR_SIZE;
    }

    // checking if the specified file_path is on a persistent memory
    size_t mapped_len = 0;
    int is_pmem;
    void *mapped_addr = pmem_map_file(file_path, (size_t) size, PMEM_FILE_CREATE, 0600, &mapped_len, &is_pmem);
    if (mapped_addr == NULL) {
        strcpy(errmsg, pmem_errormsg());
        return HEAP_ERROR_PMEM_MAP_FILE;
    }
    int rc = pmem_unmap(mapped_addr, mapped_len);
    if (rc != 0) {
        strcpy(errmsg, pmem_errormsg());
        return HEAP_ERROR_PMEM_MAP_FILE;
    }

    // create the PMEM kind heap with Memkind
    struct memkind *pmem_kind = NULL;
    char *dir_path = dirname((char *) file_path);
    int err = memkind_create_pmem(dir_path, (size_t) size, &pmem_kind);
    if (err) {
        memkind_error_message(err, errmsg, MEMKIND_ERROR_MESSAGE_SIZE);
        return HEAP_ERROR_CREATE_PMEM_KIND;
    }

    *heap = malloc(sizeof(struct hz_heap));
    (*heap)->should_destroy = 1;
    (*heap)->is_pmem = is_pmem;
    (*heap)->kind = pmem_kind;

    return HEAP_SUCCESS;
}

int hz_create_heap(int kind_index, struct hz_heap **heap, char *errmsg) {
    // make sure we return NULL if we encounter an error
    *heap = NULL;

    struct memkind *kind;

    // check if the specified kind is known
    switch (kind_index) {
        case KIND_DRAM:
            kind = MEMKIND_DEFAULT;
            break;
        case KIND_DRAM_HUGEPAGE:
            kind = MEMKIND_HUGETLB;
            break;
        case KIND_PMEM_DAX_KMEM:
            kind = MEMKIND_DAX_KMEM;
            break;
        default:
            sprintf(errmsg, "Unknown kind %d", kind_index);
            return HEAP_ERROR_UNKNOWN_KIND;
    }

    // check if the specified kind is available
    int rc = memkind_check_available(kind);
    if (rc != MEMKIND_SUCCESS) {
        memkind_error_message(rc, errmsg, MEMKIND_ERROR_MESSAGE_SIZE);
        return HEAP_ERROR_KIND_UNAVAILABLE;
    }

    *heap = (struct hz_heap *) malloc(sizeof(struct hz_heap));
    (*heap)->should_destroy = 0;
    (*heap)->is_pmem = 0;
    (*heap)->kind = kind;

    return HEAP_SUCCESS;
}

int hz_close_heap(struct hz_heap *heap, char *errmsg) {
    if (heap->should_destroy) {
        int rc = memkind_destroy_kind(heap->kind);
        if (rc != MEMKIND_SUCCESS) {
            memkind_error_message(rc, errmsg, MEMKIND_ERROR_MESSAGE_SIZE);

            free(heap);
            return HEAP_ERROR_DESTROY_KIND;
        }
    }

    free(heap);
    return HEAP_SUCCESS;
}
