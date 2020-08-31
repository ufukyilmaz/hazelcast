#include "hz_heap.h"
#include <memkind.h>
#include <libpmem.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <libgen.h>

int hz_init(char *errmsg) {
    // setting "MEMKIND_HOG_MEMORY" to "1" makes Memkind to not release the freed memory back to the OS
    // with PMEM kind this prevents hole punching when freeing, which can be pretty expensive during Hazelcast shutdown
    // when Hazelcast frees all allocated memory blocks
    //
    // by default we set "MEMKIND_HOG_MEMORY=1" so that the users don't need to bother with this
    //
    // if "HZ_SET_HOG_MEMORY" was previously set to "0", we don't set "MEMKIND_HOG_MEMORY", which indicates the user's
    // intention to release the memory in every case
    //
    // if "MEMKIND_HOG_MEMORY" was previously set to any value, we don't set it to "1", which indicates that the user
    // wants to control hogging memory, and doesn't want Hazelcast to set it
    // this is a safe "escape" option if setting the environment variable programmatically is not working for some reason
    const char *set_hog = secure_getenv(HZ_SET_HOG_MEMORY);
    const char *hog = secure_getenv(MEMKIND_HOG_MEMORY);
    if ((set_hog == NULL || strcmp(set_hog, HZ_SET_HOG_MEMORY_DONTSET) != 0) && hog == NULL) {
        int setenv_rc = setenv(MEMKIND_HOG_MEMORY, MEMKIND_HOG_MEMORY_ENABLED, 0);
        if (setenv_rc != 0) {
            strcpy(errmsg, "Could not set the MEMKIND_HOG_MEMORY environment variable");
            return HEAP_ERROR_SETENV_HOG;
        }
    }
    return HEAP_SUCCESS;
}

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
    int trunc_rc = truncate(file_path, 0);
    if (trunc_rc != 0) {
        strcpy(errmsg, "Could not truncate the PMEM file");
        return HEAP_ERROR_TRUNCATE_PMEM_FILE;
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
