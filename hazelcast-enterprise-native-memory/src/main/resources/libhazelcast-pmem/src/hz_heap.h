#include <glob.h>

#ifndef HZ_HEAP_H
#define HZ_HEAP_H

/**
 * Return codes for the heap creation and destroy functions.
 */
enum {
    HEAP_SUCCESS = 0,
    HEAP_ERROR_SIZE,
    HEAP_ERROR_PMEM_MAP_FILE,
    HEAP_ERROR_PMEM_UNMAP_FILE,
    HEAP_ERROR_CREATE_PMEM_KIND,
    HEAP_ERROR_DESTROY_KIND,
    HEAP_ERROR_UNKNOWN_KIND,
    HEAP_ERROR_KIND_UNAVAILABLE
};

//enum {
//    KIND_DRAM = 0,
//    KIND_DRAM_HUGEPAGE,
//    KIND_PMEM_DAX_KMEM
//};
#define KIND_DRAM 0
#define KIND_DRAM_HUGEPAGE 1
#define KIND_PMEM_DAX_KMEM 2

/**
 * The struct representing the metadata for the heap created with hz_create_pmem_heap() and hz_create_heap() functions.
 */
struct hz_heap {
    /**
     * Tells if the kind should be destroyed on hz_close_heap() call.
     */
    int should_destroy;
    /**
     * Tells if this heap is a PMEM heap AND if it is created on persistent memory.
     */
    int is_pmem;
    /**
     * The pointer to the memkind struct that is used for actual heap management.
     */
    struct memkind *kind;
};

#if defined (__cplusplus)
extern "C" {
#endif

/**
 * Creates a persistent memory heap at the specified location.
 *
 * @param file_path The path of the file that is in the directory where the heap should be created
 * @param size      The size of the heap to create
 * @param heap      The pointer to the heap pointer, set to NULL if an error occurs
 * @param errmsg    The pointer to the error message that is potentially set if an error occurs
 * @return the result of the call
 */
int hz_create_pmem_heap(const char *file_path, long size, struct hz_heap **heap, char *errmsg);

/**
 * Creates a heap of the specified kind.
 *
 * @param kind_index The index of the kind the heap to create with
 * @param heap       The pointer to the heap pointer, set to NULL if an error occurs
 * @param errmsg     The pointer to the error message that is potentially set if an error occurs
 * @return the result of the call
 */
int hz_create_heap(int kind_index, struct hz_heap **heap, char *errmsg);

/**
 * Closes the heap previously created with hz_create_pmem_heap() or hz_create_heap() calls.
 *
 * @param heap   The pointer to the heap to close
 * @param errmsg The pointer to the error message that is potentially set if an error occurs
 * @return the result of the call
 */
int hz_close_heap(struct hz_heap *heap, char *errmsg);

#if defined (__cplusplus)
}
#endif

#endif //HZ_HEAP_H
