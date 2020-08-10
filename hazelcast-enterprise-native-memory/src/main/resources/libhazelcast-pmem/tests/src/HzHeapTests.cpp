#include "gtest/gtest.h"
#include "src/hz_heap.h"
#include <memkind.h>
#include <libgen.h>
#include <mcheck.h>


// NOTE: we check heap consistency in some of the test cases with mprobe() calls
//
// make sure the test is linked with -lmcheck that makes sure consistency
// checking is enabled before the first malloc() call is done

// called if mcheck encounters a heap consistency issue
void no_op(enum mcheck_status status) {}

void temp_file_name(char *file_path);

void destroy_heap(struct hz_heap *heap);


TEST(CreatePmemHeapTests, CreateHeapWithMemkindPmemMinSize) {
    char errmsg[256];
    char file_path[FILENAME_MAX];
    temp_file_name(file_path);
    struct hz_heap *heap;

    int rc = hz_create_pmem_heap(file_path, MEMKIND_PMEM_MIN_SIZE, &heap, errmsg);

    EXPECT_EQ(HEAP_SUCCESS, rc);
    EXPECT_TRUE(heap->should_destroy);
    EXPECT_TRUE(heap->kind != nullptr);

    destroy_heap(heap);
}

TEST(CreatePmemHeapTests, CreateHeapWithSmallerThanMemkindPmemMinSize) {
    char errmsg[256];
    char file_path[FILENAME_MAX];
    temp_file_name(file_path);
    struct hz_heap *heap = nullptr;

    int rc = hz_create_pmem_heap(file_path, MEMKIND_PMEM_MIN_SIZE - 1, &heap, errmsg);

    EXPECT_EQ(HEAP_ERROR_SIZE, rc);
    EXPECT_EQ(nullptr, heap);
}

TEST(CreatePmemHeapTests, CreateHeapOnNonExistentPath) {
    char errmsg[256];
    char file_path[FILENAME_MAX] = "/non-existent-dir/non-existent.file";
    struct hz_heap *heap = nullptr;

    int rc = hz_create_pmem_heap(file_path, MEMKIND_PMEM_MIN_SIZE, &heap, errmsg);

    EXPECT_EQ(HEAP_ERROR_PMEM_MAP_FILE, rc);
    EXPECT_EQ(nullptr, heap);
}

TEST(CreateHeapTests, CreateHeapWithDramKind) {
    char errmsg[256];
    char file_path[FILENAME_MAX];
    temp_file_name(file_path);
    struct hz_heap *heap;

    int rc = hz_create_heap(KIND_DRAM, &heap, errmsg);

    EXPECT_EQ(HEAP_SUCCESS, rc);
    EXPECT_FALSE(heap->should_destroy);
    EXPECT_TRUE(heap->kind != nullptr);

    destroy_heap(heap);
}

TEST(CreateHeapTests, CreateHeapWithUnknownKind) {
    char errmsg[256];
    char file_path[FILENAME_MAX];
    temp_file_name(file_path);
    struct hz_heap *heap;

    int rc = hz_create_heap(42, &heap, errmsg);

    EXPECT_EQ(HEAP_ERROR_UNKNOWN_KIND, rc);
    EXPECT_EQ(nullptr, heap);

    destroy_heap(heap);
}

TEST(CreateHeapTests, CreateHeapWithUnavailableKind) {
    char errmsg[256];
    char file_path[FILENAME_MAX];
    temp_file_name(file_path);
    struct hz_heap *heap;

    int rc = hz_create_heap(KIND_DRAM_HUGEPAGE, &heap, errmsg);

    EXPECT_EQ(HEAP_ERROR_KIND_UNAVAILABLE, rc);
    EXPECT_EQ(nullptr, heap);

    destroy_heap(heap);
}

TEST(CloseHeapTests, CloseHeapWithoutShouldDestroy) {
    mcheck(&no_op);

    char errmsg[256];
    struct hz_heap *heap = (struct hz_heap *) malloc(sizeof(struct hz_heap));
    heap->should_destroy = 0;
    int rc = hz_close_heap(heap, errmsg);

    EXPECT_EQ(HEAP_SUCCESS, rc);
    EXPECT_EQ(MCHECK_FREE, mprobe(heap)); // check if heap is freed
}

TEST(CloseHeapTests, CloseHeapWithShouldDestroy) {
    mcheck(&no_op);

    char errmsg[256];
    char file_path[FILENAME_MAX];
    temp_file_name(file_path);
    struct hz_heap *heap = (struct hz_heap *) malloc(sizeof(struct hz_heap));
    heap->should_destroy = 1;
    int rc = memkind_create_pmem(dirname((char *) file_path), MEMKIND_PMEM_MIN_SIZE, &heap->kind);

    EXPECT_EQ(MEMKIND_SUCCESS, rc);

    rc = hz_close_heap(heap, errmsg);

    EXPECT_EQ(HEAP_SUCCESS, rc);
    EXPECT_EQ(MCHECK_FREE, mprobe(heap)); // check if heap is freed
}

void temp_file_name(char *file_path) {
    FILE *tmpf = tmpfile();
    int fd = fileno(tmpf);

    char path[FILENAME_MAX];

    sprintf(path, "/proc/self/fd/%d", fd);
    readlink(path, file_path, FILENAME_MAX - 1);
}

void destroy_heap(struct hz_heap *heap) {
    if (heap) {
        if (heap->should_destroy) {
            memkind_destroy_kind(heap->kind);
        }
        free(heap);
    }
}
