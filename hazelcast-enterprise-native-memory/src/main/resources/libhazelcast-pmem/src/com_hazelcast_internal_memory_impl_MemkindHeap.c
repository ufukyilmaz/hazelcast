#include "com_hazelcast_internal_memory_impl_MemkindHeap.h"
#include "hz_heap.h"
#include <memkind.h>
#include <string.h>

void throw_OOM(JNIEnv *env, size_t size) {
    char errmsg[256];

    jclass exClass = (*env)->FindClass(env, "java/lang/OutOfMemoryError");

    sprintf(errmsg, "Failed to allocate %lu bytes!", size);
    (*env)->ThrowNew(env, exClass, errmsg);
}

void throw_io_exception(JNIEnv *env, const char *msg) {
    jclass exClass = (*env)->FindClass(env, "java/io/IOException");
    (*env)->ThrowNew(env, exClass, msg);
}

void throw_runtime_exception(JNIEnv *env, const char *msg) {
    jclass exClass = (*env)->FindClass(env, "java/lang/RuntimeException");
    (*env)->ThrowNew(env, exClass, msg);
}

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_createPmemHeap0
        (JNIEnv *env, jobject obj, jstring path, jlong size) {
    char errmsg[256];
    const char *file_path = (*env)->GetStringUTFChars(env, path, 0);
    struct hz_heap *heap;

    int rc = hz_create_pmem_heap(file_path, size, &heap, errmsg);
    if (rc != HEAP_SUCCESS) {
        throw_io_exception(env, errmsg);
        return (jlong) NULL;
    }

    return (jlong) heap;
}

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_createHeap0
        (JNIEnv *env, jobject obj, jint kindIndex) {
    struct hz_heap *heap = NULL;
    char errmsg[MEMKIND_ERROR_MESSAGE_SIZE];

    int rc = hz_create_heap(kindIndex, &heap, errmsg);
    if (rc != HEAP_SUCCESS) {
        throw_runtime_exception(env, errmsg);
        return (jlong) NULL;
    }

    return (jlong) heap;
}

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_closeHeap0
        (JNIEnv *env, jobject obj, jlong handle) {
    char errmsg[MEMKIND_ERROR_MESSAGE_SIZE];
    int rc = hz_close_heap((struct hz_heap *) handle, errmsg);
    if (rc != HEAP_SUCCESS) {
        throw_runtime_exception(env, errmsg);
    }
}

JNIEXPORT jboolean JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_isPmem0
        (JNIEnv *env, jobject obj, jlong handle) {
    struct hz_heap *heap = (struct hz_heap *) handle;

    return heap->is_pmem != 0;
}

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_alloc0
        (JNIEnv *env, jobject obj, jlong handle, jlong size) {
    struct hz_heap *heap = (struct hz_heap *) handle;
    void *p = memkind_malloc(heap->kind, (size_t) size);
    if (p == NULL) {
        throw_OOM(env, (size_t) size);
        return (jlong) NULL;
    }

    return (jlong) p;
}

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_realloc0
        (JNIEnv *env, jobject obj, jlong handle, jlong address, jlong size) {
    struct hz_heap *heap = (struct hz_heap *) handle;

    void *p = memkind_realloc(heap->kind, (void *) address, (size_t) size);
    if (p == NULL) {
        throw_OOM(env, (size_t) size);
    }

    return (jlong) p;
}

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_free0
        (JNIEnv *env, jobject obj, jlong handle, jlong address) {
    struct hz_heap *heap = (struct hz_heap *) handle;

    memkind_free(heap->kind, (void *) address);
}
