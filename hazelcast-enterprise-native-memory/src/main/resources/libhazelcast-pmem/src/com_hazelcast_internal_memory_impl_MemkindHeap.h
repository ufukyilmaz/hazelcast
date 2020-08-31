#include <jni.h>

#ifndef _Included_com_hazelcast_internal_memory_impl_MemkindHeap
#define _Included_com_hazelcast_internal_memory_impl_MemkindHeap

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_init0
        (JNIEnv *, jobject);

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_createPmemHeap0
        (JNIEnv *, jobject, jstring, jlong);

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_createHeap0
        (JNIEnv *, jobject, jint);

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_closeHeap0
        (JNIEnv *, jobject, jlong);

JNIEXPORT jboolean JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_isPmem0
        (JNIEnv *, jobject, jlong);

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_alloc0
        (JNIEnv *, jobject, jlong, jlong);

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_realloc0
        (JNIEnv *, jobject, jlong, jlong);

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_MemkindHeap_free0
        (JNIEnv *, jobject, jlong);

#endif
