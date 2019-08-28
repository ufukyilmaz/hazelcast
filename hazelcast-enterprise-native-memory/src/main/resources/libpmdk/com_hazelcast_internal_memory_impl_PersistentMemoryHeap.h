#include <jni.h>

#ifndef _Included_com_hazelcast_internal_memory_impl_PersistentMemoryHeap
#define _Included_com_hazelcast_internal_memory_impl_PersistentMemoryHeap


JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_PersistentMemoryHeap_createHeap0
  (JNIEnv*, jobject, jstring, jlong);

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_PersistentMemoryHeap_closeHeap0
  (JNIEnv*, jobject, jlong);

JNIEXPORT jboolean JNICALL Java_com_hazelcast_internal_memory_impl_PersistentMemoryHeap_isPmem0
  (JNIEnv*, jobject, jlong);

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_PersistentMemoryHeap_alloc0
  (JNIEnv*, jobject, jlong, jlong);

JNIEXPORT jlong JNICALL Java_com_hazelcast_internal_memory_impl_PersistentMemoryHeap_realloc0
  (JNIEnv*, jobject, jlong, jlong, jlong);

JNIEXPORT void JNICALL Java_com_hazelcast_internal_memory_impl_PersistentMemoryHeap_free0
  (JNIEnv*, jobject, jlong, jlong);

#endif
