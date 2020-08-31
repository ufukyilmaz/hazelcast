#include <jni.h>

#ifndef HAZELCAST_PMEM_COM_HAZELCAST_INTERNAL_MEMORY_IMPL_NUMA_H
#define HAZELCAST_PMEM_COM_HAZELCAST_INTERNAL_MEMORY_IMPL_NUMA_H

#define NUMA_NODE_UNBOUNDED -1

JNIEXPORT jboolean JNICALL Java_com_hazelcast_internal_memory_impl_NUMA_available
        (JNIEnv *, jobject);

JNIEXPORT jint JNICALL Java_com_hazelcast_internal_memory_impl_NUMA_maxNumaNode
        (JNIEnv *env, jobject obj);

JNIEXPORT jint JNICALL Java_com_hazelcast_internal_memory_impl_NUMA_currentThreadBoundedNumaNode
        (JNIEnv *, jobject);

#endif //HAZELCAST_PMEM_COM_HAZELCAST_INTERNAL_MEMORY_IMPL_NUMA_H
