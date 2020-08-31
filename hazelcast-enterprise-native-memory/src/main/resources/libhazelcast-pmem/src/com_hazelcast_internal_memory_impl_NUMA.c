#include "com_hazelcast_internal_memory_impl_NUMA.h"
#include "util.h"
#include <numa.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <sys/types.h>

JNIEXPORT jboolean JNICALL Java_com_hazelcast_internal_memory_impl_NUMA_available
        (JNIEnv *env, jobject obj) {
    int res = numa_available();
    // result code 0 means successful for numa_available()
    // if it returns other values, the result of the subsequent calls are unspecified
    return (jboolean) res == 0 ? 1 : 0;
}

JNIEXPORT jint JNICALL Java_com_hazelcast_internal_memory_impl_NUMA_maxNumaNode
        (JNIEnv *env, jobject obj) {
    return (jint) numa_max_node();
}

JNIEXPORT jint JNICALL Java_com_hazelcast_internal_memory_impl_NUMA_currentThreadBoundedNumaNode
        (JNIEnv *env, jobject obj) {
    pid_t tid = syscall(SYS_gettid);

    struct bitmask *thread_cpumask = numa_allocate_cpumask();
    numa_sched_getaffinity(tid, thread_cpumask);

    struct bitmask *node_cpumask = numa_allocate_cpumask();
    int bound_numa_node = NUMA_NODE_UNBOUNDED;
    for (int node = 0; node <= numa_max_node() && bound_numa_node == NUMA_NODE_UNBOUNDED; ++node) {
        numa_bitmask_clearall(node_cpumask);
        numa_node_to_cpus(node, node_cpumask);
        if (numa_bitmask_equal(thread_cpumask, node_cpumask)) {
            bound_numa_node = node;
        }
    }

    numa_free_cpumask(thread_cpumask);
    numa_free_cpumask(node_cpumask);

    return bound_numa_node;
}
