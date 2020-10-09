#ifndef HAZELCAST_PMEM_HZ_NUMA_H
#define HAZELCAST_PMEM_HZ_NUMA_H

#include <numa.h>

#if defined (__cplusplus)
extern "C" {
#endif

/**
 * Tests if the given thread with the CPU bitmask "thread_cpumask" can run only on the CPUs of the NUMA node with
 * CPU bitmask "node_cpumask". If the thread can run only on the given NUMA node, the function returns 1, otherwise 0.
 *
 * Note that the function requires the two CPU masks to be of the same size.
 *
 * @param node_cpumask   The CPU bitmask of the NUMA node
 * @param thread_cpumask The CPU bitmask of the thread
 * @return 1 if the thread is allowed to run on the given NUMA node, 0 otherwise
 */
int hz_numa_node_bound(struct bitmask *node_cpumask, struct bitmask *thread_cpumask);

#if defined (__cplusplus)
}
#endif

#endif //HZ_NUMA_H
