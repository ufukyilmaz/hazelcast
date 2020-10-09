#include "hz_numa.h"
#include <numa.h>

int hz_numa_node_bound(struct bitmask *node_cpumask, struct bitmask *thread_cpumask) {
    unsigned int thread_mask_bits = numa_bitmask_nbytes(node_cpumask) * 8;
    int node_bound = 1;

    for (int cpuIdx = 0; cpuIdx < thread_mask_bits && node_bound == 1; ++cpuIdx) {
        int thread_isset = numa_bitmask_isbitset(thread_cpumask, cpuIdx);
        int node_isset = numa_bitmask_isbitset(node_cpumask, cpuIdx);

        // we are interested only in the CPUs of other NUMA nodes
        if (node_isset == 0 && thread_isset == 1) {
            node_bound = 0;
        }
    }

    return node_bound;
}
