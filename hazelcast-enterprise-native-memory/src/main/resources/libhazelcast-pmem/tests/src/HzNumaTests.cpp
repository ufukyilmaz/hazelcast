#include "gtest/gtest.h"
#include "src/hz_numa.h"

TEST(NumaBoundTests, testAllNodeCPUsAllowed_Bounded) {
    struct bitmask *node_cpumask = numa_allocate_cpumask();
    struct bitmask *thread_cpumask = numa_allocate_cpumask();

    // NUMA node mask: 011b
    numa_bitmask_setbit(node_cpumask, 0);
    numa_bitmask_setbit(node_cpumask, 1);

    // thread mask: 011b
    numa_bitmask_setbit(thread_cpumask, 0);
    numa_bitmask_setbit(thread_cpumask, 1);

    ASSERT_TRUE(hz_numa_node_bound(node_cpumask, thread_cpumask));
}

TEST(NumaBoundTests, testSubsetOfNodeCPUsAllowed_Bounded) {
    struct bitmask *node_cpumask = numa_allocate_cpumask();
    struct bitmask *thread_cpumask = numa_allocate_cpumask();

    // NUMA node mask: 011b
    numa_bitmask_setbit(node_cpumask, 0);
    numa_bitmask_setbit(node_cpumask, 1);

    // thread mask: 010b
    numa_bitmask_setbit(thread_cpumask, 1);

    ASSERT_TRUE(hz_numa_node_bound(node_cpumask, thread_cpumask));
}

TEST(NumaBoundTests, testOtherCPUsAllowed_NotBounded) {
    struct bitmask *node_cpumask = numa_allocate_cpumask();
    struct bitmask *thread_cpumask = numa_allocate_cpumask();

    // NUMA mode mask: 0111b
    numa_bitmask_setbit(node_cpumask, 0);
    numa_bitmask_setbit(node_cpumask, 1);
    numa_bitmask_setbit(node_cpumask, 2);

    // thread mask: 01011b
    numa_bitmask_setbit(thread_cpumask, 0);
    numa_bitmask_setbit(thread_cpumask, 1);
    numa_bitmask_setbit(thread_cpumask, 3);

    ASSERT_FALSE(hz_numa_node_bound(node_cpumask, thread_cpumask));
}
