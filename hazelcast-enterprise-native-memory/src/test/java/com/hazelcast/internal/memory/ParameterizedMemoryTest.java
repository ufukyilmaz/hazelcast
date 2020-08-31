package com.hazelcast.internal.memory;

import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.PersistentMemoryDirectoryConfig;
import com.hazelcast.internal.memory.impl.LibMalloc;
import com.hazelcast.internal.memory.impl.LibMallocFactory;
import com.hazelcast.internal.memory.impl.MemkindMallocFactory;
import com.hazelcast.internal.memory.impl.MemkindUtil;
import com.hazelcast.internal.memory.impl.UnsafeMallocFactory;
import com.hazelcast.test.HazelcastTestSupport;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

import java.util.Collection;

import static com.hazelcast.internal.memory.PmemTestUtil.firstOf;
import static com.hazelcast.internal.memory.impl.MemkindHeap.PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY;
import static java.util.Arrays.asList;

public class ParameterizedMemoryTest extends HazelcastTestSupport {

    @Parameterized.Parameter
    public AllocationSource allocationSource;

    @Parameterized.Parameters(name = "persistentMemory: {0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {AllocationSource.UNSAFE},
                {AllocationSource.MEMKIND_PMEM},
                {AllocationSource.MEMKIND_DRAM},
                });
    }

    @AfterClass
    public static void cleanUp() {
        System.clearProperty(MemkindUtil.HD_MEMKIND);
    }

    enum AllocationSource {
        UNSAFE,
        MEMKIND_PMEM,
        MEMKIND_DRAM
    }

    static LibMallocFactory newLibMallocFactory(AllocationSource allocationSource) {
        LibMallocFactory libMallocFactory = null;
        switch (allocationSource) {
            case UNSAFE:
                libMallocFactory = new UnsafeMallocFactory(new FreeMemoryChecker());
                break;
            case MEMKIND_PMEM:
                NativeMemoryConfig config = new NativeMemoryConfig();
                config.getPersistentMemoryConfig()
                      .addDirectoryConfig(new PersistentMemoryDirectoryConfig(firstOf(PERSISTENT_MEMORY_DIRECTORIES)));
                libMallocFactory = new MallocFactoryDelegate(new MemkindMallocFactory(config));
                break;
            case MEMKIND_DRAM:
                System.setProperty(MemkindUtil.HD_MEMKIND, "true");
                libMallocFactory = new MallocFactoryDelegate(new MemkindMallocFactory(new NativeMemoryConfig()));
                break;
        }
        return libMallocFactory;
    }

    /**
     * Doubles the size of the allocated heap, because PMDK library
     * utilizes some memory for internal purposes.
     */
    private static class MallocFactoryDelegate implements LibMallocFactory {

        private final LibMallocFactory delegate;

        private MallocFactoryDelegate(LibMallocFactory delegate) {
            this.delegate = delegate;
        }

        @Override
        public LibMalloc create(long size) {
            return delegate.create(2 * size);
        }
    }

    static LibMalloc newLibMalloc(AllocationSource allocationSource) {
        LibMallocFactory libMallocFactory = newLibMallocFactory(allocationSource);
        return libMallocFactory.create(1 << 28);
    }

    @BeforeClass
    public static void init() {
        System.setProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY, "true");
    }

    @AfterClass
    public static void cleanup() {
        System.clearProperty(PERSISTENT_MEMORY_CHECK_DISABLED_PROPERTY);
    }

    void checkPlatform() {
        if (allocationSource != AllocationSource.UNSAFE) {
            assumeThatLinuxOS();
        }
    }
}
