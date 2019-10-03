package com.hazelcast.client.enterprise;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Set;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static java.util.Arrays.asList;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseClientDiagnosticsTests extends HazelcastTestSupport {

    @Parameters(name = "allocatorType:{0}")
    public static Iterable<Object[]> parameters() {
        return asList(new Object[][]{
                {NativeMemoryConfig.MemoryAllocatorType.POOLED},
                {NativeMemoryConfig.MemoryAllocatorType.STANDARD},
        });
    }

    @Parameter
    public NativeMemoryConfig.MemoryAllocatorType allocatorType;

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private final TestHazelcastFactory hazelcastFactory = new TestHazelcastFactory();

    @Before
    public void setUp() {
        hazelcastFactory.newHazelcastInstance();
    }

    @After
    public void tearDown() {
        hazelcastFactory.shutdownAll();
    }

    @Test
    public void assertMemoryMetricsRegisteredWhenNativeMemoryIsEnabled() throws Exception {
        ClientConfig clientConfig = makeClientConfig(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);

        MetricsRegistryImpl metricsRegistry = clientImpl.getMetricsRegistry();
        Set<String> metrics = metricsRegistry.getNames();

        assertContains(metrics, "[unit=count,metric=memorymanager.stats.committedNative]");
        assertContains(metrics, "[unit=count,metric=memorymanager.stats.freeNative]");
        assertContains(metrics, "[unit=count,metric=memorymanager.stats.maxMetadata]");
        assertContains(metrics, "[unit=count,metric=memorymanager.stats.maxNative]");
        assertContains(metrics, "[unit=count,metric=memorymanager.stats.usedMetadata]");
        assertContains(metrics, "[unit=count,metric=memorymanager.stats.usedNative]");
    }

    @Test
    public void assertMemoryMetricsNotRegisteredWhenNativeMemoryIsDisabled() throws Exception {
        ClientConfig clientConfig = makeClientConfig(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);

        MetricsRegistryImpl metricsRegistry = clientImpl.getMetricsRegistry();
        Set<String> metrics = metricsRegistry.getNames();

        assertNotContains(metrics, "[unit=count,metric=memorymanager.stats.committedNative]");
        assertNotContains(metrics, "[unit=count,metric=memorymanager.stats.freeNative]");
        assertNotContains(metrics, "[unit=count,metric=memorymanager.stats.maxMetadata]");
        assertNotContains(metrics, "[unit=count,metric=memorymanager.stats.maxNative]");
        assertNotContains(metrics, "[unit=count,metric=memorymanager.stats.usedMetadata]");
        assertNotContains(metrics, "[unit=count,metric=memorymanager.stats.usedNative]");
    }

    private ClientConfig makeClientConfig(boolean enableNativeMemory) throws Exception {
        ClientConfig clientConfig = new ClientConfig()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.directory", temporaryFolder.newFolder().getAbsolutePath());
        if (enableNativeMemory) {
            clientConfig.setNativeMemoryConfig(makeNativeMemoryConfig());
        }
        return clientConfig;
    }

    private NativeMemoryConfig makeNativeMemoryConfig() {
        return new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(16, MemoryUnit.MEGABYTES))
                .setAllocatorType(allocatorType);
    }
}
