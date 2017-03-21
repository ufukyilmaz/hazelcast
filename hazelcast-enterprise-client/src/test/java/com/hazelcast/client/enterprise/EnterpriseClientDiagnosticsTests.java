package com.hazelcast.client.enterprise;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.ClientTestUtil;
import com.hazelcast.client.impl.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseClientDiagnosticsTests extends HazelcastTestSupport {

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Parameterized.Parameter
    public NativeMemoryConfig.MemoryAllocatorType allocatorType;

    @Parameterized.Parameters(
            name = "allocatorType:{0}"
    )
    public static Iterable<Object[]> parameters() {
        return Arrays.asList(new Object[][]{
                {NativeMemoryConfig.MemoryAllocatorType.POOLED},
                {NativeMemoryConfig.MemoryAllocatorType.STANDARD}});
    }

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
    public void assertMemoryMetricsRegisteredWhenNativeMemoryIsEnabled() throws IOException {
        ClientConfig clientConfig = makeClientConfig(true);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);

        MetricsRegistryImpl metricsRegistry = clientImpl.getMetricsRegistry();
        Set<String> metrics = metricsRegistry.getNames();

        assertContains(metrics, "memorymanager.stats.committedNative");
        assertContains(metrics, "memorymanager.stats.freeNative");
        assertContains(metrics, "memorymanager.stats.maxMetadata");
        assertContains(metrics, "memorymanager.stats.maxNative");
        assertContains(metrics, "memorymanager.stats.usedMetadata");
        assertContains(metrics, "memorymanager.stats.usedNative");
    }

    @Test
    public void assertMemoryMetricsNotRegisteredWhenNativeMemoryIsDisabled() throws IOException {
        ClientConfig clientConfig = makeClientConfig(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = ClientTestUtil.getHazelcastClientInstanceImpl(client);

        MetricsRegistryImpl metricsRegistry = clientImpl.getMetricsRegistry();
        Set<String> metrics = metricsRegistry.getNames();

        assertNotContains(metrics, "memorymanager.stats.committedNative");
        assertNotContains(metrics, "memorymanager.stats.freeNative");
        assertNotContains(metrics, "memorymanager.stats.maxMetadata");
        assertNotContains(metrics, "memorymanager.stats.maxNative");
        assertNotContains(metrics, "memorymanager.stats.usedMetadata");
        assertNotContains(metrics, "memorymanager.stats.usedNative");
    }

    private ClientConfig makeClientConfig(boolean enableNativeMemory) throws IOException {
        ClientConfig clientConfig = new ClientConfig();
        if (enableNativeMemory) {
            clientConfig.setNativeMemoryConfig(makeNativeMemoryConfig());
        }
        clientConfig.setProperty("hazelcast.diagnostics.enabled", "true");
        clientConfig.setProperty("hazelcast.diagnostics.metric.level", "INFO");
        clientConfig.setProperty("hazelcast.diagnostics.directory", temporaryFolder.newFolder().getAbsolutePath());
        return clientConfig;
    }

    public NativeMemoryConfig makeNativeMemoryConfig() {
        return new NativeMemoryConfig()
                .setEnabled(true)
                .setSize(new MemorySize(16, MemoryUnit.MEGABYTES))
                .setAllocatorType(allocatorType);
    }
}
