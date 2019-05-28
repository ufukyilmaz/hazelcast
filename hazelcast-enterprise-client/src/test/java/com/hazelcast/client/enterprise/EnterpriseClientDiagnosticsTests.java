package com.hazelcast.client.enterprise;

import static com.hazelcast.client.impl.clientside.ClientTestUtil.getHazelcastClientInstanceImpl;
import static java.util.Arrays.asList;

import java.util.Set;

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

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.impl.clientside.HazelcastClientInstanceImpl;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParametersRunnerFactory;
import com.hazelcast.internal.metrics.impl.MetricsRegistryImpl;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParametersRunnerFactory.class)
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

        assertContains(metrics, "memorymanager.stats.committedNative");
        assertContains(metrics, "memorymanager.stats.freeNative");
        assertContains(metrics, "memorymanager.stats.maxMetadata");
        assertContains(metrics, "memorymanager.stats.maxNative");
        assertContains(metrics, "memorymanager.stats.usedMetadata");
        assertContains(metrics, "memorymanager.stats.usedNative");
    }

    @Test
    public void assertMemoryMetricsNotRegisteredWhenNativeMemoryIsDisabled() throws Exception {
        ClientConfig clientConfig = makeClientConfig(false);
        HazelcastInstance client = hazelcastFactory.newHazelcastClient(clientConfig);
        HazelcastClientInstanceImpl clientImpl = getHazelcastClientInstanceImpl(client);

        MetricsRegistryImpl metricsRegistry = clientImpl.getMetricsRegistry();
        Set<String> metrics = metricsRegistry.getNames();

        assertNotContains(metrics, "memorymanager.stats.committedNative");
        assertNotContains(metrics, "memorymanager.stats.freeNative");
        assertNotContains(metrics, "memorymanager.stats.maxMetadata");
        assertNotContains(metrics, "memorymanager.stats.maxNative");
        assertNotContains(metrics, "memorymanager.stats.usedMetadata");
        assertNotContains(metrics, "memorymanager.stats.usedNative");
    }

    private ClientConfig makeClientConfig(boolean enableNativeMemory) throws Exception {
        ClientConfig clientConfig = new ClientConfig()
                .setProperty("hazelcast.diagnostics.enabled", "true")
                .setProperty("hazelcast.diagnostics.metric.level", "INFO")
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
