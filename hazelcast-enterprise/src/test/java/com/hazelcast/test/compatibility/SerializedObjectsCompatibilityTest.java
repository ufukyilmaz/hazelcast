package com.hazelcast.test.compatibility;

import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionAware;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.internal.serialization.impl.HeapData;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.memory.FreeMemoryChecker;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.memory.StandardMemoryManager;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.spi.properties.HazelcastProperties;
import com.hazelcast.test.HazelcastParallelParametersRunnerFactory;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.test.compatibility.SerializedObjectsAccessor.SerializedObject;
import com.hazelcast.version.Version;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.InvalidClassException;
import java.nio.ByteOrder;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE;
import static com.hazelcast.instance.BuildInfoProvider.HAZELCAST_INTERNAL_OVERRIDE_VERSION;
import static com.hazelcast.internal.cluster.Versions.CURRENT_CLUSTER_VERSION;
import static com.hazelcast.internal.cluster.Versions.PREVIOUS_CLUSTER_VERSION;
import static com.hazelcast.test.compatibility.SamplingSerializationService.isTestClass;
import static com.hazelcast.util.StringUtil.LINE_SEPARATOR;
import static java.lang.String.format;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;

/**
 * Tests compatibility of objects serialized in previous, compatible Hazelcast versions with current.
 */
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParallelParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class SerializedObjectsCompatibilityTest extends HazelcastTestSupport {

    private static final String CLASSPATH_RESOURCE_PATTERN = "com/hazelcast/test/compatibility/serialized-objects-%s";
    private static final String EE_CLASSPATH_RESOURCE_PATTERN = "com/hazelcast/test/compatibility/serialized-objects-%s-ee";
    private static final ILogger LOGGER = Logger.getLogger(SerializedObjectsCompatibilityTest.class);

    @Parameters(name = "samplesVersion: {0} testDeserializerVersion={1}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{
                {"3.11", "3.11"},
                {"3.11.2", "3.11"},
        });
    }

    @Parameter
    public String samplesVersion;

    @Parameter(1)
    public String testDeserializerVersion;

    private InternalSerializationService currentSerializationService;
    private String serializedObjectsResource;
    private String eeSerializedObjectsResource;

    @Before
    public void setup() {
        assumeTrue("This test must be updated for execution during 4.1 development cycle "
                + "with serialized objects samples from 4.0", Version.of("4.1").equals(CURRENT_CLUSTER_VERSION));
        serializedObjectsResource = format(CLASSPATH_RESOURCE_PATTERN, samplesVersion);
        eeSerializedObjectsResource = format(EE_CLASSPATH_RESOURCE_PATTERN, samplesVersion);
    }

    @Test
    public void testObjectsAreDeserializedInCurrentVersion_whenOSSerializationService() {
        // OS serialization version always uses current cluster version, so override this to emulate sample serialization version
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION, testDeserializerVersion);
        System.setProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE, "false");
        try {
            currentSerializationService = new DefaultSerializationServiceBuilder()
                    .setEnableSharedObject(true)
                    .build();
            assumeBigEndianSerialization(currentSerializationService);
            SerializedObjectsAccessor serializedObjects = new SerializedObjectsAccessor(serializedObjectsResource);
            assertObjectsAreDeserialized(serializedObjects);
        } finally {
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_VERSION);
            System.clearProperty(HAZELCAST_INTERNAL_OVERRIDE_ENTERPRISE);
        }
    }

    @Test
    public void testObjectsAreDeserializedInCurrentVersion_whenEESerializationService() {
        MemorySize size = new MemorySize(16, MemoryUnit.MEGABYTES);
        FreeMemoryChecker freeMemoryChecker = new FreeMemoryChecker(new HazelcastProperties((Properties) null));
        StandardMemoryManager memoryManager = new StandardMemoryManager(size, freeMemoryChecker);

        currentSerializationService = new EnterpriseSerializationServiceBuilder()
                .setMemoryManager(memoryManager)
                .setClusterVersionAware(new TestClusterVersionAware())
                .setVersionedSerializationEnabled(true)
                .setEnableSharedObject(true)
                .build();
        assumeBigEndianSerialization(currentSerializationService);
        SerializedObjectsAccessor serializedObjects = new SerializedObjectsAccessor(eeSerializedObjectsResource);
        assertObjectsAreDeserialized(serializedObjects);
    }

    private void assertObjectsAreDeserialized(SerializedObjectsAccessor serializedObjects) {
        Set<String> failedClassNames = new HashSet<String>();

        for (SerializedObject object : serializedObjects) {
            try {
                Object deserializedObject = fromBytes(object.getBytes());
                assertNotNull(deserializedObject);
            } catch (Exception e) {
                if (isTestClass(object.getClassName())) {
                    LOGGER.fine(object.getClassName() + " is a test class, deserialization failure ignored", e);
                    continue;
                }
                if (e.getCause() instanceof InvalidClassException) {
                    // a Java serializable's deserialization failed
                    // check whether the serializable that failed deserialization is a test class
                    InvalidClassException invalidClassException = (InvalidClassException) e.getCause();
                    if (isTestClass(invalidClassException.classname)) {
                        LOGGER.fine(object.getClassName() + " failed deserialization due to failure deserializing"
                                + " a test class and may be ignored", invalidClassException);
                        continue;
                    }
                }
                LOGGER.severe(e);
                failedClassNames.add(object.getClassName());
            }
        }

        if (!failedClassNames.isEmpty()) {
            StringBuilder failureMessageBuilder = new StringBuilder("Failed to deserialize classes:").append(LINE_SEPARATOR);
            for (String className : failedClassNames) {
                failureMessageBuilder.append(className).append(LINE_SEPARATOR);
            }
            fail(failureMessageBuilder.toString());
        }
    }

    private <T> T fromBytes(byte[] bytes) {
        Data data = new HeapData(bytes);
        return currentSerializationService.toObject(data);
    }

    private static class TestClusterVersionAware implements EnterpriseClusterVersionAware {
        @Override
        public Version getClusterVersion() {
            return PREVIOUS_CLUSTER_VERSION;
        }
    }

    private static void assumeBigEndianSerialization(InternalSerializationService internalSerializationService) {
        assumeTrue(ByteOrder.BIG_ENDIAN.equals(internalSerializationService.getByteOrder()));
    }
}
