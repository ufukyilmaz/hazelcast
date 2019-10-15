package com.hazelcast.internal.hotrestart.encryption;

import com.hazelcast.config.Config;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.hotrestart.HotRestartTestSupport;
import com.hazelcast.internal.hotrestart.impl.encryption.EncryptionManager;
import com.hazelcast.internal.util.StringUtil;
import org.junit.Test;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;

import static com.hazelcast.cluster.ClusterShutdownTest.assertNodesShutDownEventually;
import static com.hazelcast.cluster.ClusterShutdownTest.getNodes;
import static com.hazelcast.internal.hotrestart.encryption.TestHotRestartEncryptionUtils.withBasicEncryptionAtRestConfig;
import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public abstract class AbstractHotRestartReencryptionIntegrationTest extends HotRestartTestSupport {

    private static final byte[] KEY1_BYTES = StringUtil.stringToBytes("0123456789012345");
    private static final byte[] KEY2_BYTES = StringUtil.stringToBytes("1234567890123450");
    private static final byte[] KEY3_BYTES = StringUtil.stringToBytes("2345678901234501");
    private static final byte[] KEY4_BYTES = StringUtil.stringToBytes("3456789012345012");

    @Parameters(name = "clusterSize:{0}")
    public static Collection<Object[]> parameters() {
        return asList(new Object[][]{{1}, {5}});
    }

    @Parameter
    public int clusterSize;

    /**
     * The initial keys to be stored in the Secure Store (current is last).
     */
    final byte[][] initialKeys() {
        return new byte[][]{KEY1_BYTES};
    }

    @Test
    public void testRotateAndWait() {
        restartRotate(new byte[][]{KEY1_BYTES}, KEY1_BYTES, KEY2_BYTES);
        assertReencryptedEventually(KEY2_BYTES);
    }

    @Test
    public void testRotateAndWaitRepeatedly() {
        resetFixture(clusterSize);
        for (byte[] key : new byte[][]{KEY2_BYTES, KEY3_BYTES, KEY4_BYTES}) {
            rotateKeys(key);
            assertReencryptedEventually(key);
        }
    }

    @Test
    public void testRestartRotateAndWaitRepeatedly() {
        restartRotate(new byte[][]{KEY1_BYTES}, KEY2_BYTES);
        assertReencryptedEventually(KEY2_BYTES);
        restartRotate(new byte[][]{KEY2_BYTES}, KEY3_BYTES);
        assertReencryptedEventually(KEY3_BYTES);
        restartRotate(new byte[][]{KEY3_BYTES}, KEY4_BYTES);
        assertReencryptedEventually(KEY4_BYTES);
    }

    @Test
    public void testRestartRotateRepeatedlyAndWait() {
        restartRotate(new byte[][]{KEY1_BYTES}, KEY2_BYTES);
        restartRotate(new byte[][]{KEY1_BYTES, KEY2_BYTES}, KEY3_BYTES);
        restartRotate(new byte[][]{KEY1_BYTES, KEY2_BYTES, KEY3_BYTES}, KEY4_BYTES);
        assertReencryptedEventually(KEY4_BYTES);
    }

    @Test
    public void testStartupFails_whenKeyRotatedOutDuringShutdown() {
        resetFixture(clusterSize);
        shutdownCluster();
        rotateKeys(KEY2_BYTES);
        final CountDownLatch latch = new CountDownLatch(clusterSize);
        final Map<Integer, String> issues = new ConcurrentHashMap<>();
        for (int i = 0; i < clusterSize; i++) {
            int j = i;
            spawn(() -> {
                try {
                    newHazelcastInstance(this::makeConfig);
                    issues.put(j, "passed");
                } catch (Throwable e) {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));
                    if (!sw.toString().contains("Cannot find master encryption key")) {
                        issues.put(j, "unexpected: " + sw.toString());
                    }
                } finally {
                    latch.countDown();
                }
            });
        }
        assertOpenEventually(latch);
        assertTrue(issues.toString(), issues.isEmpty());
    }

    private void restartRotate(byte[][] restartMasterKeys, byte[]... rotatedMasterKeys) {
        shutdownCluster();
        rotateKeys(restartMasterKeys);
        resetFixture(clusterSize);
        rotateKeys(rotatedMasterKeys);
    }

    /**
     * Rotate the Secure Store to the specified keys (current is last).
     */
    protected abstract void rotateKeys(byte[]... masterKeys);

    protected abstract SecureStoreConfig getSecureStoreConfig();

    private Config makeConfig() {
        Config config = new Config().setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config.getHotRestartPersistenceConfig().setEnabled(true).setBaseDir(baseDir);
        return withBasicEncryptionAtRestConfig(config, getSecureStoreConfig());
    }

    private void resetFixture(int clusterSize) {
        restartCluster(clusterSize, this::makeConfig);
    }

    private void shutdownCluster() {
        HazelcastInstance[] instances = getAllHazelcastInstances().toArray(new HazelcastInstance[0]);
        Node[] nodes = getNodes(instances);
        if (nodes.length > 0) {
            instances[0].getCluster().shutdown();
        }
        assertNodesShutDownEventually(nodes);
    }

    private void assertReencryptedEventually(byte[] masterKey) {
        assertEqualsEventually(() -> areAllKeyFilesReencrypted(masterKey), true);
    }

    private boolean areAllKeyFilesReencrypted(byte[] masterKey) {
        List<Path> keyFiles;
        try {
            keyFiles = Files
                    .find(baseDir.toPath(), Integer.MAX_VALUE, (path, attr) -> path.endsWith(EncryptionManager.KEY_FILE_NAME))
                    .collect(Collectors.toList());
            if (!keyFiles.stream().allMatch(path -> isEncryptedUsingKey(path, masterKey))) {
                return false;
            }
        } catch (Exception e) {
            // there may be (checked, unchecked) IO errors when the files gets deleted/modified concurrently etc.
            return false;
        }
        assertEquals(clusterSize, keyFiles.size());
        return true;
    }

    private static boolean isEncryptedUsingKey(Path keyFilePath, byte[] keyBytes) {
        byte[] keyHash = EncryptionManager.computeKeyHash(keyBytes);
        try (DataInputStream in = new DataInputStream(Files.newInputStream(keyFilePath))) {
            String base64EncodedKeyHash = in.readUTF();
            byte[] fileKeyHash = Base64.getDecoder().decode(base64EncodedKeyHash);
            return Arrays.equals(keyHash, fileKeyHash);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
    }

}
