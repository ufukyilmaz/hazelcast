package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftGroupId;
import com.hazelcast.cp.internal.RaftInvocationManager;
import com.hazelcast.cp.internal.RaftTestApplyOp;
import com.hazelcast.cp.internal.RaftTestQueryOp;
import com.hazelcast.cp.internal.raft.QueryPolicy;
import com.hazelcast.enterprise.EnterpriseSerialParametersRunnerFactory;
import com.hazelcast.internal.util.DirectoryLock;
import com.hazelcast.logging.ILogger;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Future;

import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX;
import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.CP_MEMBER_FILE_NAME;
import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.getMetadataGroupIdFileName;
import static com.hazelcast.cp.persistence.ClusterPersistenceTest.awaitLeaderElectionAndGetTerm;
import static com.hazelcast.cp.persistence.FileIOSupport.TMP_SUFFIX;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.move;
import static java.nio.file.Files.write;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Objects.requireNonNull;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeThat;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseSerialParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class ClusterPersistenceFailureTest extends PersistenceTestSupport {

    @Test
    public void when_membersCrashWhileOperationsOngoing_then_recoversData() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        Address[] addresses = getAddresses(instances);

        RaftInvocationManager invocationManager = getRaftInvocationManager(instances[2]);
        RaftGroupId group = invocationManager.createRaftGroup("group").join();

        int increments = 5000;
        Future<Integer> f = spawn(() -> {
            for (int i = 0; i < increments; i++) {
                invocationManager.invoke(group, new RaftTestApplyOp(i + 1)).join();
                sleepMillis(1);
            }
            return invocationManager.<Integer>query(group, new RaftTestQueryOp(), QueryPolicy.LINEARIZABLE).join();
        });

        sleepMillis(500);
        // crash majority
        instances[0].getLifecycleService().terminate();

        sleepMillis(500);
        instances[1].getLifecycleService().terminate();

        // restart majority back
        instances[1] = restartInstance(addresses[1], config);
        sleepMillis(500);
        instances[0] = restartInstance(addresses[0], config);

        int value = f.get();
        assertEquals(value, increments);

        assertNotNull(instances[0].getCPSubsystem().getLocalCPMember());
        assertNotNull(instances[1].getCPSubsystem().getLocalCPMember());
        assertCommitIndexesSame(instances, group);
    }

    @Test
    public void when_cpMemberRestartsWithoutCPMemberFile_then_restartFails() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().shutdown();

        ILogger logger = instances[1].getLoggingService().getLogger(getClass());
        File dir = getFirstAvailableMemberDir(logger);

        // Take a backup of local CP member file and delete
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        Path backup = file.toPath().resolveSibling(file.getName() + ".backup");
        copy(file.toPath(), backup);
        delete(file);

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart with missing CP member file!");
        } catch (Exception ignored) {
        }

        // Restore CP member file back
        move(backup, backup.resolveSibling(file.getName()));

        HazelcastInstance instance = restartInstance(cpMember.getAddress(), config);
        assertClusterSize(3, instance);
        assertNotNull(instance.getCPSubsystem().getLocalCPMember());
    }

    @Test
    public void when_cpMemberRestartsWithAPMemberFile_then_restartFails() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().shutdown();

        ILogger logger = instances[1].getLoggingService().getLogger(getClass());
        File dir = getFirstAvailableMemberDir(logger);

        File cpMemberFile = new File(dir, CPMetadataStoreImpl.CP_MEMBER_FILE_NAME);
        delete(cpMemberFile);
        boolean created = cpMemberFile.createNewFile();
        assertTrue(created);

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart with missing CP member file!");
        } catch (Exception ignored) {
        }
    }

    @Test
    public void when_cpMemberRestartsWithCorruptedCPMemberFile_then_restartFails() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().shutdown();

        ILogger logger = instances[1].getLoggingService().getLogger(getClass());
        File dir = getFirstAvailableMemberDir(logger);

        // Take a backup of local CP member file and delete
        File file = new File(dir, CP_MEMBER_FILE_NAME);
        Path backup = file.toPath().resolveSibling(file.getName() + ".backup");
        copy(file.toPath(), backup);
        // Corrupt local CP member file
        write(file.toPath(), randomString().getBytes());

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart with corrupted CP member file!");
        } catch (Exception ignored) {
        }

        // Restore CP member file back
        move(backup, backup.resolveSibling(file.getName()), REPLACE_EXISTING);

        HazelcastInstance instance = restartInstance(cpMember.getAddress(), config);
        assertClusterSize(3, instance);
        assertNotNull(instance.getCPSubsystem().getLocalCPMember());
    }

    @Test
    public void when_cpMemberRestartsWithCorruptedMetadataGroupIdFile_then_restartFails() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        CPMember cpMember = instances[0].getCPSubsystem().getLocalCPMember();
        instances[0].getLifecycleService().shutdown();

        ILogger logger = instances[1].getLoggingService().getLogger(getClass());
        File dir = getFirstAvailableMemberDir(logger);

        RaftGroupId metadataGroupId = getMetadataGroupId(instances[1]);

        // Create a corrupted metadata group-id file
        Path path = dir.toPath().resolve(getMetadataGroupIdFileName(metadataGroupId));
        write(path, randomString().getBytes());

        try {
            restartInstance(cpMember.getAddress(), config);
            fail(cpMember + " should not be able to restart with missing metadata group-id file!");
        } catch (Exception ignored) {
        }

        // Delete corrupted metadata group-id file
        Files.delete(path);

        HazelcastInstance instance = restartInstance(cpMember.getAddress(), config);
        assertClusterSize(3, instance);
        assertNotNull(instance.getCPSubsystem().getLocalCPMember());
    }

    private File getFirstAvailableMemberDir(ILogger logger) {
        File[] memberDirs = baseDir.listFiles(File::isDirectory);
        assertNotNull(memberDirs);

        Optional<File> memberDirOpt = Arrays.stream(memberDirs).filter(dir -> {
            try {
                DirectoryLock lock = DirectoryLock.lockForDirectory(dir, logger);
                lock.release();
                return true;
            } catch (Exception e) {
                return false;
            }
        }).findFirst();

        assertTrue(memberDirOpt.isPresent());
        return memberDirOpt.get();
    }

    @Test
    public void when_activeCPMembersFileMissing_thenMembersCannotDiscover() throws Exception {
        assumeThat("When address change is detected members publish their new addresses and that allows to discover each other.",
                restartAddressPolicy, equalTo(AddressPolicy.REUSE_EXACT));

        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        int initialTerm = awaitLeaderElectionAndGetTerm(instances, getMetadataGroupId(instances[0]));

        Address[] addresses = getAddresses(instances);
        factory.terminateAll();

        File[] dirs = baseDir.listFiles(File::isDirectory);
        assertNotNull(dirs);
        assertEquals(addresses.length, dirs.length);
        for (File dir : dirs) {
            Arrays.stream(requireNonNull(dir.listFiles(f -> f.getName().startsWith(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX))))
                    .forEach(f -> {
                        assertThat(f.getName(), not(endsWith(TMP_SUFFIX)));
                        File backup = new File(dir, f.getName() + TMP_SUFFIX);
                        try {
                            move(f.toPath(), backup.toPath(), REPLACE_EXISTING);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });
        }

        Config configTmp = createConfig(3, 3);
        configTmp.getCPSubsystemConfig().setDataLoadTimeoutSeconds(5);
        instances = restartInstances(addresses, configTmp);
        assertThat(instances, emptyArray());
        factory.terminateAll();

        // Restore active CP members
        for (File dir : dirs) {
            Arrays.stream(requireNonNull(dir.listFiles(f -> f.getName().startsWith(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX))))
                    .forEach(f -> {
                        assertThat(f.getName(), endsWith(TMP_SUFFIX));
                        Path backup = f.toPath();
                        Path file = backup.resolveSibling(f.getName().replaceAll(TMP_SUFFIX, ""));
                        try {
                            move(backup, file, REPLACE_EXISTING);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });
        }

        // All instances should be able to restore
        HazelcastInstance[] restartedInstances2 = restartInstances(addresses, config);
        assertEquals(addresses.length, restartedInstances2.length);
        assertClusterSizeEventually(addresses.length, restartedInstances2);
        waitUntilCPDiscoveryCompleted(restartedInstances2);

        int latestTerm = awaitLeaderElectionAndGetTerm(restartedInstances2, getMetadataGroupId(restartedInstances2[0]));
        assertThat(latestTerm, greaterThan(initialTerm));
    }

    @Test
    public void when_activeCPMembersFileCorrupted_thenMembersCannotDiscover() throws Exception {
        Config config = createConfig(3, 3);
        HazelcastInstance[] instances = factory.newInstances(config, 3);
        waitUntilCPDiscoveryCompleted(instances);

        int initialTerm = awaitLeaderElectionAndGetTerm(instances, getMetadataGroupId(instances[0]));

        Address[] addresses = getAddresses(instances);
        factory.terminateAll();

        File[] dirs = baseDir.listFiles(File::isDirectory);
        assertNotNull(dirs);
        assertEquals(addresses.length, dirs.length);
        for (File dir : dirs) {
            Arrays.stream(requireNonNull(dir.listFiles(f -> f.getName().startsWith(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX))))
                    .forEach(f -> {
                        assertThat(f.getName(), not(endsWith(TMP_SUFFIX)));
                        File backup = new File(dir, f.getName() + TMP_SUFFIX);
                        try {
                            copy(f.toPath(), backup.toPath());
                            // Corrupt active CP members file
                            write(f.toPath(), randomString().getBytes());
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });
        }

        Config configTmp = createConfig(3, 3);
        configTmp.getCPSubsystemConfig().setDataLoadTimeoutSeconds(5);
        instances = restartInstances(addresses, configTmp);
        assertThat(instances, emptyArray());
        factory.terminateAll();

        // Restore active CP members
        for (File dir : dirs) {
            File[] files = dir.listFiles(f ->
                    f.getName().startsWith(ACTIVE_CP_MEMBERS_FILE_NAME_PREFIX) && f.getName().endsWith(TMP_SUFFIX));
            Arrays.stream(requireNonNull(files))
                    .forEach(f -> {
                        Path backup = f.toPath();
                        Path file = backup.resolveSibling(f.getName().replaceAll(TMP_SUFFIX, ""));
                        try {
                            move(backup, file, REPLACE_EXISTING);
                        } catch (IOException e) {
                            throw new AssertionError(e);
                        }
                    });
        }

        // All instances should be able to restore
        HazelcastInstance[] restartedInstances2 = restartInstances(addresses, config);
        assertEquals(addresses.length, restartedInstances2.length);
        assertClusterSizeEventually(addresses.length, restartedInstances2);
        waitUntilCPDiscoveryCompleted(restartedInstances2);

        int latestTerm = awaitLeaderElectionAndGetTerm(restartedInstances2, getMetadataGroupId(restartedInstances2[0]));
        assertThat(latestTerm, greaterThan(initialTerm));
    }
}
