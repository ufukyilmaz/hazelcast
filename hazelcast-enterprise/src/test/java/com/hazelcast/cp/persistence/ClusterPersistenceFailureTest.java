package com.hazelcast.cp.persistence;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.cp.CPMember;
import com.hazelcast.cp.internal.RaftGroupId;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.concurrent.Future;

import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.ACTIVE_CP_MEMBERS_FILE_NAME;
import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.CP_MEMBER_FILE_NAME;
import static com.hazelcast.cp.persistence.CPMetadataStoreImpl.getMetadataGroupIdFileName;
import static com.hazelcast.cp.persistence.ClusterPersistenceTest.awaitLeaderElectionAndGetTerm;
import static com.hazelcast.internal.nio.IOUtil.delete;
import static java.nio.file.Files.copy;
import static java.nio.file.Files.move;
import static java.nio.file.Files.write;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static org.hamcrest.Matchers.emptyArray;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
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
            File file = new File(dir, ACTIVE_CP_MEMBERS_FILE_NAME);
            File backup = new File(dir, file.getName() + ".backup");
            copy(file.toPath(), backup.toPath());
            // Delete active CP members file
            delete(file);
        }

        Future f = spawn(() -> {
            restartInstances(addresses, config);
        });

        Config configTmp = createConfig(3, 3);
        configTmp.getCPSubsystemConfig().setDataLoadTimeoutSeconds(5);
        instances = restartInstances(addresses, configTmp);
        assertThat(instances, emptyArray());
        factory.terminateAll();

        // Restore active CP members
        for (File dir : dirs) {
            Path backup = dir.toPath().resolve(ACTIVE_CP_MEMBERS_FILE_NAME + ".backup");
            Path file = backup.resolveSibling(ACTIVE_CP_MEMBERS_FILE_NAME);
            move(backup, file, REPLACE_EXISTING);
        }

        // All instances should be able to restore
        HazelcastInstance[] restartedInstances2 = restartInstances(addresses, config);
        assertEquals(addresses.length, restartedInstances2.length);
        assertClusterSize(addresses.length, restartedInstances2);
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
            File file = new File(dir, ACTIVE_CP_MEMBERS_FILE_NAME);
            File backup = new File(dir, file.getName() + ".backup");
            copy(file.toPath(), backup.toPath());
            // Corrupt active CP members file
            delete(file);
            write(file.toPath(), randomString().getBytes());
        }

        Config configTmp = createConfig(3, 3);
        configTmp.getCPSubsystemConfig().setDataLoadTimeoutSeconds(5);
        instances = restartInstances(addresses, configTmp);
        assertThat(instances, emptyArray());
        factory.terminateAll();

        // Restore active CP members
        for (File dir : dirs) {
            Path backup = dir.toPath().resolve(ACTIVE_CP_MEMBERS_FILE_NAME + ".backup");
            Path file = backup.resolveSibling(ACTIVE_CP_MEMBERS_FILE_NAME);
            move(backup, file, REPLACE_EXISTING);
        }

        // All instances should be able to restore
        HazelcastInstance[] restartedInstances2 = restartInstances(addresses, config);
        assertEquals(addresses.length, restartedInstances2.length);
        assertClusterSize(addresses.length, restartedInstances2);
        waitUntilCPDiscoveryCompleted(restartedInstances2);

        int latestTerm = awaitLeaderElectionAndGetTerm(restartedInstances2, getMetadataGroupId(restartedInstances2[0]));
        assertThat(latestTerm, greaterThan(initialTerm));
    }
}
