package com.hazelcast.enterprise;

import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IAtomicLong;
import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.license.domain.License;
import com.hazelcast.license.util.LicenseHelper;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.CompatibilityTest;
import com.hazelcast.util.RootCauseMatcher;
import org.junit.After;
import org.junit.Assume;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import java.util.Arrays;
import java.util.Collection;

import static com.hazelcast.enterprise.SampleLicense.SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.UNLIMITED_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.V5_SECURITY_ONLY_LICENSE;
import static com.hazelcast.enterprise.SampleLicense.V5_UNLIMITED_LICENSE;
import static com.hazelcast.instance.BuildInfoProvider.getBuildInfo;
import static com.hazelcast.license.domain.LicenseVersion.V4;
import static com.hazelcast.spi.properties.GroupProperty.ENTERPRISE_LICENSE_KEY;
import static com.hazelcast.test.CompatibilityTestHazelcastInstanceFactory.CURRENT_VERSION;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.test.HazelcastTestSupport.waitAllForSafeState;
import static java.lang.String.format;
import static org.junit.Assert.assertEquals;

// RU_COMPAT_3_10: tests specific to 3.10.x -> 3.11 rolling upgrade
// Tests with a mix of unlimited/security-only licenses report PostJoinWanOperation exceptions
// because the security-only member does not instantiate an EnterpriseWanReplicationService.
// These do not affect the correctness of the test.
@RunWith(Parameterized.class)
@Parameterized.UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category(CompatibilityTest.class)
public class RollingUpgradeLicenseTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    // config used for old members
    @Parameter
    public Config oldMemberConfig;

    @Parameter(1)
    public String licenseDescription;

    // config used for new member (supporting V5 license)
    @Parameter(2)
    public Config newMemberConfig;

    @Parameter(3)
    public boolean rollingUpgradeLicensed;

    private CompatibilityTestHazelcastInstanceFactory factory;

    @Parameters(name = "{1}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {v4SecurityOnlyLicenseConfig(), "security only v4", v4SecurityOnlyLicenseConfig(), true},
                {v4SecurityOnlyLicenseConfig(), "security only v4-v5", v5SecurityOnlyLicenseConfig(), false},
                {v4SecurityOnlyLicenseConfig(), "security only v4-unlimited v5", v5UnlimitedLicenseConfig(), true},
                {v4UnlimitedLicenseConfig(), "unlimited v4", v4UnlimitedLicenseConfig(), true},
                {v4UnlimitedLicenseConfig(), "unlimited v4-security v4", v4SecurityOnlyLicenseConfig(), true},
                {v4UnlimitedLicenseConfig(), "unlimited v4-security v5", v5SecurityOnlyLicenseConfig(), false},
                {v4UnlimitedLicenseConfig(), "unlimited v4-v5", v5UnlimitedLicenseConfig(), true},
                {v5UnlimitedLicenseConfig(), "unlimited v5", v5UnlimitedLicenseConfig(), true},
                {v5SecurityOnlyLicenseConfig(), "security only v5-unlimited v5", v5UnlimitedLicenseConfig(), false},
                {v5UnlimitedLicenseConfig(), "unlimited v5-security only v5", v5SecurityOnlyLicenseConfig(), false},
                });
    }

    @After
    public void tearDown() {
        if (factory != null) {
            factory.terminateAll();
        }
    }

    // test upgrade from pre-3.10.5 with V4 license
    @Test
    public void testUpgradeFrom3_10_withV4License() {
        assumeV4License(oldMemberConfig);
        testRollingUpgrade("3.10.2");
    }

    // test upgrade from 3.10.5 cluster
    @Test
    public void testUpgradeFrom3_10_5() {
        testRollingUpgrade("3.10.5");
    }

    private void testRollingUpgrade(String fromVersion) {
        String[] versions = new String[] {fromVersion,
                                          fromVersion,
                                          CURRENT_VERSION,
                                          CURRENT_VERSION,
                                          fromVersion};
        factory = new CompatibilityTestHazelcastInstanceFactory(versions);

        HazelcastInstance[] members = new HazelcastInstance[versions.length];

        // start old version cluster
        members[0] = factory.newHazelcastInstance(oldMemberConfig);
        members[1] = factory.newHazelcastInstance(oldMemberConfig);

        testClusterOperates(members);

        // add new members
        expectException();
        members[2] = factory.newHazelcastInstance(newMemberConfig);
        members[3] = factory.newHazelcastInstance(newMemberConfig);

        testClusterOperates(members);

        // terminate old members -> a new member is master
        members[0].getLifecycleService().terminate();
        members[1].getLifecycleService().terminate();

        waitAllForSafeState(members[2]);
        // start an old member
        members[4] = factory.newHazelcastInstance(oldMemberConfig);

        testClusterOperates(members);

        // remove last 3.10 member from cluster, upgrade cluster version and test
        upgradeCluster(members[4], members[3]);
        testClusterOperates(members);
    }

    private void expectException() {
        if (!rollingUpgradeLicensed) {
            // when RU is not allowed by license, new node fails with IllegalStateException
            // this is wrapped in a GuardianException
            expectedException.expect(new RootCauseMatcher(IllegalStateException.class));
        }
    }

    private void upgradeCluster(HazelcastInstance oldMember, HazelcastInstance newMember) {
        oldMember.getLifecycleService().terminate();
        waitAllForSafeState(newMember);
        newMember.getCluster().changeClusterVersion(Versions.CURRENT_CLUSTER_VERSION);
    }

    // smoke test: perform some operations on the cluster
    private void testClusterOperates(HazelcastInstance[] members) {
        for (int i = 1; i < 10; i++) {
            int memberIndex = i % members.length;
            HazelcastInstance member = members[memberIndex];
            if (member == null || !member.getLifecycleService().isRunning()) {
                continue;
            }
            String name = randomString();
            IAtomicLong counter = member.getAtomicLong(name);
            for (int j = 0; j < 100; j++) {
                counter.getAndIncrement();
            }
            assertEquals(100, counter.get());
        }
    }

    private void assumeV4License(Config config) {
        String licenseKey = config.getProperty(GroupProperty.ENTERPRISE_LICENSE_KEY.getName());
        License license = LicenseHelper.getLicense(licenseKey, getBuildInfo().getVersion());
        Assume.assumeTrue(format("Assumed version %s does not match actual license version %s",
                V4, license.getVersion()), V4.equals(license.getVersion()));
    }

    private static Config v4UnlimitedLicenseConfig() {
        return configWithLicense(UNLIMITED_LICENSE);
    }

    private static Config v5UnlimitedLicenseConfig() {
        return configWithLicense(V5_UNLIMITED_LICENSE);
    }

    private static Config v4SecurityOnlyLicenseConfig() {
        return configWithLicense(SECURITY_ONLY_LICENSE);
    }

    private static Config v5SecurityOnlyLicenseConfig() {
        return configWithLicense(V5_SECURITY_ONLY_LICENSE);
    }

    private static Config configWithLicense(String licenseKey) {
        Config config = new Config();
        config.getProperties().setProperty(ENTERPRISE_LICENSE_KEY.getName(), licenseKey);
        return config;
    }
}
