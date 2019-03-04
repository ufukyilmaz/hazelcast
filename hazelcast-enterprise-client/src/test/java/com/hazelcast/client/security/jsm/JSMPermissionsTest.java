package com.hazelcast.client.security.jsm;

import static com.hazelcast.nio.IOUtil.closeResource;
import static com.hazelcast.util.StringUtil.isNullOrEmpty;
import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.security.AccessControlException;
import java.security.Permission;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.hamcrest.CoreMatchers;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientUserCodeDeploymentConfig;
import com.hazelcast.client.test.TestAwareClientFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.SSLConfig;
import com.hazelcast.config.UserCodeDeploymentConfig.ClassCacheMode;
import com.hazelcast.config.UserCodeDeploymentConfig.ProviderMode;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.HazelcastInstanceAware;
import com.hazelcast.core.IExecutorService;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.instance.EnterpriseNodeExtension;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.security.jsm.HazelcastRuntimePermission;
import com.hazelcast.test.annotation.QuickTest;

import net.sourceforge.prograde.sm.ProGradeJSM;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category({ QuickTest.class })
public class JSMPermissionsTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();
    @Rule
    public TemporaryFolder tempFolder = new TemporaryFolder();

    private final TestAwareClientFactory factory = new TestAwareClientFactory();

    @After
    public void cleanup() {
        System.setSecurityManager(null);
        factory.terminateAll();
    }

    @Test
    public void testLicenseKeyProtected() throws Exception {
        HazelcastInstance client = startMemberAndClient();
        initSecurityManager(new HazelcastRuntimePermission("com.hazelcast.config.Config.getLicenseKey"));

        IExecutorService executorService = client.getExecutorService("executor");
        Future<String> future = executorService.submitToKeyOwner(new GetLicenseKeyCallable(), "foo");
        expected.expectCause(CoreMatchers.isA(AccessControlException.class));
        future.get();
    }

    @Test
    public void testLicenseObjectProtected() throws Exception {
        HazelcastInstance client = startMemberAndClient();
        initSecurityManager(new HazelcastRuntimePermission("com.hazelcast.instance.EnterpriseNodeExtension.getLicense"));

        IExecutorService executorService = client.getExecutorService("executor");
        Future<String> future = executorService.submitToKeyOwner(new GetLicenseCallable(), "foo");
        expected.expectCause(CoreMatchers.isA(AccessControlException.class));
        future.get();
    }

    @Test
    public void testSSLConfigProtected() throws Exception {
        HazelcastInstance client = startMemberAndClient();
        initSecurityManager(new HazelcastRuntimePermission("com.hazelcast.config.NetworkConfig.getSSLConfig"));

        IExecutorService executorService = client.getExecutorService("executor");
        Future<String> future = executorService.submitToKeyOwner(new GetSSLConfigCallable(), "foo");
        expected.expectCause(CoreMatchers.isA(AccessControlException.class));
        future.get();
    }

    @Test
    public void testAccessAllowed() throws Exception {
        HazelcastInstance client = startMemberAndClient();
        initSecurityManager();

        IExecutorService executorService = client.getExecutorService("executor");
        Future<String> getLicenseFuture = executorService.submitToKeyOwner(new GetLicenseKeyCallable(), "foo");
        assertEquals(SampleLicense.UNLIMITED_LICENSE, getLicenseFuture.get());
        getLicenseFuture = executorService.submitToKeyOwner(new GetLicenseCallable(), "foo");
        assertEquals("vulnerable", getLicenseFuture.get());
        Future<String> getSSLConfigFuture = executorService.submitToKeyOwner(new GetSSLConfigCallable(), "foo");
        assertEquals("secret", getSSLConfigFuture.get());
    }

    private HazelcastInstance startMemberAndClient() {
        Config config = new Config();
        config.getNetworkConfig().setSSLConfig(new SSLConfig().setEnabled(false).setProperty("password", "secret"));
        config.setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config.getUserCodeDeploymentConfig().setEnabled(true).setClassCacheMode(ClassCacheMode.ETERNAL)
        .setProviderMode(ProviderMode.LOCAL_AND_CACHED_CLASSES);
        factory.newHazelcastInstance(config);

        ClientUserCodeDeploymentConfig clientUCDConfig = new ClientUserCodeDeploymentConfig().setEnabled(true)
                .addClass(GetLicenseCallable.class).addClass(GetSSLConfigCallable.class);
        ClientConfig clientConfig = new ClientConfig().setUserCodeDeploymentConfig(clientUCDConfig);
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);
        return client;
    }

    private void initSecurityManager(Permission... deniedPermissions) throws IOException {
        File policyFile = createPolicyWithDeniedEntries(deniedPermissions);
        System.setProperty("java.security.policy", policyFile.getAbsolutePath());
        System.setSecurityManager(new ProGradeJSM());
    }

    private File createPolicyWithDeniedEntries(Permission[] deniedPermissions) throws IOException {
        File file = tempFolder.newFile("hazelcast.policy");
        FileWriter fw = new FileWriter(file, true);
        try {
            fw.append("priority \"deny\";\n\n"
                    + "grant {\n\n"
                    + "    permission java.security.AllPermission;\n\n"
                    + "}\n\n"
                    + "deny {\n");
            for (Permission perm : deniedPermissions) {
                fw.append("    permission ")
                .append(perm.getClass().getName())
                .append(" \"").append(perm.getName()).append("\"");
                if (!isNullOrEmpty(perm.getActions())) {
                    fw.append(" \"").append(perm.getActions()).append("\"");
                }
                fw.append(";\n");
            }
            fw.append("};\n");
        } finally {
            closeResource(fw);
        }
        return file;
    }

    public static class GetLicenseKeyCallable implements Callable<String>, Serializable, HazelcastInstanceAware {

        private transient HazelcastInstance hz;

        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        public String call() {
            return hz.getConfig().getLicenseKey();
        }
    }

    public static class GetLicenseCallable implements Callable<String>, Serializable, HazelcastInstanceAware {

        private transient HazelcastInstance hz;

        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        public String call() {
            ClusterServiceImpl cs = (ClusterServiceImpl) hz.getCluster();
            EnterpriseNodeExtension nodeExt = (EnterpriseNodeExtension) cs.getNodeEngine().getNode().getNodeExtension();
            nodeExt.getLicense();
            return "vulnerable";
        }
    }

    public static class GetSSLConfigCallable implements Callable<String>, Serializable, HazelcastInstanceAware {

        private transient HazelcastInstance hz;

        public void setHazelcastInstance(HazelcastInstance hz) {
            this.hz = hz;
        }

        public String call() {
            return hz.getConfig().getNetworkConfig().getSSLConfig().getProperty("password");
        }
    }
}
