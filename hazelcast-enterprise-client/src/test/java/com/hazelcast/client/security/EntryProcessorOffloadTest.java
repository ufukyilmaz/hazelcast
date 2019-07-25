package com.hazelcast.client.security;

import static com.hazelcast.config.PermissionConfig.PermissionType.MAP;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.not;
import static org.junit.Assert.assertThat;

import java.io.Serializable;
import java.security.AccessControlException;
import java.util.Map.Entry;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.test.TestHazelcastFactory;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.core.Offloadable;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.EntryBackupProcessor;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelTest.class })
public class EntryProcessorOffloadTest {

    private final TestHazelcastFactory factory = new TestHazelcastFactory();

    @After
    public void cleanup() {
        factory.terminateAll();
    }

    @Test
    public void test() {
        Config config = new Config();
        PermissionConfig perm = new PermissionConfig(MAP, "*", "dev").addAction(ActionConstants.ACTION_ALL);
        config.getSecurityConfig().setEnabled(true).addClientPermissionConfig(perm);
        config.getSecurityConfig().addSecurityInterceptorConfig(new SecurityInterceptorConfig(new SI()));
        factory.newHazelcastInstance(config);

        ClientConfig clientConfig = new ClientConfig();
        HazelcastInstance client = factory.newHazelcastClient(clientConfig);

        IMap<String, String> map = client.getMap(randomName());
        map.put("key", "value");

        assertThat(map.executeOnKey("key", new MySampleEntryProcessor()).toString(), containsString("partition-operation"));
        assertThat(map.executeOnKey("key", new MySampleOffloadableEntryProcessor()).toString(),
                not(containsString("partition-operation")));
    }

    public static class MySampleOffloadableEntryProcessor implements EntryProcessor<String, String>, Serializable, Offloadable {

        private static final long serialVersionUID = 7196208989609354831L;

        public MySampleOffloadableEntryProcessor() {
        }

        public String getExecutorName() {
            return "TEST-durable-executor-name";
        }

        public EntryBackupProcessor<String, String> getBackupProcessor() {
            return null;
        }

        public String process(Entry<String, String> entry) {
            entry.setValue("orange");
            return Thread.currentThread().getName();
        }

    }

    public static class MySampleEntryProcessor implements EntryProcessor<String, String>, Serializable {

        private static final long serialVersionUID = 7196208989609354831L;

        public MySampleEntryProcessor() {
        }

        public EntryBackupProcessor<String, String> getBackupProcessor() {
            return null;
        }

        public String process(Entry<String, String> entry) {
            entry.setValue("broccoli");
            return Thread.currentThread().getName();
        }

    }

    @SuppressWarnings("unchecked")
    public static class SI implements SecurityInterceptor {

        @Override
        public void before(Credentials credentials, String objectType, String objectName, String methodName,
                Parameters parameters) throws AccessControlException {
            System.out.println("before " + formatArgs(credentials, objectType, objectName, methodName, parameters));
        }

        @Override
        public void after(Credentials credentials, String objectType, String objectName, String methodName,
                Parameters parameters) {
            System.out.println("after " + formatArgs(credentials, objectType, objectName, methodName, parameters));
        }

        private String formatArgs(Credentials credentials, String objectType, String objectName, String methodName,
                Parameters parameters) {
            StringBuilder builder = new StringBuilder();
            builder.append("[credentials=").append(credentials).append(", objectType=").append(objectType)
                    .append(", objectName=").append(objectName).append(", methodName=").append(methodName)
                    .append(", parameters=[");
            for (Object param: parameters) {
                builder.append(param).append(", ");
            }
            builder.append("]]");
            return builder.toString();
        }
    }
}
