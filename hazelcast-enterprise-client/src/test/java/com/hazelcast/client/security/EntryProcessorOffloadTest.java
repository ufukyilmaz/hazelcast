package com.hazelcast.client.security;

import static com.hazelcast.config.PermissionConfig.PermissionType.MAP;
import static com.hazelcast.test.HazelcastTestSupport.randomName;
import static java.util.stream.Collectors.joining;
import static java.util.stream.StreamSupport.stream;
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
import com.hazelcast.core.Offloadable;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.map.EntryProcessor;
import com.hazelcast.map.IMap;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
import com.hazelcast.security.permission.ActionConstants;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
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

    public static class MySampleOffloadableEntryProcessor implements EntryProcessor<String, String, String>, Serializable, Offloadable {

        private static final long serialVersionUID = 7196208989609354831L;

        public MySampleOffloadableEntryProcessor() {
        }

        public String getExecutorName() {
            return "TEST-durable-executor-name";
        }

        public String process(Entry<String, String> entry) {
            entry.setValue("orange");
            return Thread.currentThread().getName();
        }

    }

    public static class MySampleEntryProcessor implements EntryProcessor<String, String, String>, Serializable {

        private static final long serialVersionUID = 7196208989609354831L;

        public MySampleEntryProcessor() {
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
            System.out.println("before [credentials=" + credentials + ", objectType=" + objectType + ", objectName="
                    + objectName + ", methodName=" + methodName + ", parameters="
                    + stream(parameters.spliterator(), false).map(String::valueOf).collect(joining()) + "]");
        }

        @Override
        public void after(Credentials credentials, String objectType, String objectName, String methodName,
                Parameters parameters) {
            System.out.println("after [credentials=" + credentials + ", objectType=" + objectType + ", objectName=" + objectName
                    + ", methodName=" + methodName + ", parameters="
                    + stream(parameters.spliterator(), false).map(String::valueOf).collect(joining()) + "]");
        }

    }
}
