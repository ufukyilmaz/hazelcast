package com.hazelcast.internal.ascii;

import com.hazelcast.config.Config;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.core.HazelcastInstance;
import org.junit.Test;

import java.util.logging.Level;

import static com.hazelcast.test.AbstractHazelcastClassRunner.getTestMethodName;
import static java.net.HttpURLConnection.HTTP_FORBIDDEN;
import static org.junit.Assert.assertEquals;

public class RestLogLevelEnterpriseWithSecurityTest extends RestLogLevelEnterpriseTest {

    private static final String WRONG_PASSWORD = "foo";

    @Override
    protected String getPassword() {
        return "dev-pass";
    }

    @Override
    protected Config createConfig() {
        Config config = super.createConfig();
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("realm",
                new RealmConfig().setUsernamePasswordIdentityConfig(getTestMethodName(), getPassword()));
        return config;
    }

    @Test
    public void testWrongPasswordWithSecurity() throws Exception {
        Config config = createReadWriteConfig();
        HazelcastInstance instance = factory.newHazelcastInstance(config);
        HTTPCommunicator communicator = new HTTPCommunicator(instance);
        String clusterName = config.getClusterName();
        assertEquals(HTTP_FORBIDDEN, communicator.setLogLevel(clusterName, WRONG_PASSWORD, Level.FINE).responseCode);
        assertEquals(HTTP_FORBIDDEN, communicator.resetLogLevel(clusterName, WRONG_PASSWORD).responseCode);
    }

}
