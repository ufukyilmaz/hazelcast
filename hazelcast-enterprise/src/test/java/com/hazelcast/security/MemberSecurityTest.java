package com.hazelcast.security;

import com.hazelcast.config.Config;
import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.nio.serialization.PortableReader;
import com.hazelcast.nio.serialization.PortableWriter;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.TestHazelcastInstanceFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Properties;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class MemberSecurityTest extends HazelcastTestSupport {

    @Test
    public void testAcceptMember() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();

        factory.newHazelcastInstance(config); // master
        HazelcastInstance member = factory.newHazelcastInstance(config);
        assertClusterSize(2, member);
    }

    @Test(expected = IllegalStateException.class)
    public void testDenyMemberWrongCredentials() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);
        CredentialsFactoryConfig credentialsFactoryConfig = new CredentialsFactoryConfig();
        credentialsFactoryConfig.setImplementation(new ICredentialsFactory() {
            @Override
            public Credentials newCredentials() {
                return new UsernamePasswordCredentials("invalid", "credentials");
            }

            @Override
            public void destroy() {
            }

            @Override
            public void configure(GroupConfig groupConfig, Properties properties) {
            }
        });
        secCfg.setMemberCredentialsConfig(credentialsFactoryConfig);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        factory.newHazelcastInstance(config);
    }

    public static class InValidCredentials extends AbstractCredentials {

        public InValidCredentials() {
            super("invalid-group-name");
        }

        @Override
        protected void writePortableInternal(PortableWriter writer) {
        }

        @Override
        protected void readPortableInternal(PortableReader reader) {
        }

        @Override
        public int getFactoryId() {
            return 1234;
        }

        @Override
        public int getClassId() {
            return 1;
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testDenyMember() {
        final Config config = new Config();
        final SecurityConfig secCfg = config.getSecurityConfig();
        secCfg.setEnabled(true);

        final TestHazelcastInstanceFactory factory = createHazelcastInstanceFactory();
        factory.newHazelcastInstance(config); // master
        factory.newHazelcastInstance(new Config());
    }
}
