/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.client.security;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.config.Config;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.Parameters;
import com.hazelcast.security.SecurityInterceptor;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.security.AccessControlException;

import static com.hazelcast.config.PermissionConfig.PermissionType;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class SecurityInterceptorTest {

    @Before
    @After
    public void cleanupClass() {
        HazelcastClient.shutdownAll();
        Hazelcast.shutdownAll();
    }

    @Test
    public void testException_afterIntercept() {
        final Config config = createConfig();
        final SecurityConfig securityConfig = config.getSecurityConfig();
        final SecurityInterceptorConfig securityInterceptorConfig = new SecurityInterceptorConfig();
        securityInterceptorConfig.setImplementation(new ExceptionThrowingInterceptor(false));
        securityConfig.addSecurityInterceptorConfig(securityInterceptorConfig);

        Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap("map");
        map.put("key", "value");
    }

    @Test(expected = AccessControlException.class)
    public void testException_beforeIntercept() {
        final Config config = createConfig();
        final SecurityConfig securityConfig = config.getSecurityConfig();
        final SecurityInterceptorConfig securityInterceptorConfig = new SecurityInterceptorConfig();
        securityInterceptorConfig.setImplementation(new ExceptionThrowingInterceptor(true));
        securityConfig.addSecurityInterceptorConfig(securityInterceptorConfig);

        Hazelcast.newHazelcastInstance(config);
        final HazelcastInstance client = HazelcastClient.newHazelcastClient();
        final IMap<Object, Object> map = client.getMap("map");
        map.put("key", "value");
    }

    static class ExceptionThrowingInterceptor implements SecurityInterceptor {

        final boolean throwInBefore;

        ExceptionThrowingInterceptor(final boolean throwInBefore) {
            this.throwInBefore = throwInBefore;
        }

        @Override
        public void before(Credentials credentials, String objectType, String objectName,
                           String methodName, Parameters parameters) throws AccessControlException {
            if (throwInBefore) {
                throw new RuntimeException();
            }
        }

        @Override
        public void after(Credentials credentials, String objectType, String objectName,
                          String methodName, Parameters parameters) {
            if (!throwInBefore) {
                throw new RuntimeException();
            }
        }
    }

    private Config createConfig() {
        final Config config = new Config();
        PermissionConfig perm = new PermissionConfig(PermissionType.ALL, "", null);
        config.getSecurityConfig().setEnabled(true).addClientPermissionConfig(perm);
        return config;
    }
}
