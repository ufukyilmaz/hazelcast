package com.hazelcast.security.impl;

import static com.hazelcast.security.ClusterLoginModule.OPTION_SKIP_ENDPOINT;
import static com.hazelcast.security.ClusterLoginModule.OPTION_SKIP_IDENTITY;
import static com.hazelcast.security.ClusterLoginModule.OPTION_SKIP_ROLE;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.security.Principal;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.hazelcast.config.Config;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.security.ClusterEndpointPrincipal;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.ConfigCallback;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.EndpointCallback;
import com.hazelcast.security.HazelcastPrincipal;
import com.hazelcast.security.UsernamePasswordCredentials;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import junit.framework.AssertionFailedError;

/**
 * Unit tests for {@link DefaultLoginModule}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class DefaultLoginModuleTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void testDefaultValues() throws Exception {
        Subject subject = new Subject();
        doLogin(subject, emptyMap(), "dev", "dev-pass", new Config());
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertEquals("Unexpected Identity in the Subject", "dev", getIdentity(subject));
        assertEquals("Unexpected Endpoint in the Subject", "127.0.0.1", getEndpoint(subject));
        assertEquals("Unexpected Role in the Subject", "dev", getRole(subject));
    }

    @Test
    public void testExplicitGroupConfig() throws Exception {
        Subject subject = new Subject();
        doLogin(subject, emptyMap(), "test", "pass", createConfig("test", "pass"));
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertEquals("Unexpected Identity in the Subject", "test", getIdentity(subject));
        assertEquals("Unexpected Endpoint in the Subject", "127.0.0.1", getEndpoint(subject));
        assertEquals("Unexpected Role in the Subject", "test", getRole(subject));
    }

    @Test
    public void testWrongUsername() throws Exception {
        Subject subject = new Subject();
        DefaultLoginModule lm = new DefaultLoginModule();
        lm.initialize(subject, new TestCallbackHandler("test", "pass", createConfig("testX", "pass")), emptyMap(), emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    @Test
    public void testWrongUsernameWithouIdentity() throws Exception {
        Subject subject = new Subject();
        DefaultLoginModule lm = new DefaultLoginModule();
        lm.initialize(subject, new TestCallbackHandler("test", "pass", new Config().setClusterName("testX")), emptyMap(),
                emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    @Test
    public void testWrongPassword() throws Exception {
        Subject subject = new Subject();
        DefaultLoginModule lm = new DefaultLoginModule();
        lm.initialize(subject, new TestCallbackHandler("test", "pass", createConfig("test", "passX")), emptyMap(), emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    @Test
    public void testPasswordNotUsedWithoutIdentity() throws Exception {
        Subject subject = new Subject();
        doLogin(subject, emptyMap(), "test", "whateverpassword", new Config().setClusterName("test"));
        assertEquals("Unexpected Identity in the Subject", "test", getIdentity(subject));
    }

    @Test
    public void testNullConfig() throws Exception {
        Subject subject = new Subject();
        DefaultLoginModule lm = new DefaultLoginModule();
        lm.initialize(subject, new TestCallbackHandler("dev", "dev-pass", null), emptyMap(), emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    @Test
    public void testSkipRole() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_SKIP_ROLE, "true");
        doLogin(subject, options, "dev", "dev-pass", new Config());
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getRole(subject));
        subject.getPrincipals().clear();
        options.put(OPTION_SKIP_ROLE, "false");
        doLogin(subject, options, "dev", "dev-pass", new Config());
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
    }

    @Test
    public void testSkipIdentity() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_SKIP_IDENTITY, "true");
        doLogin(subject, options, "dev", "dev-pass", new Config());
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getIdentity(subject));
    }

    @Test
    public void testSkipEndpoint() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_SKIP_ENDPOINT, "true");
        doLogin(subject, options, "dev", "dev-pass", new Config());
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getEndpoint(subject));
    }

    private Config createConfig(String username, String password) {
        Config config = new Config();
        config.setClusterName(username);
        config.getSecurityConfig().setEnabled(true).setMemberRealmConfig("realm",
                new RealmConfig().setUsernamePasswordIdentityConfig(username, password));
        return config;
    }

    private void doLogin(Subject subject, Map<String, ?> options, String name, String password, Config config)
            throws LoginException {
        DefaultLoginModule lm = new DefaultLoginModule();
        lm.initialize(subject, new TestCallbackHandler(name, password, config), emptyMap(), options);
        lm.login();
        assertEquals("Login should not add Principals to the Subject", 0,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        lm.commit();
    }

    private String getIdentity(Subject subject) {
        return getPrincipalName(subject, ClusterIdentityPrincipal.class);
    }

    private String getEndpoint(Subject subject) {
        return getPrincipalName(subject, ClusterEndpointPrincipal.class);
    }

    private String getRole(Subject subject) {
        return getPrincipalName(subject, ClusterRolePrincipal.class);
    }

    private String getPrincipalName(Subject subject, Class<? extends Principal> c) {
        Set<String> principals = getPrincipalNames(subject, c);
        switch (principals.size()) {
            case 0:
                return null;
            case 1:
                return principals.iterator().next();
            default:
                throw new AssertionFailedError("More then one (" + principals.size() + ") Principal of type "
                        + c.getClass().getSimpleName() + " was found in the Subject");
        }
    }

    private Set<String> getPrincipalNames(Subject subject, Class<? extends Principal> c) {
        return subject.getPrincipals(c).stream().map(Principal::getName).collect(toSet());
    }

    /**
     * Callback handler which handles {@link NameCallback} and {@link PasswordCallback} serving provided values and the
     * {@link EndpointCallback} with a hardcoded value "127.0.0.1".
     */
    static class TestCallbackHandler implements CallbackHandler {

        private final String name;
        private final String password;
        private final Config config;

        TestCallbackHandler(String name, String password, Config config) {
            this.name = name;
            this.password = password;
            this.config = config;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof EndpointCallback) {
                    ((EndpointCallback) cb).setEndpoint("127.0.0.1");
                } else if (cb instanceof NameCallback) {
                    ((NameCallback) cb).setName(name);
                } else if (cb instanceof PasswordCallback) {
                    ((PasswordCallback) cb).setPassword(password == null ? null : password.toCharArray());
                } else if (cb instanceof CredentialsCallback) {
                    ((CredentialsCallback) cb).setCredentials(new UsernamePasswordCredentials(name, password));
                } else if (cb instanceof ConfigCallback) {
                    ((ConfigCallback) cb).setConfig(config);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }
}
