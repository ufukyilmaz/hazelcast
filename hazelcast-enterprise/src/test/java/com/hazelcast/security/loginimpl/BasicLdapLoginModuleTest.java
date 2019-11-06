package com.hazelcast.security.loginimpl;

import static com.hazelcast.test.HazelcastTestSupport.assertTrueEventually;
import static java.util.Collections.emptyMap;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.directory.SearchControls;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.directory.api.util.DummySSLSocketFactory;
import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.LdapServer;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.security.CertificatesCallback;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.EndpointCallback;
import com.hazelcast.security.HazelcastPrincipal;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests for {@link BasicLdapLoginModule} which use embedded ApacheDS LDAP server to test against.
 */
@CreateDS(name = "myDS",
        partitions = {
            @CreatePartition(name = "test", suffix = "dc=hazelcast,dc=com")
        })
@CreateLdapServer(
        transports = {
            @CreateTransport(protocol = "LDAP", address = "127.0.0.1"),
            @CreateTransport(protocol = "LDAPS", address = "127.0.0.1", ssl = true),
        },
        keyStore = "src/test/resources/com/hazelcast/nio/ssl/ldap.jks",
        certificatePassword = "123456")
@ApplyLdifFiles({"hazelcast.com.ldif"})
@RunWith(Parameterized.class)
@UseParametersRunnerFactory(EnterpriseParallelParametersRunnerFactory.class)
@Category({ QuickTest.class })
public class BasicLdapLoginModuleTest {

    @ClassRule
    public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

    @Rule
    public ExpectedException expected = ExpectedException.none();

    @Parameter
    public boolean tls;

    @Parameters(name = "TLS:{0}")
    public static Object[] data() {
        return new Object[] { false, true };
    }

    @BeforeClass
    public static void beforeClass() {
        assertTrueEventually(() -> verifyLdapSearch(serverRule.getLdapServer()));
    }

    @Test
    public void testAuthentication() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "cn");
        doLogin("jduke", "theduke", subject, options);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertRoles(subject, "Java Duke");
    }

    @Test
    public void testAuthenticateByNewBind_wrongPassword() throws Exception {
        Subject subject = new Subject();
        expected.expect(FailedLoginException.class);
        doLogin("jduke", "foo", subject, createBasicLdapOptions());
    }

    @Test
    public void testAttributeRoleMapping() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "attribute");
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "description");
        options.put(BasicLdapLoginModule.OPTION_USER_NAME_ATTRIBUTE, "cn");
        Subject subject = new Subject();
        doLogin("hazelcast", "imdg", subject, options);
        assertIdentity(subject, "Best IMDG");
        assertRoles(subject, "cn=Role1,ou=Roles,dc=hazelcast,dc=com");
    }

    @Test
    public void testAttributeRoleMappingParseDN() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "attribute");
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "description");
        options.put(BasicLdapLoginModule.OPTION_PARSE_DN, "true");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "dc");
        Subject subject = new Subject();
        doLogin("hazelcast", "imdg", subject, options);
        assertRoles(subject, "hazelcast", "com");
    }

    @Test
    public void testDirectRoleMapping() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "direct");
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "description");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        Subject subject = new Subject();
        doLogin("hazelcast", "imdg", subject, options);
        assertRoles(subject, "Role1");
    }

    @Test
    public void testDirectRoleMappingRecursion() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "direct");
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "description");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        options.put(BasicLdapLoginModule.OPTION_ROLE_RECURSION_MAX_DEPTH, "5");
        Subject subject = new Subject();
        doLogin("hazelcast", "imdg", subject, options);
        assertRoles(subject, "Role1", "Role2", "Role3");
    }

    @Test
    public void testReverseRoleMapping() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "reverse");
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "member");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        Subject subject = new Subject();
        doLogin("jduke", "theduke", subject, options);
        assertRoles(subject, "Admin");
    }

    @Test
    public void testReverseRoleMappingRecursion() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "reverse");
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "member");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        options.put(BasicLdapLoginModule.OPTION_ROLE_RECURSION_MAX_DEPTH, "5");
        Subject subject = new Subject();
        doLogin("jduke", "theduke", subject, options);
        assertRoles(subject, "Admin", "Dev");
    }

    @Test
    public void testSearchFilterOneLevel() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "reverse");
        options.put(BasicLdapLoginModule.OPTION_ROLE_SEARCH_SCOPE, "one-level");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        options.put(BasicLdapLoginModule.OPTION_ROLE_FILTER, "(cn=Admin)");
        Subject subject = new Subject();
        doLogin("jduke", "theduke", subject, options);
        assertRoles(subject);
    }

    @Test
    public void testSearchFilterOneLevel2() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "reverse");
        options.put(BasicLdapLoginModule.OPTION_ROLE_SEARCH_SCOPE, "one-level");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        options.put(BasicLdapLoginModule.OPTION_ROLE_FILTER, "(cn=Admin)");
        options.put(BasicLdapLoginModule.OPTION_ROLE_CONTEXT, "ou=Roles,dc=hazelcast,dc=com");
        Subject subject = new Subject();
        doLogin("jduke", "theduke", subject, options);
        assertRoles(subject, "Admin");
    }

    @Test
    public void testSearchFilter() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(BasicLdapLoginModule.OPTION_ROLE_MAPPING_MODE, "reverse");
        options.put(BasicLdapLoginModule.OPTION_ROLE_NAME_ATTRIBUTE, "cn");
        options.put(BasicLdapLoginModule.OPTION_ROLE_FILTER, "(member={memberDN})");
        options.put(BasicLdapLoginModule.OPTION_ROLE_CONTEXT, "ou=Roles,dc=hazelcast,dc=com");
        options.put(BasicLdapLoginModule.OPTION_ROLE_RECURSION_MAX_DEPTH, "5");
        Subject subject = new Subject();
        doLogin("jduke", "theduke", subject, options);
        assertRoles(subject, "Admin", "Dev");
    }

    @Test
    public void testLdapUrlFallback() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        // use an arbitrary unasigned IP address to test LDAP failover.
        // An IPv4 address from the multicast range is used in thistest
        options.put(Context.PROVIDER_URL, "ldap://224.0.0.3 " + getServerUrl());
        Subject subject = new Subject();
        doLogin("jduke", "theduke", subject, options);
        assertIdentity(subject, "jduke");
    }

    protected Map<String, String> createBasicLdapOptions() {
        Map<String, String> options = new HashMap<>();
        options.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        options.put(Context.PROVIDER_URL, getServerUrl());
        if (tls) {
            options.put("java.naming.ldap.factory.socket", DummySSLSocketFactory.class.getName());
        }
        return options;
    }

    protected LdapServer getLdapServer() {
        return serverRule.getLdapServer();
    }

    private String getServerUrl() {
        return tls
                ? "ldaps://127.0.0.1:" + getLdapServer().getPortSSL()
                : "ldap://127.0.0.1:" + getLdapServer().getPort();
    }

    protected String getLoginForUid(String uid) {
        return "uid=" + uid + ",ou=Users,dc=hazelcast,dc=com";
    }

    protected void assertRoles(Subject subject, String... roles) {
        Set<String> rolesInSubject = subject.getPrincipals(ClusterRolePrincipal.class).stream()
                .map(p -> p.getName())
                .collect(Collectors.toSet());
        assertEquals("Unexpected number of roles in the Subject", roles.length, rolesInSubject.size());
        for (String role: roles) {
            if (!rolesInSubject.contains(role)) {
                fail("Role '" + role + "' was not found in the Subject");
            }
        }
    }

    protected void assertIdentity(Subject subject, String expected) {
        Set<String> identitiesInSubject = subject.getPrincipals(ClusterIdentityPrincipal.class).stream()
                .map(p -> p.getName())
                .collect(Collectors.toSet());
        assertEquals("Unexpected number of roles in the Subject", 1, identitiesInSubject.size());
        if (!identitiesInSubject.contains(expected)) {
            fail("Identity '" + expected + "' was not found in the Subject");
        }
    }

    protected void doLogin(String uid, String password, Subject subject, Map<String, ?> options) throws LoginException {
        LoginModule lm = createLoginModule();
        lm.initialize(subject, new TestCallbackHandler(getLoginForUid(uid), password), emptyMap(), options);
        lm.login();
        assertEquals("Login should not add Principals to the Subject", 0,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        lm.commit();
    }

    protected LoginModule createLoginModule() {
        return new BasicLdapLoginModule();
    }

    protected static void verifyLdapSearch(LdapServer ldapServer) throws Exception {
        String ldapUrl = "ldap://127.0.0.1:" + ldapServer.getPort();
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.put(Context.PROVIDER_URL, ldapUrl);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        env.put(Context.SECURITY_CREDENTIALS, "secret");
        LdapContext ctx = new InitialLdapContext(env, null);
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(SearchControls.SUBTREE_SCOPE);
        NamingEnumeration<?> namingEnum = ctx.search("dc=hazelcast,dc=com", "(uid=jduke)", searchControls);
        assertTrue(namingEnum.hasMore());
    }

    /**
     * Callback handler which handles {@link EndpointCallback} (hardcoded value "127.0.0.1") and {@link CertificatesCallback}.
     */
    static class TestCallbackHandler implements CallbackHandler {
        private final String username;
        private final String password;

        TestCallbackHandler(String username, String password) {
            this.username = username;
            this.password = password;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof EndpointCallback) {
                    ((EndpointCallback) cb).setEndpoint("127.0.0.1");
                } else if (cb instanceof NameCallback) {
                    ((NameCallback) cb).setName(username);
                } else if (cb instanceof PasswordCallback) {
                    ((PasswordCallback) cb).setPassword(password == null ? null : password.toCharArray());
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }
}
