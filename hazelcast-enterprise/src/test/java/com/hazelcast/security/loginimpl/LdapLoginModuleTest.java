package com.hazelcast.security.loginimpl;

import static org.junit.Assert.assertEquals;

import java.util.Map;

import javax.naming.Context;
import javax.security.auth.Subject;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.spi.LoginModule;

import org.apache.directory.server.annotations.CreateLdapServer;
import org.apache.directory.server.annotations.CreateTransport;
import org.apache.directory.server.core.annotations.ApplyLdifFiles;
import org.apache.directory.server.core.annotations.CreateDS;
import org.apache.directory.server.core.annotations.CreatePartition;
import org.apache.directory.server.core.integ.CreateLdapServerRule;
import org.apache.directory.server.ldap.LdapServer;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import com.hazelcast.enterprise.EnterpriseParallelParametersRunnerFactory;
import com.hazelcast.security.HazelcastPrincipal;
import com.hazelcast.test.annotation.QuickTest;

/**
 * Tests for {@link LdapLoginModule} which extends {@link BasicLdapLoginModuleTest} and adds test scenarios for user search and
 * password attribute testing.
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
public class LdapLoginModuleTest extends BasicLdapLoginModuleTest {

    @ClassRule
    public static CreateLdapServerRule serverRule = new CreateLdapServerRule();

    @Test
    public void testAuthenticateByNewBind() throws Exception {
        Subject subject = new Subject();
        Map<String, String> options = createBasicLdapOptions();
        options.put(LdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "cn");
        doLogin(getLoginForUid("jduke"), "theduke", subject, options);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertRoles(subject, "Java Duke");
    }

    @Override
    protected String getLoginForUid(String uid) {
        return uid;
    }

    @Test
    public void testAuthenticateByPasswordAttribute() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(LdapLoginModule.OPTION_PASSWORD_ATTRIBUTE, "uid");
        Subject subject = new Subject();
        doLogin("jduke", "jduke", subject, options);
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
    }

    @Test
    public void testAuthenticateByPasswordAttribute_wrongPassword() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(LdapLoginModule.OPTION_PASSWORD_ATTRIBUTE, "uid");
        Subject subject = new Subject();
        expected.expect(FailedLoginException.class);
        doLogin("jduke", "theduke", subject, options);
    }

    @Test
    public void testUserContext_notFound() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(LdapLoginModule.OPTION_USER_CONTEXT, "ou=Roles,dc=hazelcast,dc=com");
        Subject subject = new Subject();
        expected.expect(FailedLoginException.class);
        doLogin("hazelcast", "imdg", subject, options);
    }

    @Test
    public void testUserContext_found() throws Exception {
        Map<String, String> options = createBasicLdapOptions();
        options.put(LdapLoginModule.OPTION_ROLE_MAPPING_ATTRIBUTE, "description");
        options.put(LdapLoginModule.OPTION_USER_CONTEXT, "ou=Users,dc=hazelcast,dc=com");
        options.put(LdapLoginModule.OPTION_USER_FILTER, "(&(cn={login})(objectClass=inetOrgPerson))");
        Subject subject = new Subject();
        doLogin("Best IMDG", "imdg", subject, options);
        assertIdentity(subject, "hazelcast");
        assertRoles(subject, "cn=Role1,ou=Roles,dc=hazelcast,dc=com");
    }

    @Override
    protected LdapServer getLdapServer() {
        return serverRule.getLdapServer();
    }

    protected LoginModule createLoginModule() {
        return new LdapLoginModule();
    }

    protected Map<String, String> createBasicLdapOptions() {
        Map<String, String> options = super.createBasicLdapOptions();
        options.put(Context.SECURITY_AUTHENTICATION, "simple");
        options.put(Context.SECURITY_PRINCIPAL, "uid=admin,ou=system");
        options.put(Context.SECURITY_CREDENTIALS, "secret");
        return options;
    }

}
