package com.hazelcast.security.impl;

import static com.hazelcast.TestEnvironmentUtil.readResource;
import static com.hazelcast.security.ClusterLoginModule.OPTION_SKIP_ENDPOINT;
import static com.hazelcast.security.ClusterLoginModule.OPTION_SKIP_IDENTITY;
import static com.hazelcast.security.ClusterLoginModule.OPTION_SKIP_ROLE;
import static com.hazelcast.security.impl.X509CertificateLoginModule.OPTION_ROLE_ATTRIBUTE;
import static com.hazelcast.util.StringUtil.stringToBytes;
import static java.util.Collections.emptyMap;
import static java.util.stream.Collectors.toSet;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.Principal;
import java.security.PublicKey;
import java.security.SignatureException;
import java.security.cert.Certificate;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginException;

import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import com.hazelcast.security.CertificatesCallback;
import com.hazelcast.security.ClusterEndpointPrincipal;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterRolePrincipal;
import com.hazelcast.security.EndpointCallback;
import com.hazelcast.security.HazelcastPrincipal;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;

import junit.framework.AssertionFailedError;

/**
 * Unit tests for {@link X509CertificateLoginModule}.
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({ QuickTest.class, ParallelJVMTest.class })
public class X509CertificateLoginModuleTest {

    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    private static final byte[] CERT_CHAIN = readResource(X509CertificateLoginModuleTest.class,
            "/com/hazelcast/nio/ssl/fullchain.pem");
    /**
     * Subject name: {@code "CN=role1,OU=test,CN=Hazelcast,CN=hazelcast,C=CZ"}.
     */
    private static final byte[] CERT_MULTI_CN = stringToBytes("-----BEGIN CERTIFICATE-----\n"
                    + "MIIBwDCCAWOgAwIBAgIEPWQKdjAMBggqhkjOPQQDAgUAMFQxCzAJBgNVBAYTAkNa\n"
                    + "MRIwEAYDVQQDEwloYXplbGNhc3QxEjAQBgNVBAMTCUhhemVsY2FzdDENMAsGA1UE\n"
                    + "CxMEdGVzdDEOMAwGA1UEAxMFcm9sZTEwHhcNMTkwNTMxMTIyMjUxWhcNMzkwNTI2\n"
                    + "MTIyMjUxWjBUMQswCQYDVQQGEwJDWjESMBAGA1UEAxMJaGF6ZWxjYXN0MRIwEAYD\n"
                    + "VQQDEwlIYXplbGNhc3QxDTALBgNVBAsTBHRlc3QxDjAMBgNVBAMTBXJvbGUxMFkw\n"
                    + "EwYHKoZIzj0CAQYIKoZIzj0DAQcDQgAE/I/a7/D8vymPIOPbcVjB2Y4d6KqIRDCa\n"
                    + "Y7WC+YpFRnw/t8n24wXT9J/SNthJcZHw2HTtrIzu45qB1orSdR58J6MhMB8wHQYD\n"
                    + "VR0OBBYEFI5JYi0raYr3A6/zX3Vyc2uVS0/iMAwGCCqGSM49BAMCBQADSQAwRgIh\n"
                    + "AKAHXF4YNBsfXsj+LIeaQaCnXb77ljzHkSyBVhRmq6j1AiEAiHu+AkhEnelx6LGf\n"
                    + "irp3QV0GYW2w9/relFiywoDFkbM=\n"
                    + "-----END CERTIFICATE-----");

    @Test
    public void testDefaultRoleAttribute() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_CHAIN));
        Subject subject = new Subject();
        doLogin(subject, emptyMap(), certs);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertEquals("Unexpected Identity in the Subject", "CN=server,O=Hazelcast Test,C=US", getIdentity(subject));
        assertEquals("Unexpected Endpoint in the Subject", "127.0.0.1", getEndpoint(subject));
        assertEquals("Unexpected Role in the Subject", "server", getRole(subject));
    }

    @Test
    public void testRoleAttributeFound() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_CHAIN));
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_ROLE_ATTRIBUTE, "C");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertEquals("Unexpected Identity in the Subject", "CN=server,O=Hazelcast Test,C=US", getIdentity(subject));
        assertEquals("Unexpected Endpoint in the Subject", "127.0.0.1", getEndpoint(subject));
        assertEquals("Unexpected Role in the Subject", "US", getRole(subject));
    }

    @Test
    public void testRoleAttributeNotFound() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_CHAIN));
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_ROLE_ATTRIBUTE, "SN");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getRole(subject));
    }

    @Test
    public void testRoleAttributeMultiValue() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_MULTI_CN));
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_ROLE_ATTRIBUTE, "CN");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 5,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertEquals("Unexpected Identity in the Subject", "CN=role1,OU=test,CN=Hazelcast,CN=hazelcast,C=CZ",
                getIdentity(subject));
        HashSet<String> expected = new HashSet<String>();
        expected.add("role1");
        expected.add("Hazelcast");
        expected.add("hazelcast");
        assertEquals("Unexpected Role in the Subject", expected, getPrincipalNames(subject, ClusterRolePrincipal.class));
    }

    @Test
    public void testSkipRole() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_MULTI_CN));
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_ROLE_ATTRIBUTE, "OU");
        options.put(OPTION_SKIP_ROLE, "true");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getRole(subject));
        subject.getPrincipals().clear();
        options.put(OPTION_SKIP_ROLE, "false");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 3,
                subject.getPrincipals(HazelcastPrincipal.class).size());
    }

    @Test
    public void testSkipIdentity() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_CHAIN));
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_SKIP_IDENTITY, "true");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getIdentity(subject));
    }

    @Test
    public void testSkipEndpoint() throws Exception {
        Certificate[] certs = toArray(generateCertificates(CERT_CHAIN));
        Subject subject = new Subject();
        Map<String, String> options = new HashMap<>();
        options.put(OPTION_SKIP_ENDPOINT, "true");
        doLogin(subject, options, certs);
        assertEquals("Unexpected number or principals in the Subject", 2,
                subject.getPrincipals(HazelcastPrincipal.class).size());
        assertNull(getEndpoint(subject));
    }

    @Test
    public void testNullCertChain() throws Exception {
        Subject subject = new Subject();
        X509CertificateLoginModule lm = new X509CertificateLoginModule();
        lm.initialize(subject, new TestCallbackHandler(null), emptyMap(), emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    @Test
    public void testEmptyCertChain() throws LoginException {
        Subject subject = new Subject();
        X509CertificateLoginModule lm = new X509CertificateLoginModule();
        lm.initialize(subject, new TestCallbackHandler(new Certificate[0]), emptyMap(), emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    @Test
    public void testCertificateNotX509() throws Exception {
        Certificate[] certs = new Certificate[] { new TestCertificate("Foo") };
        Subject subject = new Subject();
        X509CertificateLoginModule lm = new X509CertificateLoginModule();
        lm.initialize(subject, new TestCallbackHandler(certs), emptyMap(), emptyMap());
        expectedException.expect(LoginException.class);
        lm.login();
    }

    private void doLogin(Subject subject, Map<String, ?> options, Certificate[] certs) throws LoginException {
        X509CertificateLoginModule lm = new X509CertificateLoginModule();
        lm.initialize(subject, new TestCallbackHandler(certs), emptyMap(), options);
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

    private Certificate[] toArray(Collection<? extends Certificate> certs) {
        if (certs == null) {
            return null;
        }
        ArrayList<? extends Certificate> arrayList = new ArrayList<>(certs);
        return arrayList.toArray(new Certificate[arrayList.size()]);
    }

    private static Collection<? extends Certificate> generateCertificates(byte[] certBytes) throws CertificateException {
        if (certBytes == null) {
            return null;
        }
        ByteArrayInputStream bais = new ByteArrayInputStream(certBytes);
        return generateCertificates(bais);
    }

    private static Collection<? extends Certificate> generateCertificates(InputStream is) throws CertificateException {
        CertificateFactory cf = CertificateFactory.getInstance("X.509");
        return cf.generateCertificates(is);
    }

    /**
     * Callback handler which handles {@link EndpointCallback} (hardcoded value "127.0.0.1") and {@link CertificatesCallback}.
     */
    static class TestCallbackHandler implements CallbackHandler {
        Certificate[] certs;

        TestCallbackHandler(Certificate[] certs) {
            this.certs = certs;
        }

        @Override
        public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
            for (Callback cb : callbacks) {
                if (cb instanceof EndpointCallback) {
                    ((EndpointCallback) cb).setEndpoint("127.0.0.1");
                } else if (cb instanceof CertificatesCallback) {
                    ((CertificatesCallback) cb).setCertificates(certs);
                } else {
                    throw new UnsupportedCallbackException(cb);
                }
            }
        }
    }

    /**
     * Dummy {@link Certificate} implementation.
     */
    public static class TestCertificate extends Certificate {

        protected TestCertificate(String type) {
            super(type);
        }

        @Override
        public byte[] getEncoded() throws CertificateEncodingException {
            return null;
        }

        @Override
        public void verify(PublicKey key) throws CertificateException, NoSuchAlgorithmException, InvalidKeyException,
                NoSuchProviderException, SignatureException {
        }

        @Override
        public void verify(PublicKey key, String sigProvider) throws CertificateException, NoSuchAlgorithmException,
                InvalidKeyException, NoSuchProviderException, SignatureException {
        }

        @Override
        public String toString() {
            return null;
        }

        @Override
        public PublicKey getPublicKey() {
            return null;
        }
    }
}
