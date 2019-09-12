package com.hazelcast.security.impl;

import static com.hazelcast.security.impl.LdapUtils.getAttributeValues;

import java.io.IOException;
import java.security.cert.Certificate;
import java.security.cert.X509Certificate;
import java.util.Collection;

import javax.naming.NamingException;
import javax.naming.ldap.LdapName;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import com.hazelcast.security.CertificatesCallback;
import com.hazelcast.security.ClusterIdentityPrincipal;
import com.hazelcast.security.ClusterLoginModule;

/**
 * Hazelcast LoginModule implementation for use together with mutual TLS authentication. This login module leaves the
 * authentication step on the TLS implementation and just verifies a X.509 certificate is present for the connection. The main
 * usage of the login module is for the client authorization (assigning roles). The role names are parsed from a attribute in
 * the X.500 certificate Subject name. Full Subject DN is used as a name in the {@link ClusterIdentityPrincipal}.
 * <p>
 * Samples:
 *
 * <pre>
 * Client comes with a Certificate with Subject DN "CN=server,O=Hazelcast,C=US" and no option is specified for this login module.
 * Assigned principals:
 * - ClusterIdentityPrincipal: "CN=server,O=Hazelcast,C=US"
 * - ClusterRolePrinpical: "server"
 * - ClusterEndpointPrincipal: [IP address of the client]
 *
 * If option roleAttribute=O is used in the same scenario, then identity and endpoint will remain unchanged and role will change:
 * - ClusterRolePrinpical: "Hazelcast"
 *
 * If option roleAttribute=SN is used in the same scenario, then no role will be assigned - i.e. no ClusterRolePrincipal
 * in the Subject.
 *
 * If multiple values are present for the role attribute, then all values are assigned as a role name.
 * E.g. "cn=X, cn=Y, cn=Z, ou=Engineering, o=ACME, c=CZ" with default role attribute value ("cn") will result in following
 * role principals:
 * - ClusterRolePrinpical: "X", "Y", "Z"
 * </pre>
 */
public class X509CertificateLoginModule extends ClusterLoginModule {

    /**
     * Login module option name under which role name attribute is stored.
     */
    public static final String OPTION_ROLE_ATTRIBUTE = "roleAttribute";
    /**
     * Default value for {@value #OPTION_ROLE_ATTRIBUTE} attribute.
     */
    public static final String DEFAULT_ROLE_ATTRIBUTE = "cn";

    private String name;

    @Override
    public boolean onLogin() throws LoginException {
        CertificatesCallback cb = new CertificatesCallback();
        try {
            callbackHandler.handle(new Callback[] { cb });
        } catch (IOException | UnsupportedCallbackException e) {
            throw new FailedLoginException("Unable to retrieve Certificates. " + e.getMessage());
        }
        Certificate[] certs = cb.getCertificates();
        if (certs == null || certs.length == 0 || !(certs[0] instanceof X509Certificate)) {
            throw new FailedLoginException("No valid X.509 certificate found");
        }
        X509Certificate x509Cert = (X509Certificate) certs[0];
        String roleAttribute = getStringOption(OPTION_ROLE_ATTRIBUTE, DEFAULT_ROLE_ATTRIBUTE);
        name = x509Cert.getSubjectX500Principal().getName();
        if (!isSkipRole()) {
            try {
                LdapName ldapName = new LdapName(name);
                Collection<String> roles = getAttributeValues(ldapName, roleAttribute);
                for (String role : roles) {
                    addRole(role);
                }
            } catch (NamingException e) {
                throw new FailedLoginException("Unable to parse Subject name from the X.509 certificate" + name);
            }
        }
        return true;
    }

    @Override
    protected String getName() {
        return name;
    }

}
