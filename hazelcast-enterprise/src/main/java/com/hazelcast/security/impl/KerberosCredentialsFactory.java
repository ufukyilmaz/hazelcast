package com.hazelcast.security.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.security.PrivilegedAction;
import java.util.Properties;

import javax.security.auth.Subject;
import javax.security.auth.callback.CallbackHandler;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;
import org.ietf.jgss.Oid;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.ICredentialsFactory;
import com.hazelcast.security.SimpleTokenCredentials;
import com.hazelcast.security.TokenCredentials;

/**
 * Credentials factory which provides Kerberos service tickets (wrapped as GSS-API token).
 */
public class KerberosCredentialsFactory implements ICredentialsFactory {

    /**
     * Property name which allows to configure static service principal name (SPN).
     * It's meant for usecases where all members share a single Kerberos identity.
     */
    public static final String PROPERTY_SPN = "spn";

    /**
     * Property name for defining prefix of the Service Principal name. It's default value is {@value #DEFAULT_VALUE_PREFIX}.
     * By default the member's principal name (for which this credentials factory asks the service ticket) is in form
     * "[servicePrefix][memberIpAddress]@[REALM]" (e.g. "hz/192.168.1.1@ACME.COM").
     */
    public static final String PROPERTY_PREFIX = "serviceNamePrefix";

    /**
     * Property name for defining Kerberos realm name (e.g. "ACME.COM").
     */
    public static final String PROPERTY_REALM = "realm";

    /**
     * Property name for referencing Security realm name in Hazelcast configuration. The realm's authentication configuration
     * (when defined) can be used to fill the {@link Subject} with Kerberos credentials (e.g. TGT).
     */
    public static final String PROPERTY_SECURITY_REALM = "securityRealm";

    /**
     * Property name which allows using fully qualified domain name instead of IP address when the SPN is constructed from a
     * prefix and realm name. For instance, when set {@code true}, the SPN {@code "hz/192.168.1.1@ACME.COM"} could become
     * {@code "hz/member1.acme.com@ACME.COM"} (given the reverse DNS lookup for 192.168.1.1 returns the "member1.acme.com"
     * hostname).
     */
    public static final String PROPERTY_USE_CANONICAL_HOSTNAME = "useCanonicalHostname";

    /**
     * Default value for {@link #PROPERTY_PREFIX} property.
     */
    public static final String DEFAULT_VALUE_PREFIX = "hz/";

    private static final Oid KRB5_OID;
    static {
        try {
            KRB5_OID = new Oid("1.2.840.113554.1.2.2");
        } catch (GSSException e) {
            throw new RuntimeException(e);
        }
    }

    private final ILogger logger = Logger.getLogger(KerberosCredentialsFactory.class);

    private volatile String spn;
    private volatile String serviceNamePrefix;
    private volatile String serviceRealm;
    private volatile boolean useCanonicalHostname;

    private volatile String securityRealm;
    private volatile CallbackHandler callbackHandler;

    @Override
    public void configure(CallbackHandler callbackHandler) {
        this.callbackHandler = callbackHandler;
    }

    @Override
    public void init(Properties properties) {
        spn = properties.getProperty(PROPERTY_SPN);
        serviceNamePrefix = properties.getProperty(PROPERTY_PREFIX);
        serviceRealm = properties.getProperty(PROPERTY_REALM);
        securityRealm = properties.getProperty(PROPERTY_SECURITY_REALM);
        useCanonicalHostname = Boolean.parseBoolean(properties.getProperty(PROPERTY_USE_CANONICAL_HOSTNAME));
        if (spn != null && serviceNamePrefix != null) {
            throw new InvalidConfigurationException(
                    "Service name must not be configured together with the service name prefix.");
        }
        if (serviceNamePrefix == null) {
            serviceNamePrefix = DEFAULT_VALUE_PREFIX;
        }
    }

    @Override
    public Credentials newCredentials(Address address) {
        String serviceName = spn;
        if (serviceName == null) {
            if (address == null) {
                throw new IllegalArgumentException(
                        "Kerberos Service principal name can't be generated without the address provided.");
            }
            serviceName = serviceNamePrefix + getSpnHostPart(address);
        }
        if (serviceRealm != null) {
            serviceName = serviceName + "@" + serviceRealm;
        }
        Subject subject = SecurityUtil.getRunAsSubject(callbackHandler, securityRealm);
        if (logger.isFineEnabled()) {
            logger.fine("Creating KerberosCredentials for serviceName=" + serviceName + ", Subject=" + subject);
        }
        TokenCredentials token = null;
        if (subject != null) {
            final String tmpName = serviceName;
            token = Subject.doAs(subject, (PrivilegedAction<TokenCredentials>) () -> createTokenCredentials(tmpName));
        } else {
            token = createTokenCredentials(serviceName);
        }
        return token;
    }

    protected String getSpnHostPart(Address address) {
        String host = address.getHost();
        if (useCanonicalHostname) {
            try {
                host = InetAddress.getByName(host).getCanonicalHostName();
            } catch (UnknownHostException e) {
                logger.fine("Getting canonical hostname for the address failed: " + address, e);
            }
        }
        return host;
    }

    @Override
    public Credentials newCredentials() {
        return newCredentials(null);
    }

    @Override
    public void destroy() {
    }

    private TokenCredentials createTokenCredentials(String serviceName) {
        try {
            GSSManager manager = GSSManager.getInstance();
            GSSContext gssContext = manager.createContext(manager.createName(serviceName, null), KRB5_OID, null,
                    GSSContext.DEFAULT_LIFETIME);
            gssContext.requestMutualAuth(false);
            gssContext.requestConf(false);
            gssContext.requestInteg(false);
            byte[] token = gssContext.initSecContext(new byte[0], 0, 0);
            if (!gssContext.isEstablished()) {
                logger.warning("GSSContext was not established in a single step. The TokenCredentials won't be created.");
                return null;
            }
            return new SimpleTokenCredentials(token);
        } catch (GSSException e) {
            logger.warning("Establishing GSSContext failed", e);
        }
        return null;
    }

}
