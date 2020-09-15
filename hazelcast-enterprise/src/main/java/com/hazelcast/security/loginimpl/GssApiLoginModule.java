package com.hazelcast.security.loginimpl;

import static com.hazelcast.security.impl.SecurityUtil.createKerberosJaasRealmConfig;
import static com.hazelcast.security.impl.SecurityUtil.getRunAsSubject;

import java.io.IOException;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import java.util.Base64;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSException;
import org.ietf.jgss.GSSManager;

import com.hazelcast.config.InvalidConfigurationException;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.security.ClusterLoginModule;
import com.hazelcast.security.Credentials;
import com.hazelcast.security.CredentialsCallback;
import com.hazelcast.security.TokenCredentials;
import com.hazelcast.security.impl.SecurityUtil;

/**
 * Hazelcast GSS-API LoginModule implementation.
 */
public class GssApiLoginModule extends ClusterLoginModule {

    /**
     * Name of the option which allows disabling some of the checks on incoming token (e.g. passes authentication even if the
     * mutual authentication is required by the token).
     */
    public static final String OPTION_RELAX_FLAGS_CHECK = "relaxFlagsCheck";

    /**
     * Option name for referencing Security realm name in Hazelcast configuration. The realm's authentication configuration
     * (when defined) will be used to fill the {@link Subject} with Kerberos credentials (e.g. KeyTab entry).
     */
    public static final String OPTION_SECURITY_REALM = "securityRealm";

    /**
     * Option name which allows cutting off the Kerberos realm part from authenticated name. When the property value is set to
     * {@code true}, the {@code '@REALM'} part is removed from the name (e.g. {@code jduke@ACME.COM} becomes {@code jduke}).
     */
    public static final String OPTION_USE_NAME_WITHOUT_REALM = "useNameWithoutRealm";

    /**
     * Option name which allows (together with the {@link #OPTION_PRINCIPAL}) simplification of security realm
     * configurations. For basic scenarios you don't need to specify the {@link #OPTION_SECURITY_REALM}, but you can instead
     * define directly kerberos principal name and keytab file path with credentials for given principal.
     * <p>
     * This property is only used when the {@link #OPTION_SECURITY_REALM} is not configured.
     */
    public static final String OPTION_KEYTAB_FILE = "keytabFile";

    /**
     * Option name which allows (together with the {@link #OPTION_KEYTAB_FILE}) simplification of security realm
     * configurations. For basic scenarios you don't need to specify the {@link #OPTION_SECURITY_REALM}, but you can instead
     * define directly kerberos principal name and keytab file path with credentials for given principal.
     * <p>
     * This property is only used when the {@link #OPTION_SECURITY_REALM} is not configured.
     */
    public static final String OPTION_PRINCIPAL = "principal";

    private static final AtomicBoolean KRB5_REALM_GENERATED_WARNING_PRINTED = new AtomicBoolean(false);

    private String name;

    @Override
    protected void onInitialize() {
        super.onInitialize();
        String securityRealm  = getStringOption(OPTION_SECURITY_REALM, null);
        String keytabFile = getStringOption(OPTION_KEYTAB_FILE, null);
        String principal = getStringOption(OPTION_PRINCIPAL, null);
        if (securityRealm != null && (principal != null || keytabFile != null)) {
            throw new InvalidConfigurationException(
                    "The principal and keytabFile must not be configured when securityRealm is used.");
        }
    }

    @Override
    @SuppressWarnings("checkstyle:NPathComplexity")
    public boolean onLogin() throws LoginException {
        CredentialsCallback cc = new CredentialsCallback();
        try {
            callbackHandler.handle(new Callback[] { cc });
        } catch (IOException | UnsupportedCallbackException e) {
            throw new FailedLoginException("Unable to retrieve Certificates. " + e.getMessage());
        }
        Credentials creds = cc.getCredentials();
        if (creds == null || !(creds instanceof TokenCredentials)) {
            throw new FailedLoginException("No valid TokenCredentials found");
        }
        TokenCredentials tokenCreds = (TokenCredentials) creds;
        byte[] token = tokenCreds.getToken();
        if (token == null) {
            throw new FailedLoginException("No token found in TokenCredentials.");
        }
        if (logger.isFineEnabled()) {
            logger.fine("Received Token: " + Base64.getEncoder().encodeToString(token));
        }
        String krbAcceptorRealmName = getStringOption(OPTION_SECURITY_REALM, null);
        Subject subject;
        if (krbAcceptorRealmName != null) {
            subject = getRunAsSubject(callbackHandler, krbAcceptorRealmName);
        } else {
            String keytabPath = getStringOption(OPTION_KEYTAB_FILE, null);
            String principal = getStringOption(OPTION_PRINCIPAL, "*");
            RealmConfig realmConfig = createKerberosJaasRealmConfig(principal, keytabPath, false);
            if (realmConfig != null && KRB5_REALM_GENERATED_WARNING_PRINTED.compareAndSet(false, true)) {
                logger.warning("Using generated Kerberos acceptor realm configuration is not intended for production use. "
                        + "It's recommended to properly configure the Krb5LoginModule manually to fit your needs. "
                        + "Following configuration was generated from provided keytab and principal properties:\n"
                        + SecurityUtil.generateRealmConfigXml(realmConfig, "krb5Acceptor"));
            }
            subject = getRunAsSubject(callbackHandler, realmConfig);
        }
        if (subject != null) {
            try {
                Subject.doAs(subject, (PrivilegedExceptionAction<Void>) () -> acceptToken(token));
            } catch (PrivilegedActionException e) {
                if (e.getCause() instanceof LoginException) {
                    throw (LoginException) e.getCause();
                } else {
                    LoginException loginException = new LoginException("Accepting the token failed");
                    loginException.initCause(e.getCause());
                    throw loginException;
                }
            }
        } else {
            acceptToken(token);
        }
        return true;
    }

    private Void acceptToken(byte[] token) throws LoginException {
        GSSManager gssManager = GSSManager.getInstance();
        try {
            GSSContext gssContext = gssManager.createContext((GSSCredential) null);
            token = gssContext.acceptSecContext(token, 0, token.length);

            boolean relaxChecks = getBoolOption(OPTION_RELAX_FLAGS_CHECK, false);
            if (!gssContext.isEstablished()) {
                throw new FailedLoginException("Multi-step negotiation is not supported by this login module");
            }
            if (!relaxChecks) {
                if (token != null && token.length > 0) {
                    // sending a token back to client is not supported
                    throw new FailedLoginException("Mutual authentication is not supported by this login module");
                }
                if (gssContext.getConfState() || gssContext.getIntegState()) {
                    throw new FailedLoginException("Confidentiality and data integrity is not provided by this login module.");
                }
            }
            name = getAuthenticatedName(gssContext);
            if (!getBoolOption(OPTION_SKIP_ROLE, false)) {
                addRole(name);
            }
        } catch (GSSException e) {
            logger.fine("Accepting the GSS-API token failed.", e);
            throw new LoginException("Accepting the GSS-API token failed. " + e.getMessage());
        }
        return null;
    }

    protected String getAuthenticatedName(GSSContext gssContext) throws GSSException {
        String srcName = gssContext.getSrcName().toString();
        if (getBoolOption(OPTION_USE_NAME_WITHOUT_REALM, false)) {
            int pos = srcName.lastIndexOf('@');
            if (pos > -1) {
                srcName = srcName.substring(0, pos);
            }
        }
        return srcName;
    }

    @Override
    protected String getName() {
        return name;
    }

}
