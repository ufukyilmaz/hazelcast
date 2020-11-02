package com.hazelcast.security.impl;

import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.ConfigXmlGenerator;
import com.hazelcast.config.ConfigXmlGenerator.XmlGenerator;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.security.JaasAuthenticationConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.internal.util.AddressUtil;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.security.RealmConfigCallback;
import com.hazelcast.security.permission.AllPermissions;
import com.hazelcast.security.permission.AtomicLongPermission;
import com.hazelcast.security.permission.AtomicReferencePermission;
import com.hazelcast.security.permission.CachePermission;
import com.hazelcast.security.permission.CardinalityEstimatorPermission;
import com.hazelcast.security.permission.ClusterPermission;
import com.hazelcast.security.permission.ConfigPermission;
import com.hazelcast.security.permission.CountDownLatchPermission;
import com.hazelcast.security.permission.DurableExecutorServicePermission;
import com.hazelcast.security.permission.ExecutorServicePermission;
import com.hazelcast.security.permission.FlakeIdGeneratorPermission;
import com.hazelcast.security.permission.ListPermission;
import com.hazelcast.security.permission.LockPermission;
import com.hazelcast.security.permission.MapPermission;
import com.hazelcast.security.permission.MultiMapPermission;
import com.hazelcast.security.permission.PNCounterPermission;
import com.hazelcast.security.permission.QueuePermission;
import com.hazelcast.security.permission.ReliableTopicPermission;
import com.hazelcast.security.permission.ReplicatedMapPermission;
import com.hazelcast.security.permission.RingBufferPermission;
import com.hazelcast.security.permission.ScheduledExecutorPermission;
import com.hazelcast.security.permission.SemaphorePermission;
import com.hazelcast.security.permission.SetPermission;
import com.hazelcast.security.permission.TopicPermission;
import com.hazelcast.security.permission.TransactionPermission;
import com.hazelcast.security.permission.UserCodeDeploymentPermission;

import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;

import static com.hazelcast.internal.util.StringUtil.formatXml;
import static com.hazelcast.internal.util.StringUtil.lowerCaseInternal;

import java.io.File;
import java.io.IOException;
import java.util.Set;

/**
 * Helper methods related to Hazelcast security routines.
 */
@SuppressWarnings({ "checkstyle:classdataabstractioncoupling", "checkstyle:ClassFanOutComplexity" })
public final class SecurityUtil {

    private static final String TEMP_LOGIN_CONTEXT_NAME = "realmConfigLogin";
    private static final String FQCN_KRB5LOGINMODULE_SUN = "com.sun.security.auth.module.Krb5LoginModule";
    private static final String FQCN_KRB5LOGINMODULE_IBM = "com.ibm.security.auth.module.Krb5LoginModule";

    private static final ILogger LOGGER = Logger.getLogger(SecurityUtil.class);

    private static final ThreadLocal<Boolean> SECURE_CALL = new ThreadLocal<Boolean>();

    private SecurityUtil() {
    }

    @SuppressWarnings({"checkstyle:cyclomaticcomplexity", "checkstyle:returncount"})
    public static ClusterPermission createPermission(PermissionConfig permissionConfig) {
        final Set<String> actionSet = permissionConfig.getActions();
        final String[] actions = actionSet.toArray(new String[0]);
        switch (permissionConfig.getType()) {
            case ALL:
                return new AllPermissions();
            case MAP:
                return new MapPermission(permissionConfig.getName(), actions);
            case QUEUE:
                return new QueuePermission(permissionConfig.getName(), actions);
            case ATOMIC_LONG:
                return new AtomicLongPermission(permissionConfig.getName(), actions);
            case ATOMIC_REFERENCE:
                return new AtomicReferencePermission(permissionConfig.getName(), actions);
            case COUNTDOWN_LATCH:
                return new CountDownLatchPermission(permissionConfig.getName(), actions);
            case EXECUTOR_SERVICE:
                return new ExecutorServicePermission(permissionConfig.getName(), actions);
            case LIST:
                return new ListPermission(permissionConfig.getName(), actions);
            case LOCK:
                return new LockPermission(permissionConfig.getName(), actions);
            case MULTIMAP:
                return new MultiMapPermission(permissionConfig.getName(), actions);
            case SEMAPHORE:
                return new SemaphorePermission(permissionConfig.getName(), actions);
            case SET:
                return new SetPermission(permissionConfig.getName(), actions);
            case TOPIC:
                return new TopicPermission(permissionConfig.getName(), actions);
            case FLAKE_ID_GENERATOR:
                return new FlakeIdGeneratorPermission(permissionConfig.getName(), actions);
            case TRANSACTION:
                return new TransactionPermission();
            case DURABLE_EXECUTOR_SERVICE:
                return new DurableExecutorServicePermission(permissionConfig.getName(), actions);
            case CARDINALITY_ESTIMATOR:
                return new CardinalityEstimatorPermission(permissionConfig.getName(), actions);
            case SCHEDULED_EXECUTOR:
                return new ScheduledExecutorPermission(permissionConfig.getName(), actions);
            case PN_COUNTER:
                return new PNCounterPermission(permissionConfig.getName(), actions);
            case CACHE:
                return new CachePermission(permissionConfig.getName(), actions);
            case USER_CODE_DEPLOYMENT:
                return new UserCodeDeploymentPermission(actions);
            case CONFIG:
                return new ConfigPermission();
            case RING_BUFFER:
                return new RingBufferPermission(permissionConfig.getName(), actions);
            case RELIABLE_TOPIC:
                return new ReliableTopicPermission(permissionConfig.getName(), actions);
            case REPLICATEDMAP:
                return new ReplicatedMapPermission(permissionConfig.getName(), actions);
            default:
                throw new IllegalArgumentException(permissionConfig.getType().toString());
        }
    }

    static void setSecureCall() {
        if (isSecureCall()) {
            throw new SecurityException("Not allowed! <SECURE_CALL> flag is already set!");
        }
        SECURE_CALL.set(Boolean.TRUE);
    }

    static void resetSecureCall() {
        if (!isSecureCall()) {
            throw new SecurityException("Not allowed! <SECURE_CALL> flag is not set!");
        }
        SECURE_CALL.remove();
    }

    private static boolean isSecureCall() {
        final Boolean value = SECURE_CALL.get();
        return value != null && value;
    }

    public static boolean addressMatches(String address, String pattern) {
        return AddressUtil.matchInterface(address, pattern);
    }

    /**
     * Runs JAAS authentication ({@link LoginContext#login()}) on {@link RealmConfig} with given name retrieved by using given
     * {@link CallbackHandler}. Return either the authenticated {@link Subject} when the authentication passes or {@code null}.
     *
     * @param callbackHandler handler used to retrieve the {@link RealmConfig}
     * @param securityRealm name of a security realm to be retrieved by callbackHandler
     * @return {@link Subject} when the authentication passes, {@code null} otherwise
     */
    public static Subject getRunAsSubject(CallbackHandler callbackHandler, String securityRealm) {
        if (securityRealm == null) {
            if (LOGGER.isFineEnabled()) {
                LOGGER.fine("No RunAs Subject created for callbackHandler=" + callbackHandler + ", realm is not provided");
            }
            return null;
        }
        RealmConfigCallback cb = new RealmConfigCallback(securityRealm);
        try {
            callbackHandler.handle(new Callback[] { cb });
        } catch (IOException | UnsupportedCallbackException e) {
            LOGGER.info("Unable to retrieve the RealmConfig", e);
            return null;
        }
        return getRunAsSubject(callbackHandler, cb.getRealmConfig());
    }

    /**
     * Runs JAAS authentication ({@link LoginContext#login()}) on given {@link RealmConfig}.
     * Return either the authenticated {@link Subject} when the authentication passes or {@code null}.
     *
     * @param callbackHandler handler used to retrieve the {@link RealmConfig}
     * @return {@link Subject} when the authentication passes, {@code null} otherwise
     */
    public static Subject getRunAsSubject(CallbackHandler callbackHandler, RealmConfig realmConfig) {
        if (realmConfig == null) {
            if (LOGGER.isFineEnabled()) {
                LOGGER.fine("The realmConfig is not provided.");
            }
            return null;
        }
        LoginConfigurationDelegate loginConfiguration = new LoginConfigurationDelegate(realmConfig.asLoginModuleConfigs());
        try {
            LoginContext lc = new LoginContext(TEMP_LOGIN_CONTEXT_NAME, new Subject(), callbackHandler, loginConfiguration);
            lc.login();
            return lc.getSubject();
        } catch (LoginException e) {
            LOGGER.info("Authentication failed.", e);
            return null;
        }
    }

    public static RealmConfig createKerberosJaasRealmConfig(String principal, String keytabPath, boolean isInitiator) {
        if (keytabPath == null) {
            if (LOGGER.isFineEnabled()) {
                LOGGER.fine("The keytab path is not provided.");
            }
            return null;
        }
        LoginModuleConfig krb5LoginModuleConfig;
        if (isIbmJvm()) {
            krb5LoginModuleConfig = new LoginModuleConfig(FQCN_KRB5LOGINMODULE_IBM, LoginModuleUsage.REQUIRED)
                    .setProperty("useKeytab", new File(keytabPath).toURI().toString())
                    .setProperty("credsType", isInitiator ? "both" : "acceptor");
        } else {
            krb5LoginModuleConfig = new LoginModuleConfig(FQCN_KRB5LOGINMODULE_SUN, LoginModuleUsage.REQUIRED)
                    .setOrClear("keyTab", keytabPath).setProperty("doNotPrompt", "true")
                    .setProperty("useKeyTab", "true")
                    .setProperty("storeKey", "true")
                    .setProperty("isInitiator", Boolean.toString(isInitiator));
        }
        krb5LoginModuleConfig
            .setOrClear("principal", principal)
            .setProperty("refreshKrb5Config", "true");
        RealmConfig kerberosRealmConfig = new RealmConfig()
                .setJaasAuthenticationConfig(new JaasAuthenticationConfig().addLoginModuleConfig(krb5LoginModuleConfig));
        if (LOGGER.isFineEnabled()) {
            LOGGER.fine(
                    "A helper security realm for Kerberos keytab-based authentication was generated: " + kerberosRealmConfig);
        }
        return kerberosRealmConfig;
    }

    public static String generateRealmConfigXml(RealmConfig realmConfig, String realmName) {
        StringBuilder xmlBuilder = new StringBuilder();
        XmlGenerator gen = new XmlGenerator(xmlBuilder);
        SecurityXmlGenerator cfgGen = new SecurityXmlGenerator();
        cfgGen.securityRealmGenerator(gen, realmName, realmConfig);
        return formatXml(xmlBuilder.toString(), 2);
    }

    private static boolean isIbmJvm() {
        String vendor = System.getProperty("java.vendor");
        return vendor != null && lowerCaseInternal(vendor).startsWith("ibm");
    }

    private static class SecurityXmlGenerator extends ConfigXmlGenerator {

        @Override
        protected void securityRealmGenerator(XmlGenerator gen, String name, RealmConfig c) {
            super.securityRealmGenerator(gen, name, c);
        }
    }
}
