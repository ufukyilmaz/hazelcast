package com.hazelcast.security.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.EncryptionAtRestConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.JavaKeyStoreSecureStoreConfig;
import com.hazelcast.config.SecureStoreConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.config.security.RealmConfig;
import com.hazelcast.config.security.UsernamePasswordIdentityConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecretStrengthPolicy;
import com.hazelcast.security.WeakSecretException;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.internal.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.internal.util.ExceptionUtil.rethrow;
import static com.hazelcast.security.WeakSecretException.ENFORCED;
import static com.hazelcast.security.WeakSecretException.formatMessage;
import static com.hazelcast.security.impl.SecurityConstants.DEFAULT_SECRET_STRENGTH_POLICY_CLASS;
import static com.hazelcast.security.impl.SecurityConstants.SECRET_STRENGTH_POLICY_CLASS;
import static java.lang.System.getProperty;

public class WeakSecretsConfigChecker {

    private static final String LINE_SEP = getProperty("line.separator");

    private final Config config;

    private final SecretStrengthPolicy policy;

    public WeakSecretsConfigChecker(Config config) {
        this.config = config;

        SecretStrengthPolicy policy = null;
        try {
            policy = newInstance(WeakSecretsConfigChecker.class.getClassLoader(),
                    getProperty(SECRET_STRENGTH_POLICY_CLASS, DEFAULT_SECRET_STRENGTH_POLICY_CLASS));
        } catch (Exception e) {
            rethrow(e);
        }

        this.policy = policy;
    }

    public void evaluateAndReport(ILogger logger) {
        Map<String, EnumSet<WeakSecretError>> report = evaluate();
        if (!report.isEmpty()) {
            logger.warning(constructBanner(report));

            if (ENFORCED) {
                throw new WeakSecretException("Weak secrets found in configuration, check output above for more details.");
            }
        }
    }

    public Map<String, EnumSet<WeakSecretError>> evaluate() {
        Map<String, EnumSet<WeakSecretError>> result = new HashMap<>();

        SecurityConfig securityConfig = config.getSecurityConfig();
        if (securityConfig != null && securityConfig.isEnabled()) {
            checkRealmsPasswords(result, securityConfig.getRealmConfigs());
        }

        SymmetricEncryptionConfig sec = ConfigAccessor.getActiveMemberNetworkConfig(config).getSymmetricEncryptionConfig();
        if (sec != null && sec.isEnabled()) {
            EnumSet<WeakSecretError> symEncPwdWeaknesses = getWeaknesses(sec.getPassword());
            if (!symEncPwdWeaknesses.isEmpty()) {
                result.put("Symmetric Encryption Password", symEncPwdWeaknesses);
            }

            EnumSet<WeakSecretError> symEncSaltWeaknesses = getWeaknesses(sec.getSalt());
            if (!symEncSaltWeaknesses.isEmpty()) {
                result.put("Symmetric Encryption Salt", symEncSaltWeaknesses);
            }
        }

        HotRestartPersistenceConfig hotRestartPersistenceConfig = config.getHotRestartPersistenceConfig();
        if (hotRestartPersistenceConfig != null && hotRestartPersistenceConfig.isEnabled()) {
            Map<String, String> hotRestartSecrets = getHotRestartSecrets(hotRestartPersistenceConfig);
            for (Map.Entry<String, String> entry : hotRestartSecrets.entrySet()) {
                EnumSet<WeakSecretError> weaknesses = getWeaknesses(entry.getValue());
                if (!weaknesses.isEmpty()) {
                    result.put(entry.getKey(), weaknesses);
                }
            }
        }

        return result;
    }

    @SuppressWarnings("checkstyle:nestedifdepth")
    private static Map<String, String> getHotRestartSecrets(HotRestartPersistenceConfig hotRestartPersistenceConfig) {
        Map<String, String> secrets = new HashMap<>();
        if (hotRestartPersistenceConfig.isEnabled()) {
            EncryptionAtRestConfig encryptionAtRestConfig = hotRestartPersistenceConfig.getEncryptionAtRestConfig();
            if (encryptionAtRestConfig.isEnabled()) {
                SecureStoreConfig secureStoreConfig = encryptionAtRestConfig.getSecureStoreConfig();
                if (secureStoreConfig instanceof JavaKeyStoreSecureStoreConfig) {
                    JavaKeyStoreSecureStoreConfig javaKeyStoreSecureStoreConfig =
                            (JavaKeyStoreSecureStoreConfig) secureStoreConfig;
                    String javaKeyStorePassword = javaKeyStoreSecureStoreConfig.getPassword();
                    if (javaKeyStorePassword != null) {
                        secrets.put("Hot Restart Encryption Java KeyStore password", javaKeyStorePassword);
                    }
                }
            }
        }
        return secrets;
    }

    private void checkRealmsPasswords(Map<String, EnumSet<WeakSecretError>> result, Map<String, RealmConfig> realms) {
        if (realms == null) {
            return;
        }
        for (Map.Entry<String, RealmConfig> entry : realms.entrySet()) {
            UsernamePasswordIdentityConfig identityCfg = entry.getValue().getUsernamePasswordIdentityConfig();
            if (identityCfg != null) {
                EnumSet<WeakSecretError> pwdWeaknesses = getWeaknesses(identityCfg.getPassword());
                if (pwdWeaknesses.isEmpty()) {
                    result.put("Identity password in Security realm " + entry.getKey(), pwdWeaknesses);
                }
            }
        }
    }

    private EnumSet<WeakSecretError> getWeaknesses(String secret) {
        try {
            policy.validate(null, secret);
        } catch (WeakSecretException ex) {
            return ex.getWeaknesses();
        }

        return EnumSet.noneOf(WeakSecretError.class);
    }

    private String constructBanner(Map<String, EnumSet<WeakSecretError>> report) {
        StringBuilder banner = new StringBuilder();

        banner.append(LINE_SEP)
              .append("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@ SECURITY WARNING @@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@")
              .append(LINE_SEP);

        for (Map.Entry<String, EnumSet<WeakSecretError>> weakSecret : report.entrySet()) {
            banner.append(formatMessage(weakSecret.getKey(), weakSecret.getValue()))
                  .append(LINE_SEP)
                  .append(LINE_SEP);
        }

        banner.append("@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@");

        return banner.toString();
    }
}
