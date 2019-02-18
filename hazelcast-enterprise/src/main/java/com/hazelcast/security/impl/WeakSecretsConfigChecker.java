package com.hazelcast.security.impl;

import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigAccessor;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.SymmetricEncryptionConfig;
import com.hazelcast.logging.ILogger;
import com.hazelcast.security.SecretStrengthPolicy;
import com.hazelcast.security.WeakSecretException;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.nio.ClassLoaderUtil.newInstance;
import static com.hazelcast.security.SecurityConstants.DEFAULT_SECRET_STRENGTH_POLICY_CLASS;
import static com.hazelcast.security.SecurityConstants.SECRET_STRENGTH_POLICY_CLASS;
import static com.hazelcast.security.WeakSecretException.ENFORCED;
import static com.hazelcast.security.WeakSecretException.formatMessage;
import static com.hazelcast.util.ExceptionUtil.rethrow;
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
        Map<String, EnumSet<WeakSecretError>> result = new HashMap<String, EnumSet<WeakSecretError>>();

        GroupConfig gc = config.getGroupConfig();
        EnumSet<WeakSecretError> groupPwdWeaknesses = getWeaknesses(gc.getPassword());
        if (!groupPwdWeaknesses.isEmpty()) {
            result.put("Group Password", groupPwdWeaknesses);
        }

        SymmetricEncryptionConfig sec = ConfigAccessor.getActiveMemberNetworkConfig(config).getSymmetricEncryptionConfig();
        if (sec != null) {
            EnumSet<WeakSecretError> symEncPwdWeaknesses = getWeaknesses(sec.getPassword());
            if (!symEncPwdWeaknesses.isEmpty()) {
                result.put("Symmetric Encryption Password", getWeaknesses(sec.getPassword()));
            }

            EnumSet<WeakSecretError> symEncSaltWeaknesses = getWeaknesses(sec.getSalt());
            if (!symEncSaltWeaknesses.isEmpty()) {
                result.put("Symmetric Encryption Salt", getWeaknesses(sec.getSalt()));
            }
        }

        return result;
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
