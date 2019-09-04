package com.hazelcast.security.impl.weaksecretrules;

import com.hazelcast.security.impl.SecretStrengthRule;
import com.hazelcast.security.impl.WeakSecretError;

import java.util.EnumSet;

import static com.hazelcast.config.SymmetricEncryptionConfig.DEFAULT_SYMMETRIC_PASSWORD;
import static com.hazelcast.config.SymmetricEncryptionConfig.DEFAULT_SYMMETRIC_SALT;
import static com.hazelcast.security.impl.WeakSecretError.DEFAULT;

public class DefaultConfigValuesRule
        implements SecretStrengthRule {

    private static final String[] DEFAULTS = new String[] {
            DEFAULT_SYMMETRIC_PASSWORD,
            DEFAULT_SYMMETRIC_SALT,
    };

    @Override
    public EnumSet<WeakSecretError> check(CharSequence secret) {
        for (String befault : DEFAULTS) {
            if (secret.equals(befault)) {
                return EnumSet.of(DEFAULT);
            }
        }

        return EnumSet.noneOf(WeakSecretError.class);
    }

}
