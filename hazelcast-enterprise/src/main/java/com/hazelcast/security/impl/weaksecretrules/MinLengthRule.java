package com.hazelcast.security.impl.weaksecretrules;

import com.hazelcast.security.impl.SecurityConstants;
import com.hazelcast.security.impl.SecretStrengthRule;
import com.hazelcast.security.impl.WeakSecretError;

import java.util.EnumSet;

import static com.hazelcast.security.impl.WeakSecretError.MIN_LEN;
import static java.lang.Integer.parseInt;
import static java.lang.System.getProperty;

public class MinLengthRule
        implements SecretStrengthRule {

    public static final int MIN_ALLOWED_SECRET_LENGTH =
            parseInt(getProperty(SecurityConstants.MIN_ALLOWED_SECRET_LENGTH, "8"));

    @Override
    public EnumSet<WeakSecretError> check(CharSequence secret) {
        return secret == null || secret.length() < MIN_ALLOWED_SECRET_LENGTH
                ? EnumSet.of(MIN_LEN)
                : EnumSet.noneOf(WeakSecretError.class);
    }

}
