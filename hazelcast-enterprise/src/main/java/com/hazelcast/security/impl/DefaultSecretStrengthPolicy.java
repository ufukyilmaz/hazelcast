package com.hazelcast.security.impl;

import com.hazelcast.security.SecretStrengthPolicy;
import com.hazelcast.security.WeakSecretException;
import com.hazelcast.security.impl.weaksecretrules.DefaultConfigValuesRule;
import com.hazelcast.security.impl.weaksecretrules.DictionaryRule;
import com.hazelcast.security.impl.weaksecretrules.LargeKeySpaceRule;
import com.hazelcast.security.impl.weaksecretrules.MinLengthRule;

import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;

/**
 *  This class is not unused, it's set via {@link SecurityConstants#SECRET_STRENGTH_POLICY_CLASS}.
 */
public class DefaultSecretStrengthPolicy
        implements SecretStrengthPolicy {

    private static final Collection<SecretStrengthRule> RULES =
            Arrays.asList(
                    new DefaultConfigValuesRule(),
                    new MinLengthRule(),
                    new DictionaryRule(),
                    new LargeKeySpaceRule()
                );

    @Override
    public void validate(String label, CharSequence secret)
            throws WeakSecretException {

        EnumSet<WeakSecretError> weaknesses = EnumSet.noneOf(WeakSecretError.class);
        for (SecretStrengthRule rule : RULES) {
            weaknesses.addAll(rule.check(secret));
        }

        if (!weaknesses.isEmpty()) {
            throw WeakSecretException.of(label, weaknesses);
        }
    }
}
