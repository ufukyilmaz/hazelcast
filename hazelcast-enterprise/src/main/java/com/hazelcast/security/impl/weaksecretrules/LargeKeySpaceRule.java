package com.hazelcast.security.impl.weaksecretrules;

import com.hazelcast.security.impl.SecretStrengthRule;
import com.hazelcast.security.impl.WeakSecretError;

import java.util.EnumSet;
import java.util.regex.Pattern;

import static com.hazelcast.security.impl.WeakSecretError.NO_ALPHA;
import static com.hazelcast.security.impl.WeakSecretError.NO_MIXED_CASE;
import static com.hazelcast.security.impl.WeakSecretError.NO_NUMERAL;
import static com.hazelcast.security.impl.WeakSecretError.NO_SPECIAL_CHARS;

public class LargeKeySpaceRule
        implements SecretStrengthRule {

    private static final Pattern SPECIAL_CHARACTER_PATTERN = Pattern.compile(".*?[^a-zA-Z0-9 ].*");

    @Override
    public EnumSet<WeakSecretError> check(CharSequence secret) {

        boolean hasAlpha = false;
        boolean hasNumeral = false;
        boolean hasLowerCase = false;
        boolean hasUpperCase = false;
        boolean hasSpecialChars = SPECIAL_CHARACTER_PATTERN.matcher(secret).matches();

        EnumSet<WeakSecretError> weaknesses = EnumSet.noneOf(WeakSecretError.class);

        for (int i = 0; i < secret.length(); i++) {
            hasLowerCase |= Character.isLowerCase(secret.charAt(i));
            hasUpperCase |= Character.isUpperCase(secret.charAt(i));
            // We can do better unicode support, with Character.isAlphabetic available in >= 1.7

            hasAlpha |= Character.isLetter(secret.charAt(i));
            hasNumeral |= Character.isDigit(secret.charAt(i));
        }

        if (!hasAlpha) {
            weaknesses.add(NO_ALPHA);
        }

        if (!hasNumeral) {
            weaknesses.add(NO_NUMERAL);
        }

        if (!hasSpecialChars) {
            weaknesses.add(NO_SPECIAL_CHARS);
        }

        if (!(hasLowerCase & hasUpperCase)) {
            weaknesses.add(NO_MIXED_CASE);
        }

        return weaknesses;
    }

}
