package com.hazelcast.security;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.security.impl.WeakSecretError;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

import static com.hazelcast.security.impl.SecurityConstants.SECRET_STRENGTH_POLICY_ENFORCED;
import static com.hazelcast.security.impl.WeakSecretError.DEFAULT;
import static com.hazelcast.security.impl.WeakSecretError.DICT_WORD;
import static com.hazelcast.security.impl.WeakSecretError.MIN_LEN;
import static com.hazelcast.security.impl.WeakSecretError.NO_ALPHA;
import static com.hazelcast.security.impl.WeakSecretError.NO_MIXED_CASE;
import static com.hazelcast.security.impl.WeakSecretError.NO_NUMERAL;
import static com.hazelcast.security.impl.WeakSecretError.NO_SPECIAL_CHARS;
import static com.hazelcast.security.impl.weaksecretrules.MinLengthRule.MIN_ALLOWED_SECRET_LENGTH;
import static java.lang.Boolean.parseBoolean;
import static java.lang.System.getProperty;

/**
 * Exception thrown when a weak secret is detected.
 * Contains the collection of weakness types, and a beautified human readable message.
 */
public final class WeakSecretException extends HazelcastException {

    public static final boolean ENFORCED = parseBoolean(getProperty(SECRET_STRENGTH_POLICY_ENFORCED, "false"));

    private static final String LINE_SEP = getProperty("line.separator");

    private static final Map<WeakSecretError, String> WEAKNESS_DESCRIPTIONS = new HashMap<WeakSecretError, String>();

    static {
        WEAKNESS_DESCRIPTIONS.put(DEFAULT, "*Must not be set to the default.");
        WEAKNESS_DESCRIPTIONS.put(MIN_LEN, "*Must contain " + MIN_ALLOWED_SECRET_LENGTH + " or more characters.");
        WEAKNESS_DESCRIPTIONS.put(DICT_WORD, "*Must not be a dictionary word.");
        WEAKNESS_DESCRIPTIONS.put(NO_ALPHA, "*Must have at least 1 alphabetic character.");
        WEAKNESS_DESCRIPTIONS.put(NO_MIXED_CASE, "*Must have at least 1 lower and 1 upper case characters.");
        WEAKNESS_DESCRIPTIONS.put(NO_NUMERAL, "*Must contain at least 1 number.");
        WEAKNESS_DESCRIPTIONS.put(NO_SPECIAL_CHARS, "*Must contain at least 1 special character.");
    }

    private final EnumSet<WeakSecretError> weaknesses;

    private WeakSecretException(EnumSet<WeakSecretError> weaknesses, String details) {
        super(details);
        this.weaknesses = weaknesses;
    }

    public WeakSecretException(String message) {
        super(message);
        this.weaknesses = null;
    }

    public EnumSet<WeakSecretError> getWeaknesses() {
        return weaknesses;
    }

    public static WeakSecretException of(String label, EnumSet<WeakSecretError> weaknesses) {
        return new WeakSecretException(weaknesses, formatMessage(label, weaknesses));
    }

    public static String formatMessage(String label, EnumSet<WeakSecretError> weaknesses) {
        StringBuilder details = new StringBuilder();

        details.append(label).append(" does not meet the current policy and complexity requirements. ");
        details.append(LINE_SEP);

        int count = 0;
        for (WeakSecretError weakness : weaknesses) {
            details.append(WEAKNESS_DESCRIPTIONS.get(weakness));
            if (++count < weaknesses.size()) {
                details.append(LINE_SEP);
            }
        }
        return details.toString();
    }
}
