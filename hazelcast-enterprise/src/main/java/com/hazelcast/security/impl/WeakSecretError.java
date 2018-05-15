package com.hazelcast.security.impl;

/**
 * Weak secret classification. A secret word (eg. password, salt etc) can be classified as
 * either Strong, based on the enforced policy, or have one or more of the following weaknesses.
 */
public enum WeakSecretError {

    /**
     * Secret is the Hazelcast default
     */
    DEFAULT,

    /**
     * Secret doesn't meet the minimum length requirements
     */
    MIN_LEN,

    /**
     * Secret is not using mixed case characters
     */
    NO_MIXED_CASE,

    /**
     * Secret is not using alphabetic characters
     */
    NO_ALPHA,

    /**
     * Secret is not using numerals
     */
    NO_NUMERAL,

    /**
     * Secret is not using special character
     */
    NO_SPECIAL_CHARS,

    /**
     * Secret is a dictionary word
     */
    DICT_WORD
}
