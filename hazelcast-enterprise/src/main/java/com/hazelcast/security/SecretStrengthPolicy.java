package com.hazelcast.security;

/**
 * Specification of a secret strength policy validator.
 * Used to validate strength of secrets (passwords, keys, salts) in the configuration, and in case a weak one is identified
 * it throws a {@link WeakSecretException}
 */
public interface SecretStrengthPolicy {

    /**
     * Validate a secret.
     * Normal return is expected if the secret is secure as defined by the implementation of this spec.
     * Exception thrown {@link WeakSecretException} otherwise.
     *
     * @param label The secret label, (eg. Group password) used to construct a human friendly message for the exception
     * @param secret The actual secret value, the one to evaluate.
     * @throws WeakSecretException
     */
    void validate(String label, CharSequence secret) throws WeakSecretException;

}
