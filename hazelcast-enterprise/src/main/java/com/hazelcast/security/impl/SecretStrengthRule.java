package com.hazelcast.security.impl;

import java.util.EnumSet;

/**
 * Specification of the required implementation for a secret strength principle
 */
public interface SecretStrengthRule {

    EnumSet<WeakSecretError> check(CharSequence secret);

}
