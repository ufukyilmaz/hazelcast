package com.hazelcast.security.impl;

public final class SecurityConstants {

    public static final String DEFAULT_LOGIN_MODULE = "com.hazelcast.security.impl.DefaultLoginModule";
    public static final String DEFAULT_POLICY_CLASS = "com.hazelcast.security.impl.DefaultPermissionPolicy";
    public static final String DEFAULT_CREDENTIALS_FACTORY_CLASS = "com.hazelcast.security.impl.DefaultCredentialsFactory";

    public static final String SECRET_STRENGTH_POLICY_ENFORCED = "hazelcast.security.secret.strength.policy.enforced";
    public static final String DEFAULT_SECRET_STRENGTH_POLICY_CLASS = "com.hazelcast.security.impl.DefaultSecretStrengthPolicy";
    public static final String MIN_ALLOWED_SECRET_LENGTH = "hazelcast.security.secret.policy.min.length";
    public static final String SECRET_STRENGTH_POLICY_CLASS = "hazelcast.security.secret.strength.policy.class";

    private SecurityConstants() {
    }
}
