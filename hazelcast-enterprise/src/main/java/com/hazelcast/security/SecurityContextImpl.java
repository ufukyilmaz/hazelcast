package com.hazelcast.security;

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.util.ExceptionUtil;

import javax.security.auth.Subject;
import javax.security.auth.login.Configuration;
import javax.security.auth.login.LoginContext;
import javax.security.auth.login.LoginException;
import java.security.AccessControlException;
import java.security.Permission;
import java.security.PermissionCollection;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.logging.Level;

public class SecurityContextImpl implements SecurityContext {

    private static ThreadLocal<ParametersImpl> threadLocal = new ThreadLocal<ParametersImpl>();

    private final ILogger logger;
    private final Node node;
    private final IPermissionPolicy policy;
    private final ICredentialsFactory credentialsFactory;
    private final Configuration memberConfiguration;
    private final Configuration clientConfiguration;
    private final List<SecurityInterceptor> interceptors;
    private final Parameters emptyParameters = new EmptyParametersImpl();

    public SecurityContextImpl(Node node) {
        super();
        this.node = node;
        logger = node.getLogger("com.hazelcast.enterprise.security");
        logger.log(Level.INFO, "Initializing Hazelcast Enterprise security context.");

        SecurityConfig securityConfig = node.config.getSecurityConfig();

        PermissionPolicyConfig policyConfig = securityConfig.getClientPolicyConfig();
        if (policyConfig.getClassName() == null) {
            policyConfig.setClassName(SecurityConstants.DEFAULT_POLICY_CLASS);
        }
        IPermissionPolicy tmpPolicy = policyConfig.getImplementation();
        if (tmpPolicy == null) {
            tmpPolicy = (IPermissionPolicy) createImplInstance(node.getConfigClassLoader(), policyConfig.getClassName());
        }
        policy = tmpPolicy;
        policy.configure(node.config, policyConfig.getProperties());

        CredentialsFactoryConfig credentialsFactoryConfig = securityConfig.getMemberCredentialsConfig();
        if (credentialsFactoryConfig.getClassName() == null) {
            credentialsFactoryConfig.setClassName(SecurityConstants.DEFAULT_CREDENTIALS_FACTORY_CLASS);
        }
        ICredentialsFactory tmpCredentialsFactory = credentialsFactoryConfig.getImplementation();
        if (tmpCredentialsFactory == null) {
            tmpCredentialsFactory = (ICredentialsFactory) createImplInstance(node.getConfigClassLoader(), credentialsFactoryConfig.getClassName());
        }
        credentialsFactory = tmpCredentialsFactory;
        credentialsFactory.configure(node.config.getGroupConfig(), credentialsFactoryConfig.getProperties());

        memberConfiguration = new LoginConfigurationDelegate(node.config, getLoginModuleConfigs(securityConfig.getMemberLoginModuleConfigs()));
        clientConfiguration = new LoginConfigurationDelegate(node.config, getLoginModuleConfigs(securityConfig.getClientLoginModuleConfigs()));
        final List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();
        interceptors = new ArrayList<SecurityInterceptor>(interceptorConfigs.size());
        for (SecurityInterceptorConfig interceptorConfig : interceptorConfigs) {
            addInterceptors(interceptorConfig);
        }
    }

    void addInterceptors(SecurityInterceptorConfig interceptorConfig) {
        SecurityInterceptor interceptor = interceptorConfig.getImplementation();
        final String className = interceptorConfig.getClassName();
        if (interceptor == null && className != null) {
            try {
                interceptor = ClassLoaderUtil.newInstance(node.getConfigClassLoader(), className);
            } catch (Exception e) {
                throw ExceptionUtil.rethrow(e);
            }
        }
        if (interceptor != null) {
            interceptors.add(interceptor);
        }
    }

    Parameters getArguments(Object[] args) {
        if (args == null) {
            return emptyParameters;
        }
        ParametersImpl arguments = threadLocal.get();
        if (arguments == null) {
            arguments = new ParametersImpl(node.getSerializationService());
            threadLocal.set(arguments);
        }
        arguments.setArgs(args);
        return arguments;
    }

    Parameters getArguments() {
        return threadLocal.get();
    }

    @Override
    public void interceptBefore(Credentials credentials, String objectType, String objectName, String methodName,
                                Object[] args) throws AccessControlException {
        if (interceptors.isEmpty()) {
            return;
        }
        final Parameters parameters = getArguments(args);
        for (SecurityInterceptor interceptor : interceptors) {
            try {
                interceptor.before(credentials, objectType, objectName, methodName, parameters);
            } catch (Throwable t) {
                throw new AccessControlException(t.getMessage());
            }
        }
    }

    @Override
    public void interceptAfter(Credentials credentials, String objectType, String objectName, String methodName) {
        if (interceptors.isEmpty()) {
            return;
        }
        final Parameters parameters = getArguments();
        for (SecurityInterceptor interceptor : interceptors) {
            try {
                interceptor.after(credentials, objectType, objectName, methodName, parameters);
            } catch (Throwable t) {
                logger.warning("Exception during interceptor.after " + interceptor);
            }
        }
    }

    public LoginContext createMemberLoginContext(Credentials credentials) throws LoginException {
        logger.log(Level.FINEST, "Creating Member LoginContext for: " + SecurityUtil.getCredentialsFullName(credentials));
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            thread.setContextClassLoader(SecurityContextImpl.class.getClassLoader());
            String name = node.getConfig().getGroupConfig().getName();
            ClusterCallbackHandler callbackHandler = new ClusterCallbackHandler(credentials);
            return new LoginContext(name, new Subject(), callbackHandler, memberConfiguration);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    public LoginContext createClientLoginContext(Credentials credentials) throws LoginException {
        logger.log(Level.FINEST, "Creating Client LoginContext for: " + SecurityUtil.getCredentialsFullName(credentials));
        Thread thread = Thread.currentThread();
        ClassLoader tccl = thread.getContextClassLoader();
        try {
            String name = node.getConfig().getGroupConfig().getName();
            ClusterCallbackHandler callbackHandler = new ClusterCallbackHandler(credentials);
            return new LoginContext(name, new Subject(), callbackHandler, clientConfiguration);
        } finally {
            thread.setContextClassLoader(tccl);
        }
    }

    private Object createImplInstance(ClassLoader cl, final String className) {
        try {
            return ClassLoaderUtil.newInstance(cl, className);
        } catch (Exception e) {
            throw new IllegalArgumentException("Could not create instance of '" + className
                    + "', cause: " + e.getMessage(), e);
        }
    }

    private LoginModuleConfig[] getLoginModuleConfigs(final List<LoginModuleConfig> modules) {
        if (modules.isEmpty()) {
            modules.add(getDefaultLoginModuleConfig());
        }
        return modules.toArray(new LoginModuleConfig[modules.size()]);
    }

    private LoginModuleConfig getDefaultLoginModuleConfig() {
        final GroupConfig groupConfig = node.config.getGroupConfig();
        final LoginModuleConfig module = new LoginModuleConfig(SecurityConstants.DEFAULT_LOGIN_MODULE,
                LoginModuleUsage.REQUIRED);
        module.getProperties().setProperty(SecurityConstants.ATTRIBUTE_CONFIG_GROUP, groupConfig.getName());
        module.getProperties().setProperty(SecurityConstants.ATTRIBUTE_CONFIG_PASS, groupConfig.getPassword());
        return module;
    }

    @Override
    public ICredentialsFactory getCredentialsFactory() {
        return credentialsFactory;
    }

    @Override
    public void checkPermission(Subject subject, Permission permission) throws SecurityException {
        PermissionCollection coll = policy.getPermissions(subject, permission.getClass());
        final boolean b = coll != null && coll.implies(permission);
        if (!b) {
            throw new AccessControlException("Permission " + permission + " denied!", permission);
        }
    }

    @Override
    public <V> SecureCallable<V> createSecureCallable(Subject subject, Callable<V> callable) {
        return new SecureCallableImpl<V>(subject, callable);
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
    }

    @Override
    public void destroy() {
        logger.log(Level.INFO, "Destroying Hazelcast Enterprise security context.");
        policy.destroy();
        credentialsFactory.destroy();
    }
}
