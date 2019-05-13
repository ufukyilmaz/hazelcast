package com.hazelcast.security;

import com.hazelcast.config.CredentialsFactoryConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.LoginModuleConfig;
import com.hazelcast.config.LoginModuleConfig.LoginModuleUsage;
import com.hazelcast.config.PermissionConfig;
import com.hazelcast.config.PermissionPolicyConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SecurityInterceptorConfig;
import com.hazelcast.instance.Node;
import com.hazelcast.logging.ILogger;
import com.hazelcast.nio.ClassLoaderUtil;
import com.hazelcast.security.impl.SecurityServiceImpl;
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
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;

import static com.hazelcast.security.impl.SecurityServiceImpl.clonePermissionConfigs;

public class SecurityContextImpl implements SecurityContext {

    private static final ThreadLocal<ParametersImpl> THREAD_LOCAL_PARAMETERS = new ThreadLocal<>();

    private final ILogger logger;
    private final Node node;
    private final IPermissionPolicy policy;
    private final ICredentialsFactory credentialsFactory;
    private final Configuration memberConfiguration;
    private final Configuration clientConfiguration;
    private final List<SecurityInterceptor> interceptors;
    private final Parameters emptyParameters = new EmptyParametersImpl();
    private final AtomicBoolean refreshPermissionsInProgress = new AtomicBoolean();

    @SuppressWarnings("checkstyle:executablestatementcount")
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

        CredentialsFactoryConfig credsCfg = securityConfig.getMemberCredentialsConfig();
        if (credsCfg.getClassName() == null) {
            credsCfg.setClassName(SecurityConstants.DEFAULT_CREDENTIALS_FACTORY_CLASS);
        }
        ICredentialsFactory credsFact = credsCfg.getImplementation();
        if (credsFact == null) {
            credsFact = (ICredentialsFactory) createImplInstance(node.getConfigClassLoader(), credsCfg.getClassName());
        }
        credentialsFactory = credsFact;
        credentialsFactory.configure(node.config.getGroupConfig(), credsCfg.getProperties());

        memberConfiguration = new LoginConfigurationDelegate(node.config,
                getLoginModuleConfigs(securityConfig.getMemberLoginModuleConfigs()));
        clientConfiguration = new LoginConfigurationDelegate(node.config,
                getLoginModuleConfigs(securityConfig.getClientLoginModuleConfigs()));
        final List<SecurityInterceptorConfig> interceptorConfigs = securityConfig.getSecurityInterceptorConfigs();
        interceptors = new ArrayList<>(interceptorConfigs.size());
        for (SecurityInterceptorConfig interceptorConfig : interceptorConfigs) {
            addInterceptors(interceptorConfig);
        }
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

    @Override
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

    @Override
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
        return new SecureCallableImpl<>(subject, callable);
    }

    @Override
    public <V> SecureCallable<?> createSecureCallable(Subject subject, Runnable runnable) {
        return new SecureCallableImpl<V>(subject, runnable);
    }

    @Override
    public void destroy() {
        logger.log(Level.INFO, "Destroying Hazelcast Enterprise security context.");
        policy.destroy();
        credentialsFactory.destroy();
    }

    @Override
    public void refreshPermissions(Set<PermissionConfig> permissionConfigs) {
        if (refreshPermissionsInProgress.compareAndSet(false, true)) {
            policy.refreshPermissions(clonePermissionConfigs(permissionConfigs));

            SecurityServiceImpl securityService = (SecurityServiceImpl) node.getSecurityService();
            securityService.setPermissionConfigs(permissionConfigs);
            refreshPermissionsInProgress.set(false);
        } else {
            throw new IllegalStateException("Permissions could not be refreshed, another update is in progress.");
        }
    }

    public ILogger getLogger(String name) {
        return node.getLogger(name);
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
        ParametersImpl params = THREAD_LOCAL_PARAMETERS.get();
        if (params == null) {
            params = new ParametersImpl(node.getSerializationService());
            THREAD_LOCAL_PARAMETERS.set(params);
        }
        params.setArgs(args);
        return params;
    }

    static Parameters getArguments() {
        return THREAD_LOCAL_PARAMETERS.get();
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
}
