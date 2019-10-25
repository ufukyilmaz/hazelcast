package com.hazelcast.security.impl;

import static com.hazelcast.security.impl.LdapUtils.getAttributeValue;
import static com.hazelcast.security.impl.LdapUtils.replacePlaceholders;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;

import java.util.Properties;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.security.auth.login.FailedLoginException;

import com.hazelcast.config.security.LdapSearchScope;


/**
 * JAAS Login module which uses LDAP protocol to verify credentials and load roles. Compared to
 * the {@link BasicLdapLoginModule}, this module doesn't expect full user DN to be provided as a login name. This module allows
 * to verify provided user credentials by doing a new LDAP bind similarly to the {@link BasicLdapLoginModule}, but it also allow
 * to compare provided password against a value defined in passwordAttribute module option. This login module expects an LDAP
 * account to be pre-configured. This account is used for searching user and roles objects. Account configuration is done by
 * using well-known {@link InitialLdapContext} environment variables as login module options:
 * <ul>
 * <li>java.naming.security.authentication</li>
 * <li>java.naming.security.principal</li>
 * <li>java.naming.security.credentials</li>
 * <li>...</li>
 * </ul>
 */
public class LdapLoginModule extends BasicLdapLoginModule {

    /**
     * Placeholder string to be replaced by provided login name in the {@value #OPTION_USER_FILTER} option.
     */
    public static final String PLACEHOLDER_LOGIN = "{login}";

    /**
     * Login module option name - LDAP Context in which user objects are searched. (E.g. ou=Users,dc=hazelcast,dc=com)
     */
    public static final String OPTION_USER_CONTEXT = "userContext";

    /**
     * Login module option name - LDAP search string for retrieving user objects based on provided login name. It usually
     * contains placeholder substring "{login}" which is replaced by the provided login name.
     */
    public static final String OPTION_USER_FILTER = "userFilter";

    /**
     * Login module option name - LDAP search scope used for {@value #OPTION_USER_FILTER} search. Allowed values:
     * <ul>
     * <li>subtree - searches for objects in the given context and its subtree</li>
     * <li>one-level - searches just one-level under the given context</li>
     * <li>object - searches (or tests) just for the context object itself (if it matches the filter criteria)</li>
     * </ul>
     */
    public static final String OPTION_USER_SEARCH_SCOPE = "userSearchScope";

    /**
     * Login module option name - Credentials verification is done by new LDAP binds by default. Nevertheless, the password can
     * be stored in a non-default LDAP attribute and in this case use passwordAttribute to configure against which LDAP
     * attribute (within user object) is the password provided during the login compared. As a result, if the passwordAttribute
     * option is provided, then the extra LDAP bind to verify credentials is not done and passwords are just compared within the
     * login module code after the retrieving user object from the LDAP server.
     */
    public static final String OPTION_PASSWORD_ATTRIBUTE = "passwordAttribute";

    /**
     * Default value for the {@value #OPTION_USER_FILTER} option.
     */
    public static final String DEFAULT_USER_FILTER = "(uid=" + PLACEHOLDER_LOGIN + ")";

    private LdapSearchScope userSearchScope;
    private String userContext;
    private String userFilter;
    private String passwordAttribute;

    @Override
    protected void onInitialize() {
        super.onInitialize();
        userSearchScope = getSearchScope(OPTION_USER_SEARCH_SCOPE);
        userContext = getStringOption(OPTION_USER_CONTEXT, "");
        userFilter = getStringOption(OPTION_USER_FILTER, DEFAULT_USER_FILTER);
        passwordAttribute = getStringOption(OPTION_PASSWORD_ATTRIBUTE, null);
    }

    @Override
    @SuppressWarnings("checkstyle:npathcomplexity")
    protected Attributes setUserDnAndGetAttributes() throws NamingException, FailedLoginException {
        SearchControls searchControls = new SearchControls();
        searchControls.setSearchScope(userSearchScope.toSearchControlValue());

        SearchResult userSearchResult = null;
        String filter = replacePlaceholders(userFilter, PLACEHOLDER_LOGIN, login);
        if (logger.isFineEnabled()) {
            logger.fine("Searching a user object in LDAP server. Filter: " + filter);
        }
        NamingEnumeration<SearchResult> namingEnum = ctx.search(userContext, filter, searchControls);
        boolean isRelative = false;
        // use the first non-referral entry
        while (namingEnum.hasMore() && !isRelative) {
            userSearchResult = namingEnum.next();
            isRelative = userSearchResult.isRelative();
        }
        if (userSearchResult == null || !isRelative) {
            throw new FailedLoginException("User not found");
        }
        userDN = userSearchResult.getName();
        if (!isNullOrEmpty(userContext)) {
            userDN = userDN + "," + userContext;
        }
        if (logger.isFineEnabled()) {
            logger.fine("Matching user object was found. DN: " + userDN);
        }
        userAttributes = userSearchResult.getAttributes();
        if (passwordAttribute == null) {
            logger.fine("Verifying user credentials by doing a new LDAP bind.");
            authenticateByNewBind(userDN, password);
        } else {
            if (logger.isFineEnabled()) {
                logger.fine("Verifying user credentials by comparing provided password against LDAP attribute "
                        + passwordAttribute);
            }
            String passwordInLdap = getAttributeValue(userAttributes, passwordAttribute);
            if (passwordInLdap == null || !passwordInLdap.equals(password)) {
                throw new FailedLoginException("Provided password doesn't match the expected value.");
            }
        }

        return userSearchResult.getAttributes();
    }

    @Override
    protected LdapContext createLdapContext() throws NamingException {
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.putAll(options);
        logLdapContextProperties(env);
        return new InitialLdapContext(env, null);
    }

    private void authenticateByNewBind(String principalDn, String password) throws FailedLoginException {
        if (isNullOrEmpty(principalDn) || isNullOrEmpty(password)) {
            throw new FailedLoginException("Anonymous bind is not allowed");
        }
        final Properties env = new Properties();
        env.putAll(options);
        env.setProperty(Context.SECURITY_AUTHENTICATION, "simple");
        env.setProperty(Context.SECURITY_PRINCIPAL, principalDn);
        env.setProperty(Context.SECURITY_CREDENTIALS, password);
        try {
            logLdapContextProperties(env);
            new InitialLdapContext(env, null).close();
        } catch (NamingException e) {
            logger.finest(e);
            throw new FailedLoginException("User authentication by LDAP bind failed");
        }
    }

    @Override
    protected String getName() {
        return name;
    }
}
