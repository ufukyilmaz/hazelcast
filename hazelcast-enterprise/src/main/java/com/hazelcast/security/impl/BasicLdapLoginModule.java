package com.hazelcast.security.impl;

import static com.hazelcast.security.impl.LdapUtils.getAttributeValue;
import static com.hazelcast.security.impl.LdapUtils.getAttributeValues;
import static com.hazelcast.security.impl.LdapUtils.replacePlaceholders;
import static com.hazelcast.internal.util.StringUtil.isNullOrEmpty;
import static com.hazelcast.internal.util.StringUtil.trim;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;

import javax.naming.Context;
import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.Attributes;
import javax.naming.directory.SearchControls;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.InitialLdapContext;
import javax.naming.ldap.LdapContext;
import javax.naming.ldap.LdapName;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.callback.UnsupportedCallbackException;
import javax.security.auth.login.FailedLoginException;
import javax.security.auth.login.LoginException;

import com.hazelcast.security.ClusterLoginModule;

/**
 * JAAS login module which uses LDAP as a user population store. It binds to the configured server with provided username (DN)
 * and password. Within the established LDAP context it loads the user attributes and makes role search queries if necessary.
 */
public class BasicLdapLoginModule extends ClusterLoginModule {

    /**
     * Placeholder string to be replaced by a user or role DN in the {@value #OPTION_ROLE_FILTER} option.
     */
    public static final String PLACEHOLDER_DN = "{memberDN}";

    /**
     * Login module option name - LDAP Attribute name which value will be used as a name in ClusterIdentityPrincipal added to
     * the JAAS Subject.
     */
    public static final String OPTION_USER_NAME_ATTRIBUTE = "userNameAttribute";

    /**
     * Login module option name - If the option is set to {@code true}, then it treats the value of the
     * {@code roleMappingAttribute} as a DN and extracts only {@code roleNameAttribute} attribute values as role names. When
     * the option value is {@code false}, then the whole value of {@code roleMappingAttribute} is used as a role name.
     * <p>
     * This option is only used when the roleMappingMode option has value "attribute".
     */
    public static final String OPTION_PARSE_DN = "parseDN";

    /**
     * Login module option name - Role mapping mode - it can have one of the following values:
     * <ul>
     * <li><em>attribute</em> - user object in the LDAP contains directly role name in the given attribute. Role name can be
     * parsed from a DN string when parseDN=true. No additional LDAP query is done to find assigned roles.</li>
     * <li><em>direct</em> - user object contains an attribute with DN(s) of assigned role(s). Role object(s) is/are loaded from
     * the LDAP and the role name is retrieved from its attributes. Role search recursion can be enabled for this mode.</li>
     * <li><em>reverse</em> - role objects are located by executing an LDAP search query with given roleFilter. In this case,
     * the role object usually contains attributes with DNs of assigned users. Role search recursion can be enabled for this
     * mode.</li>
     * </ul>
     */
    public static final String OPTION_ROLE_MAPPING_MODE = "roleMappingMode";

    /**
     * Login module option name - Name of the LDAP attribute which contains either role name or role DN.
     * <p>
     * This option is only used when the roleMappingMode option has value "attribute" or "direct".
     */
    public static final String OPTION_ROLE_MAPPING_ATTRIBUTE = "roleMappingAttribute";

    /**
     * Login module option name - LDAP Context in which assigned roles are searched. (E.g. ou=Roles,dc=hazelcast,dc=com)
     * <p>
     * This option is only used when the roleMappingMode option has value "reverse".
     */
    public static final String OPTION_ROLE_CONTEXT = "roleContext";

    /**
     * Login module option name - LDAP search string which usually contains placeholder {memberDN} to be replaced by provided
     * login name. (E.g. (member={memberDN}))
     * <p>
     * If the role search recursion is enabled (see roleRecursionMaxDepth), the {memberDN} is replaced by role DNs in the
     * recurrent searches.
     * <p>
     * This option is only used when the roleMappingMode option has value "reverse".
     */
    public static final String OPTION_ROLE_FILTER = "roleFilter";

    /**
     * Login module option name - Sets max depth of role search recursion. The default value 1 means the role search recursion
     * is disabled.
     * <p>
     * This option is only used when the roleMappingMode option has value "direct" or "reverse".
     */
    public static final String OPTION_ROLE_RECURSION_MAX_DEPTH = "roleRecursionMaxDepth";

    /**
     * This option either refers to a name of LDAP attribute within role object which contains the role name in case of "direct"
     * and "reverse" roleMappingMode values. Or it refers to the attribute name within X.500 name stored
     * in roleMappingAttribute when roleMappingMode=attribute and parseDN=true.
     */
    public static final String OPTION_ROLE_NAME_ATTRIBUTE = "roleNameAttribute";

    /**
     * LDAP search scope used for roleFilter search. Allowed values comes from the {@link SearchScope} enum:
     * <ul>
     * <li>subtree - searches for objects in the given context and its subtree</li>
     * <li>one-level - searches just one-level under the given context</li>
     * <li>object - searches (or tests) just for the context object itself (if it matches the filter criteria)</li>
     * </ul>
     * <p>
     * This option is only used when the roleMappingMode option has value "reverse".
     */
    public static final String OPTION_ROLE_SEARCH_SCOPE = "roleSearchScope";

    /**
     * @see #OPTION_USER_NAME_ATTRIBUTE
     */
    public static final String DEFAULT_USER_NAME_ATTRIBUTE = "uid";

    /**
     * @see #OPTION_PARSE_DN
     */
    public static final boolean DEFAULT_PARSE_DN = false;

    /**
     * Role search recursion is disabled by default
     *
     * @see #OPTION_ROLE_RECURSION_MAX_DEPTH
     */
    public static final int DEFAULT_ROLE_RECURSION_MAX_DEPTH = 1;

    /**
     * Default search scope for LDAP searches done by this login module. It's used when no value is configured in a search
     * related option (e.g. {@link #OPTION_ROLE_SEARCH_SCOPE}).
     */
    public static final SearchScope DEFAULT_SEARCH_SCOPE = SearchScope.SUBTREE;

    /**
     * @see #OPTION_ROLE_MAPPING_MODE
     */
    public static final RoleMappingMode DEFAULT_MAPPING_MODE = RoleMappingMode.ATTRIBUTE;

    protected String name;

    protected String login;
    protected String password;
    protected String userDN;
    protected String userNameAttribute;
    protected String roleMappingAttribute;
    protected RoleMappingMode roleMappingMode;
    protected String roleNameAttribute;
    protected String roleFilter;
    protected String roleContext;
    protected SearchScope roleSearchScope;
    protected boolean parseFromDN;
    protected int maxRecursionDepth;
    protected Attributes userAttributes;
    protected LdapContext ctx;

    protected Set<String> visitedRoleDns;

    @Override
    protected void onInitialize() {
        super.onInitialize();
        userNameAttribute = getStringOption(OPTION_USER_NAME_ATTRIBUTE, DEFAULT_USER_NAME_ATTRIBUTE);
        roleNameAttribute = getStringOption(OPTION_ROLE_NAME_ATTRIBUTE, null);
        roleMappingAttribute = getStringOption(OPTION_ROLE_MAPPING_ATTRIBUTE, null);
        roleMappingMode = getRoleMappingMode(OPTION_ROLE_MAPPING_MODE);
        roleFilter = getStringOption(OPTION_ROLE_FILTER, "(" + roleMappingAttribute + "=" + PLACEHOLDER_DN + ")");
        roleContext = getStringOption(OPTION_ROLE_CONTEXT, "");
        roleSearchScope = getSearchScope(OPTION_ROLE_SEARCH_SCOPE);
        parseFromDN = getBoolOption(OPTION_PARSE_DN, DEFAULT_PARSE_DN);
        maxRecursionDepth = getIntOption(OPTION_ROLE_RECURSION_MAX_DEPTH, DEFAULT_ROLE_RECURSION_MAX_DEPTH);
        visitedRoleDns = new HashSet<String>();
        verifyOptions();
    }

    @Override
    protected boolean onLogin() throws LoginException {
        NameCallback ncb = new NameCallback("Name");
        PasswordCallback pcb = new PasswordCallback("Password", false);
        try {
            callbackHandler.handle(new Callback[] { ncb, pcb });
        } catch (IOException | UnsupportedCallbackException e) {
            logger.finest(e);
            throw new FailedLoginException("Handling callbacks failed.. " + e.getMessage());
        }
        char[] pass = pcb.getPassword();
        login = ncb.getName();
        password = pass == null ? null : new String(pass);
        pcb.clearPassword();
        if (isNullOrEmpty(password) || isNullOrEmpty(login)) {
            throw new FailedLoginException("Both the login name and the password have to be provided.");
        }
        try {
            ctx = createLdapContext();
            try {
                userAttributes = setUserDnAndGetAttributes();
                name = getAttributeValue(userAttributes, userNameAttribute);

                if (!isSkipRole()) {
                    switch (roleMappingMode) {
                        case ATTRIBUTE:
                            addRolesFromAttribute();
                            break;
                        case DIRECT:
                            addRolesDirectMapping(1, getAttributeValues(userAttributes, roleMappingAttribute));
                            break;
                        case REVERSE:
                            addRolesReverseMapping(1, userDN);
                            break;
                        default:
                            throw new LoginException("Unexpected Role Mapping mode");
                    }
                }
            } finally {
                ctx.close();
            }
        } catch (NamingException e) {
            logger.finest(e);
            throw new FailedLoginException("Naming problem occured: " + e.getMessage());
        }

        return true;
    }

    protected void verifyOptions() {
        logger.finest("Verifying provided options and credentials");
        checkOptionInMappingMode(OPTION_PARSE_DN, RoleMappingMode.ATTRIBUTE);
        checkOptionInMappingMode(OPTION_ROLE_CONTEXT, RoleMappingMode.REVERSE);
        checkOptionInMappingMode(OPTION_ROLE_FILTER, RoleMappingMode.REVERSE);
        checkOptionInMappingMode(OPTION_ROLE_RECURSION_MAX_DEPTH, RoleMappingMode.DIRECT, RoleMappingMode.REVERSE);
    }

    private void checkOptionInMappingMode(String optionName, RoleMappingMode... supportedModes) {
        if (!logger.isWarningEnabled()) {
            return;
        }
        boolean optionConfigured = getStringOption(optionName, null) != null;
        if (optionConfigured) {
            boolean optionSupported = false;
            for (RoleMappingMode mode : supportedModes) {
                optionSupported |= (roleMappingMode == mode);
            }
            if (!optionSupported) {
                logger.warning("Login module option " + optionName + " is not supported when roleMappingMode=="
                        + roleMappingMode.toString() + ". It's only supported in following mapping mode(s): "
                        + Arrays.toString(supportedModes));
            }
        }
    }

    protected Attributes setUserDnAndGetAttributes() throws NamingException, FailedLoginException {
        userDN = login;
        return ctx.getAttributes(userDN);
    }

    protected LdapContext createLdapContext() throws NamingException {
        Properties env = new Properties();
        env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.ldap.LdapCtxFactory");
        env.putAll(options);
        env.put(Context.SECURITY_AUTHENTICATION, "simple");
        env.put(Context.SECURITY_PRINCIPAL, login);
        env.put(Context.SECURITY_CREDENTIALS, password);
        logger.fine("Creating an LDAP context");
        return new InitialLdapContext(env, null);
    }

    private void addRolesFromAttribute() throws NamingException {
        if (logger.isFineEnabled()) {
            logger.fine("Searching role names in user attribute: " + roleMappingAttribute);
        }
        for (String roleAttrVal : getAttributeValues(userAttributes, roleMappingAttribute)) {
            if (parseFromDN) {
                for (String role : getAttributeValues(new LdapName(roleAttrVal), roleNameAttribute)) {
                    addRole(role);
                }
            } else {
                addRole(roleAttrVal);
            }
        }
    }

    private void addRolesDirectMapping(int level, Collection<String> dns) throws NamingException {
        if (level > maxRecursionDepth) {
            if (logger.isFineEnabled()) {
                logger.fine("The " + OPTION_ROLE_RECURSION_MAX_DEPTH + "==" + maxRecursionDepth + " was reached.");
            }
            return;
        }

        for (String dn : dns) {
            if (!visitedRoleDns.contains(dn)) {
                visitedRoleDns.add(dn);
                if (logger.isFineEnabled()) {
                    logger.fine("Searching roles within LDAP object: " + dn);
                }
                Attributes roleObjectAttributes = ctx.getAttributes(new LdapName(dn));
                for (String role : getAttributeValues(roleObjectAttributes, roleNameAttribute)) {
                    addRole(role);
                }
                if (level < maxRecursionDepth) {
                    addRolesDirectMapping(level + 1, getAttributeValues(roleObjectAttributes, roleMappingAttribute));
                }
            }
        }
    }

    private void addRolesReverseMapping(int level, String dn) throws NamingException {
        if (level > maxRecursionDepth) {
            if (logger.isFineEnabled()) {
                logger.fine("The " + OPTION_ROLE_RECURSION_MAX_DEPTH + "==" + maxRecursionDepth + " was reached.");
            }
            return;
        }
        if (!visitedRoleDns.contains(dn)) {
            visitedRoleDns.add(dn);
            if (logger.isFineEnabled()) {
                logger.fine("Searching roles which contains mapping to LDAP object: " + dn);
            }
            String filter = replacePlaceholders(roleFilter, PLACEHOLDER_DN, dn);
            SearchControls roleSearchControls = new SearchControls();
            roleSearchControls.setSearchScope(roleSearchScope.toSearchControlValue());
            if (logger.isFineEnabled()) {
                logger.fine("Searching role objects with reverse mapping using filter: " + filter);
            }
            NamingEnumeration<SearchResult> roleSearchResultEnum = ctx.search(roleContext, filter, roleSearchControls);
            while (roleSearchResultEnum.hasMore()) {
                SearchResult roleSearchResult = roleSearchResultEnum.next();
                if (!roleSearchResult.isRelative()) {
                    continue;
                }
                for (String role : getAttributeValues(roleSearchResult.getAttributes(), roleNameAttribute)) {
                    addRole(role);
                }
                if (level < maxRecursionDepth) {
                    String roleDN = roleSearchResult.getName();
                    if (!isNullOrEmpty(roleContext)) {
                        roleDN = roleDN + "," + roleContext;
                    }
                    addRolesReverseMapping(level + 1, roleDN);
                }
            }
            roleSearchResultEnum.close();
        }
    }

    protected SearchScope getSearchScope(String optionName) {
        String optionValue = trim(getStringOption(optionName, null));
        if (optionValue == null) {
            return DEFAULT_SEARCH_SCOPE;
        }
        for (SearchScope scope : SearchScope.values()) {
            if (scope.toString().equals(optionValue)) {
                return scope;
            }
        }
        return DEFAULT_SEARCH_SCOPE;
    }

    private RoleMappingMode getRoleMappingMode(String optionName) {
        String optionValue = trim(getStringOption(optionName, null));
        if (optionValue == null) {
            return DEFAULT_MAPPING_MODE;
        }
        for (RoleMappingMode mapping : RoleMappingMode.values()) {
            if (mapping.toString().equals(optionValue)) {
                return mapping;
            }
        }
        return DEFAULT_MAPPING_MODE;
    }

    @Override
    protected String getName() {
        return name;
    }

    protected enum SearchScope {
        OBJECT("object", SearchControls.OBJECT_SCOPE), ONE_LEVEL("one-level", SearchControls.ONELEVEL_SCOPE), SUBTREE("subtree",
                SearchControls.SUBTREE_SCOPE);

        private final String valueString;
        private final int searchControlValue;

        SearchScope(String valueString, int searchControlValue) {
            this.valueString = valueString;
            this.searchControlValue = searchControlValue;
        }

        @Override
        public String toString() {
            return valueString;
        }

        public int toSearchControlValue() {
            return searchControlValue;
        }
    }

    public enum RoleMappingMode {
        ATTRIBUTE("attribute"), DIRECT("direct"), REVERSE("reverse");

        private final String valueString;

        RoleMappingMode(String valueString) {
            this.valueString = valueString;
        }

        @Override
        public String toString() {
            return valueString;
        }
    }
}
