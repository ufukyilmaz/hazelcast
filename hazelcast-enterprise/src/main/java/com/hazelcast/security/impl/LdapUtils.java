package com.hazelcast.security.impl;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;

/**
 * Utility class for LDAP/JNDI related processing.
 */
public final class LdapUtils {

    private LdapUtils() {
    }

    /**
     * Returns first value of given attribute within provided {@link Attributes}.
     *
     * @return attribute value or {@code null}
     * @throws NamingException
     */
    public static String getAttributeValue(Attributes attributes, String attribute) throws NamingException {
        Attribute attr = attributes.get(attribute);
        if (attr != null) {
            Object value = attr.get();
            if (value != null) {
                return (value instanceof byte[]) ? new String((byte[]) value, StandardCharsets.UTF_8) : value.toString();
            }
        }
        return null;
    }

    /**
     * Returns all values of given attribute within provided {@link Attributes}.
     *
     * @return collection of String values
     * @throws NamingException
     */
    public static Collection<String> getAttributeValues(Attributes attributes, String attribute) throws NamingException {
        Set<String> names = new HashSet<String>();
        Attribute attr = attribute != null && attributes != null ? attributes.get(attribute) : null;
        if (attr != null) {
            Object value = attr.get();
            if (value != null) {
                String name = (value instanceof byte[]) ? new String((byte[]) value, StandardCharsets.UTF_8) : value.toString();
                names.add(name);
            }
        }
        return names;
    }

    /**
     * Parses LDAP name to RDNs and returns values of given attribute within RDN attributes.
     *
     * @return collection of String values
     * @throws NamingException
     */
    public static Collection<String> getAttributeValues(LdapName ldapName, String attribute) throws NamingException {
        Set<String> names = new HashSet<String>();
        for (Rdn rdn : ldapName.getRdns()) {
            names.addAll(getAttributeValues(rdn.toAttributes(), attribute));
        }
        return names;
    }

    public static String replacePlaceholders(String filter, String... placeHolders) {
        int i = 0;
        while (i < placeHolders.length) {
            filter = filter.replace(placeHolders[i * 2], escapeSearchValue(placeHolders[i * 2 + 1]));
            i += 2;
        }
        return filter;
    }

    /**
     * Escapes given value for usage in a LDAP filter.
     *
     * @param value value to be escaped
     * @return escaped value
     */
    // see https://ldap.com/ldap-filters/
    public static String escapeSearchValue(String value) {
        if (value == null) {
            return null;
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < value.length(); i++) {
            char ch = value.charAt(i);
            switch (ch) {
                case '\u0000':
                    sb.append("\\00");
                    break;
                case '(':
                    sb.append("\\28");
                    break;
                case ')':
                    sb.append("\\29");
                    break;
                case '*':
                    sb.append("\\2a");
                    break;
                case '\\':
                    sb.append("\\5c");
                    break;
                default:
                    sb.append(ch);
            }
        }
        return sb.toString();
    }

}
