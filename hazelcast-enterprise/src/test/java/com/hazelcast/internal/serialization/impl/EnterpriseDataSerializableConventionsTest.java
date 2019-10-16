package com.hazelcast.internal.serialization.impl;

import com.hazelcast.cache.impl.wan.WanCacheEntryView;
import com.hazelcast.internal.util.JavaVersion;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.Set;

/**
 * Tests for classes which are used as Data in the client protocol
 */
@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class})
public class EnterpriseDataSerializableConventionsTest extends DataSerializableConventionsTest {

    private static final String[] WHITELISTED_CLASS_NAMES = new String[] {
            "com.hazelcast.com.fasterxml.jackson.core.Base64Variant",
            "com.hazelcast.com.fasterxml.jackson.core.JsonFactory",
            "com.hazelcast.com.fasterxml.jackson.core.JsonLocation",
            "com.hazelcast.com.fasterxml.jackson.core.JsonpCharacterEscapes",
            "com.hazelcast.com.fasterxml.jackson.core.Version",
            "com.hazelcast.com.fasterxml.jackson.core.io.SerializedString",
            "com.hazelcast.com.fasterxml.jackson.core.util.DefaultIndenter",
            "com.hazelcast.com.fasterxml.jackson.core.util.DefaultPrettyPrinter",
            "com.hazelcast.com.fasterxml.jackson.core.util.DefaultPrettyPrinter$FixedSpaceIndenter",
            "com.hazelcast.com.fasterxml.jackson.core.util.DefaultPrettyPrinter$NopIndenter",
            "com.hazelcast.com.fasterxml.jackson.core.util.InternCache",
            "com.hazelcast.com.fasterxml.jackson.core.util.MinimalPrettyPrinter",
            "com.hazelcast.com.fasterxml.jackson.core.util.RequestPayload",
            "com.hazelcast.com.fasterxml.jackson.core.util.Separators",
    };

    // snakeyaml classes are compiled for java target 1.8
    private static final String[] WHITELISTED_JDK8_CLASS_NAMES = new String[] {
            "com.hazelcast.org.snakeyaml.engine.v1.exceptions.Mark",
            "com.hazelcast.org.snakeyaml.engine.v1.representer.BaseRepresenter$1",
    };

    @Override
    protected Set<Class> getWhitelistedClasses() {
        Set<Class> classes = super.getWhitelistedClasses();
        classes.add(WanCacheEntryView.class);

        for (String className : WHITELISTED_CLASS_NAMES) {
            addToWhiteList(classes, className);
        }
        if (JavaVersion.isAtLeast(JavaVersion.JAVA_8)) {
            // only resolve those classes when tests are executed with Java 8 or later
            for (String className : WHITELISTED_JDK8_CLASS_NAMES) {
                addToWhiteList(classes, className);
            }
        }
        return classes;
    }

    // whitelist class accessible only by class name
    private void addToWhiteList(Set<Class> whitelist, String className) {
        try {
            whitelist.add(Class.forName(className));
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
    }
}
