package com.hazelcast.instance;

/**
 * This class is generated in a build-time from a template stored at
 * src/main/template/com/hazelcast/instance/GeneratedEnterpriseBuildProperties
 *
 * Do not edit by hand as the changes will be overwritten in the next build.
 *
 * We used to have the version info as property file, but this caused issues
 * in on environments with a complicated classloading model. Having the info
 * as a Java class provide a better control when you have multiple version of
 * Hazelcast deployed.
 *
 * WARNING: DO NOT CHANGE FIELD NAMES IN THE TEMPLATE.
 * The fields are read via reflection at {@link com.hazelcast.instance.BuildInfoProvider}
 *
 */
@SuppressWarnings("checkstyle:javadocvariable")
public final class GeneratedEnterpriseBuildProperties {
    public static final String VERSION = "${project.version}";
    public static final String BUILD = "${timestamp}";
    public static final String REVISION = "${git.commit.id.abbrev}";
    public static final String COMMIT_ID = "${git.commit.id}";
    public static final String DISTRIBUTION = "${hazelcast.distribution}";
    public static final String SERIALIZATION_VERSION = "${hazelcast.serialization.version}";

    private GeneratedEnterpriseBuildProperties() {
    }
}
