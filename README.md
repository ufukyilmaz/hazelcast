## Hazelcast Enterprise

### Maven repository

Releases repository:

````xml
<repository>
    <id>hazelcast-ee-release</id>
    <name>Hazelcast Private Release Repository</name>
    <url>https://repository.hazelcast.com/release</url>
    <releases>
        <enabled>true</enabled>
    </releases>
    <snapshots>
        <enabled>false</enabled>
    </snapshots>
</repository>
````

Snapshots repository:

````xml
<repository>
    <id>hazelcast-ee-snapshot</id>
    <name>Hazelcast Private Snapshot Repository</name>
    <url>https://repository.hazelcast.com/snapshot</url>
    <releases>
        <enabled>false</enabled>
    </releases>
    <snapshots>
        <enabled>true</enabled>
    </snapshots>
</repository>
````

### Additional Java arguments used for testing

If you need to provide additional Java arguments to test runs, then set them to `extraVmArgs` property.

**Example:**
```bash
mvn install "-DextraVmArgs=-XX:+UnlockCommercialFeatures -XX:+FlightRecorder -Dopenssl.enforce=true"
```

#### Available System properties

Property name | Default value | Description
---           | ---           | ---
`cacheName`   | "CACHE"       | Cache name used in JCache tests
`openssl.enforce` | `false`   | A true/false flag which allows to enforce running OpenSSL tests even if the OpenSSL reports it's not supported on the given platform.

### Testing for rolling upgrades compatibility

Compatibility tests are categorized with `com.hazelcast.test.annotation.CompatibilityTest` and are executed with the `compatibility-tests` profile.

```bash
mvn -Pcompatibility-tests clean install
```

Running compatibility tests requires real network, so tests are executed serially. The `compatibility-tests` profile takes care of setting up system property `hazelcast.test.compatibility=true`.

In order to execute the compatibility tests in your IDE, make sure you select tests by category `com.hazelcast.test.annotation.CompatibilityTest` and add `-Dhazelcast.test.compatibility=true` in VM arguments. Also, depending on the environment, you may see the following warning in the logs:
```
Hazelcast is bound to ... and loop-back mode is disabled in the configuration. This could cause multicast auto-discovery issues and render it unable to work. Check you network connectivity, try to enable the loopback mode and/or force -Djava.net.preferIPv4Stack=true on your JVM.
```

This issue can be resolved by adding `-Djava.net.preferIPv4Stack=true` VM argument.

In case compatibility tests are executed slowly, it may be the case that some of the released Hazelcast artifacts are not located in your local maven repository. In this case, you will see log entries like the following for each artifact that is not available locally:

```
WARNING: Hazelcast binaries for version 3.9 will be downloaded from a remote repository. You can speed up the compatibility tests by installing the missing artifacts in your local maven repository so they don't have to be downloaded each time:
 $ mvn dependency:get -Dartifact=com.hazelcast:hazelcast:3.9
```

Executing the provided `mvn` commands will download the artifacts in your local maven repository and speed up test execution.
