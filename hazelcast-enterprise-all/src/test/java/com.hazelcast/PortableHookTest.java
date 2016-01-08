package com.hazelcast;

import org.junit.Before;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

public class PortableHookTest {
    private final Set<String> enterpriseAllSet = new HashSet<String>();
    private BufferedReader eeAllInput;
    private URL ossURL;
    private BufferedReader ossIn;
    private BufferedReader eeInput;

    @Before
    public void loadResources() throws IOException {
        eeAllInput = new BufferedReader(
                new FileReader(
                        "src/main/resources/META-INF/services/com.hazelcast.PortableHook"
                )
        );

        for (String line = eeAllInput.readLine(); line != null; line = eeAllInput.readLine()) {
            if (line.startsWith("com")) {
                enterpriseAllSet.add(line);
            }
        }

        ossURL = new URL(
                "https://raw.githubusercontent.com/hazelcast/hazelcast/master/hazelcast/src/main/resources/META-INF/services/com.hazelcast.PortableHook"
        );
        ossIn = new BufferedReader(
                new InputStreamReader(ossURL.openStream()));

        eeInput = new BufferedReader(
                new FileReader(
                        "../hazelcast-enterprise/src/main/resources/META-INF/services/com.hazelcast.PortableHook"
                )
        );
    }

    @Test
    public void testMergedCorrectly() throws IOException {
        for (String line = ossIn.readLine(); line != null; line = ossIn.readLine()) {
            if (line.startsWith("com")) {
                assertTrue("Class in OSS: " + line + " is missing!", enterpriseAllSet.contains(line));
            }
        }

        for (String line = eeInput.readLine(); line != null; line = eeInput.readLine()) {
            if (line.startsWith("com")) {
                assertTrue("Class in EE: " + line + " is missing!", enterpriseAllSet.contains(line));
            }
        }
    }
}
