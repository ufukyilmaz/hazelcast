package com.hazelcast;

import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.instance.BuildInfoProvider;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertTrue;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class DataSerializerHookTest {

    private final Set<String> enterpriseAllSet = new HashSet<String>();
    private final String ossCommitId = BuildInfoProvider.getBuildInfo().getUpstreamBuildInfo().getCommitId();
    private final String eeAllPath = "src/main/resources/META-INF/services/com.hazelcast.DataSerializerHook";

    private BufferedReader ossInput;
    private BufferedReader eeInput;
    private BufferedReader wmInput;

    @Before
    public void loadResources() throws IOException {
        BufferedReader eeAllInput = new BufferedReader(new FileReader(eeAllPath));
        for (String line = eeAllInput.readLine(); line != null; line = eeAllInput.readLine()) {
            if (line.startsWith("com")) {
                enterpriseAllSet.add(line);
            }
        }

        URL ossURL = new URL("https://raw.githubusercontent.com/hazelcast/hazelcast/" + ossCommitId
                + "/hazelcast/src/main/resources/META-INF/services/com.hazelcast.DataSerializerHook"
        );
        ossInput = new BufferedReader(new InputStreamReader(ossURL.openStream()));

        eeInput = new BufferedReader(new FileReader("../hazelcast-enterprise/src/main/resources/META-INF/"
                + "services/com.hazelcast.DataSerializerHook")
        );

        URL wmURL = new URL("https://raw.githubusercontent.com/hazelcast/hazelcast-wm/master/src/main/resources/META-INF/"
                + "services/com.hazelcast.DataSerializerHook"
        );
        wmInput = new BufferedReader(new InputStreamReader(wmURL.openStream()));
    }

    @Ignore
    @Test
    public void testMergedCorrectly() throws IOException {
        for (String line = ossInput.readLine(); line != null; line = ossInput.readLine()) {
            if (line.startsWith("com")) {
                assertTrue("Class in OSS: " + line + " is missing in file: hazelcast-enterprise-all/" + eeAllPath,
                        enterpriseAllSet.contains(line));
                enterpriseAllSet.remove(line);
            }
        }

        for (String line = wmInput.readLine(); line != null; line = wmInput.readLine()) {
            if (line.startsWith("com")) {
                assertTrue("Class in WM module: " + line + " is missing!", enterpriseAllSet.contains(line));
                enterpriseAllSet.remove(line);
            }
        }

        for (String line = eeInput.readLine(); line != null; line = eeInput.readLine()) {
            if (line.startsWith("com")) {
                assertTrue("Class in EE: " + line + " is missing!", enterpriseAllSet.contains(line));
                enterpriseAllSet.remove(line);
            }
        }

        StringBuilder extraLines = new StringBuilder();
        for (String line : enterpriseAllSet) {
            extraLines.append(line).append("\n");
        }

        assertTrue("Below classes in EE doesn't exists\n" + extraLines.toString(), enterpriseAllSet.isEmpty());
    }
}
