package com.hazelcast.client.hotrestart;

import com.hazelcast.client.ClientClusterRestartEventTest;
import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseParallelJUnitClassRunner;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.spi.hotrestart.HotRestartFolderRule;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Rule;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.File;

@RunWith(EnterpriseParallelJUnitClassRunner.class)
@Category(QuickTest.class)
public class ClientHotRestartTest extends ClientClusterRestartEventTest {

    @Rule
    public HotRestartFolderRule hotRestartFolderRule = new HotRestartFolderRule();

    protected Config newConfig() {
        File baseDir = hotRestartFolderRule.getBaseDir();
        Config config = new Config().setLicenseKey(SampleLicense.UNLIMITED_LICENSE);
        config.getHotRestartPersistenceConfig()
                .setEnabled(true)
                .setBaseDir(baseDir);
        return config;
    }
}
