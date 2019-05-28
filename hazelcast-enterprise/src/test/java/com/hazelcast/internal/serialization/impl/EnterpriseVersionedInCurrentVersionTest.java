package com.hazelcast.internal.serialization.impl;

import com.hazelcast.test.annotation.ParallelJVMTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.Version;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore({"javax.net.ssl.*", "javax.security.*", "javax.management.*"})
@PrepareForTest(Version.class)
@Category({QuickTest.class, ParallelJVMTest.class})
public class EnterpriseVersionedInCurrentVersionTest extends VersionedInCurrentVersionTest {
}
