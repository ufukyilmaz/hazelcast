/*
 * Copyright (c) 2008-2015, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.spi.hotrestart.cluster;

import com.hazelcast.nio.Address;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Random;

import static com.hazelcast.nio.IOUtil.delete;
import static com.hazelcast.nio.IOUtil.toFileName;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public abstract class AbstractReaderWriterTest {

    @Rule
    public TestName testName = new TestName();

    protected InetAddress localAddress;
    protected File folder;

    @Before
    public final void setup() throws UnknownHostException {
        localAddress = InetAddress.getLocalHost();

        folder = new File(toFileName(getClass().getSimpleName()) + '_' + toFileName(testName.getMethodName()));
        delete(folder);
        if (!folder.mkdir() && !folder.exists()) {
            throw new AssertionError("Unable to create test folder: " + folder.getAbsolutePath());
        }

        setupInternal();
    }

    void setupInternal() {}

    @After
    public final void tearDown() {
        tearDownInternal();
        if (folder != null) {
            delete(folder);
        }
    }

    void tearDownInternal() {}

    Address[] initializeAddresses(int len) {
        Address[] addresses = new Address[len];
        Random random = new Random();
        for (int i = 0; i < addresses.length; i++) {
            addresses[i] = new Address("10.10.10." + random.nextInt(256), localAddress, i + 1);
        }
        return addresses;
    }

    File getNotExistingFolder() {
        return new File(folder.getParentFile(), "NOT_EXISTING_FOLDER");
    }

    static void assertAddressEquals(Address address1, Address address2) {
        if (address1 == null) {
            assertNull(address2);
        } else {
            assertEquals(address1, address2);
        }
    }
}
