/*
 * Copyright (c) 2008-2018, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.internal.cluster.impl;

import com.hazelcast.internal.serialization.impl.EnterpriseClusterVersionAware;
import com.hazelcast.internal.serialization.impl.EnterpriseSerializationServiceBuilder;
import com.hazelcast.nio.Address;
import com.hazelcast.spi.serialization.SerializationService;
import com.hazelcast.test.HazelcastParametersRunnerFactory;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.version.MemberVersion;
import com.hazelcast.version.Version;
import org.junit.Assume;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Parameterized.UseParametersRunnerFactory;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static com.hazelcast.internal.cluster.Versions.V3_10;
import static com.hazelcast.internal.cluster.Versions.V3_9;
import static com.hazelcast.test.HazelcastTestSupport.randomString;
import static com.hazelcast.version.Version.UNKNOWN;

@RunWith(Parameterized.class)
@UseParametersRunnerFactory(HazelcastParametersRunnerFactory.class)
@Category({QuickTest.class, ParallelTest.class})
public class EnterpriseSplitBrainJoinMessageSerializationTest extends SplitBrainJoinMessageSerializationTest {

    @Parameter
    public Version streamVersion;

    @Parameters(name = "{0}")
    public static Collection<Object[]> parameters() {
        return Arrays.asList(new Object[][] {
                {UNKNOWN},
                {V3_9},
                {V3_10},
        });
    }

    @Override
    SerializationService createSerializationService() {
        return new EnterpriseSerializationServiceBuilder()
                .setVersionedSerializationEnabled(true)
                .setClusterVersionAware(new EnterpriseClusterVersionAware() {
                    @Override
                    public Version getClusterVersion() {
                        return streamVersion;
                    }
                })
                .build();
    }

    // tests serialization of SplitBrainJoinMessage within a not-Versioned SplitBrainMergeValidationOp from 3.9 member
    @Test
    public void testUnversionedSerialization_from39_to310() {
        // the version of the ObjectDataInput will be UNKNOWN in this case
        Assume.assumeTrue(streamVersion.isUnknown());
        message = new SplitBrainJoinMessage((byte) 1, 2, MemberVersion.of("3.9.3"), address,
                randomString(), false, configCheck, Collections.<Address>emptyList(), 1, V3_9, 3);
        // from 3.9.3 / UNKNOWN memberListVersion bytes are not expected to be received
        assertSplitBrainJoinMessage(false);
    }

    // tests serialization of a standalone SplitBrainJoinMessage from 3.9 member
    @Test
    public void testSerialization_from39_to310() {
        // the version of the ObjectDataInput will be set to cluster version (ie 3.9) in this case
        Assume.assumeTrue(streamVersion.equals(V3_9));
        message = new SplitBrainJoinMessage((byte) 1, 2, MemberVersion.of("3.9.3"), address,
                randomString(), false, configCheck, Collections.<Address>emptyList(), 1, V3_9, 3);
        // from 3.9.3 / V3_9 memberListVersion bytes are expected to be received
        assertSplitBrainJoinMessage(true);
    }

    // tests serialization of a versioned SplitBrainJoinMessage from 3.10 member when cluster version is 3.9
    @Test
    public void testSerialization_from310_to310_whenClusterVersion39() {
        // the version of the ObjectDataInput will be set to cluster version (ie 3.9) in this case
        Assume.assumeTrue(streamVersion.equals(V3_9));
        message = new SplitBrainJoinMessage((byte) 1, 2, MemberVersion.of("3.10.1"), address,
                randomString(), false, configCheck, Collections.<Address>emptyList(), 1, V3_9, 3);
        // from 3.10.1 / V3_9 memberListVersion bytes are expected to be received
        assertSplitBrainJoinMessage(true);
    }

    // tests serialization of a versioned SplitBrainJoinMessage from 3.10 member when cluster version is 3.10
    @Test
    public void testSerialization_from310_to310_whenClusterVersion310() {
        // the version of the ObjectDataInput will be 3.10
        Assume.assumeTrue(streamVersion.equals(V3_10));
        message = new SplitBrainJoinMessage((byte) 1, 2, MemberVersion.of("3.10.1"), address,
                randomString(), false, configCheck, Collections.<Address>emptyList(), 1, V3_10, 3);
        // from 3.10.1 / V3_10 memberListVersion bytes are expected to be received
        assertSplitBrainJoinMessage(true);
    }

    // tests serialization of a SplitBrainJoinMessage from 3.11 member when cluster version is 3.10
    // or in case SplitBrainJoinMessage is no longer Versioned in 3.11 (-> stream version is UNKNOWN)
    @Test
    public void testSerialization_from311_to310() {
        // the version of the ObjectDataInput will be 3.10 or UNKNOWN
        Assume.assumeTrue(streamVersion.isUnknown() || streamVersion.isEqualTo(V3_10));
        message = new SplitBrainJoinMessage((byte) 1, 2, MemberVersion.of("3.11"), address,
                randomString(), false, configCheck, Collections.<Address>emptyList(), 1, V3_10, 3);
        // from 3.11 memberListVersion bytes are expected to be received
        assertSplitBrainJoinMessage(true);
    }
}
