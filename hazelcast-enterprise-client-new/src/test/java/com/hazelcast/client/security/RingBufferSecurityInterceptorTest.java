/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.client.security;

import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.ringbuffer.OverflowPolicy;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.ringbuffer.impl.RingbufferService;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.util.HashSet;
import java.util.concurrent.ExecutionException;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(QuickTest.class)
public class RingBufferSecurityInterceptorTest extends BaseInterceptorTest {
    String objectName;
    Ringbuffer ringBuffer;

    @Before
    public void setup() {
        objectName = randomString();
        ringBuffer = client.getRingbuffer(objectName);
    }

    @Override
    String getObjectType() {
        return RingbufferService.SERVICE_NAME;
    }

    @Test
    public void capacity() {
        interceptor.setExpectation(getObjectType(), objectName, "capacity");
        ringBuffer.capacity();
    }

    @Test
    public void size() {
        interceptor.setExpectation(getObjectType(), objectName, "size");
        ringBuffer.size();
    }

    @Test
    public void tailSequence() {
        interceptor.setExpectation(getObjectType(), objectName, "tailSequence");
        ringBuffer.tailSequence();
    }

    @Test
    public void headSequence() {
        interceptor.setExpectation(getObjectType(), objectName, "headSequence");
        ringBuffer.headSequence();
    }

    @Test
    public void remainingCapacity() {
        interceptor.setExpectation(getObjectType(), objectName, "remainingCapacity");
        ringBuffer.remainingCapacity();
    }

    @Test
    public void add() {
        String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "add", item);
        ringBuffer.add(item);
    }

    @Test
    public void addAsync() throws ExecutionException, InterruptedException {
        String item = randomString();
        interceptor.setExpectation(getObjectType(), objectName, "addAsync", item, OverflowPolicy.OVERWRITE);
        ringBuffer.addAsync(item, OverflowPolicy.OVERWRITE);
    }

    @Test
    public void readOne() throws InterruptedException {
        long sequence;
        sequence = ringBuffer.add(1);
        interceptor.setExpectation(getObjectType(), objectName, "readOne", sequence);
        ringBuffer.readOne(sequence);
    }

    @Test
    public void addAllAsync() throws ExecutionException, InterruptedException {
        final HashSet item = new HashSet();
        item.add(randomString());
        interceptor.setExpectation(getObjectType(), objectName, "addAllAsync", item, OverflowPolicy.OVERWRITE);
        ringBuffer.addAllAsync(item, OverflowPolicy.OVERWRITE);
    }

    @Test
    public void readManyAsync() throws ExecutionException, InterruptedException {
        long startSequence = ringBuffer.add(1);
        int minCount = 1;
        int maxCount = randomInt(200);

        interceptor.setExpectation(getObjectType(), objectName, "readManyAsync", startSequence, minCount, maxCount, null);
        ringBuffer.readManyAsync(startSequence, minCount, maxCount, null);
    }
}
