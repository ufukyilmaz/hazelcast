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

package com.hazelcast.client.impl.protocol.template;

import com.hazelcast.annotation.EventResponse;
import com.hazelcast.annotation.GenerateCodec;
import com.hazelcast.client.impl.protocol.EventMessageConst;
import com.hazelcast.map.impl.querycache.event.SingleEventData;

import java.util.Collection;

@GenerateCodec(id = 0, name = "Event", ns = "")
public interface EnterpriseEventResponseTemplate {

    @EventResponse(EventMessageConst.EVENT_QUERYCACHESINGLE)
    void QueryCacheSingle(SingleEventData data);

    @EventResponse(EventMessageConst.EVENT_QUERYCACHEBATCH)
    void QueryCacheBatch(Collection<SingleEventData> events, String source, int partitionId);
}
