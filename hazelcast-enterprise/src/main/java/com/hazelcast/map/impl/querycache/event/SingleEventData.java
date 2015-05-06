package com.hazelcast.map.impl.querycache.event;

import com.hazelcast.map.impl.EventData;
import com.hazelcast.map.impl.querycache.event.sequence.Sequenced;
import com.hazelcast.nio.serialization.Data;
import com.hazelcast.nio.serialization.SerializationService;

/**
 * Event data contract which is sent to subscriber side.
 */
public interface SingleEventData extends Sequenced, EventData {

    Object getKey();

    Object getValue();

    Data getDataKey();

    Data getDataNewValue();

    Data getDataOldValue();

    long getCreationTime();

    void setSerializationService(SerializationService serializationService);
}
