package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.MapServiceContext;
import com.hazelcast.map.impl.PartitionContainer;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorHandler;
import com.hazelcast.map.impl.querycache.accumulator.AccumulatorInfo;
import com.hazelcast.map.impl.querycache.event.DefaultSingleEventData;
import com.hazelcast.map.impl.querycache.event.SingleEventData;
import com.hazelcast.map.impl.querycache.event.sequence.PartitionSequencer;
import com.hazelcast.map.impl.querycache.publisher.MapPublisherRegistry;
import com.hazelcast.map.impl.querycache.publisher.PartitionAccumulatorRegistry;
import com.hazelcast.map.impl.querycache.publisher.PublisherContext;
import com.hazelcast.map.impl.querycache.publisher.PublisherRegistry;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.SerializationService;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.util.ExceptionUtil;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Includes enterprise specific {@link com.hazelcast.map.QueryCache QueryCache} specific
 * additions to {@link MapReplicationOperation}.
 * <p/>
 * Mainly carries events in {@link Accumulator} to the replica.
 */
public class EnterpriseMapReplicationOperation extends MapReplicationOperation {

    private transient SerializationHandler handler;
    private transient MapService mapService;
    private transient int partitionId;
    private transient List artifacts;

    public EnterpriseMapReplicationOperation() {
        super();
    }

    public EnterpriseMapReplicationOperation(MapService mapService, PartitionContainer container,
                                             int partitionId, int replicaIndex) {
        super(mapService, container, partitionId, replicaIndex);
        this.handler = new SerializationHandler();
        this.mapService = mapService;
        this.partitionId = partitionId;

    }

    @Override
    public void run() {
        super.run();
        populateAccumulators();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        handler.setOut(out);
        int accumulatorCountToBeReplicated = getAccumulatorCountToBeReplicated();

        out.writeInt(accumulatorCountToBeReplicated);

        Collection<PublisherRegistry> publisherRegistries = getPublisherRegistries();
        for (PublisherRegistry publisherRegistry : publisherRegistries) {
            Map<String, PartitionAccumulatorRegistry> allPublisherRegistry = publisherRegistry.getAll();
            Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = allPublisherRegistry.values();
            for (PartitionAccumulatorRegistry accumulatorRegistry : partitionAccumulatorRegistries) {
                Map<Integer, Accumulator> partitionToAccumulator = accumulatorRegistry.getAll();
                Accumulator accumulator = partitionToAccumulator.get(partitionId);
                if (accumulator == null) {
                    continue;
                }

                AccumulatorInfo info = accumulator.getInfo();
                String mapName = info.getMapName();
                String cacheName = info.getCacheName();
                int size = accumulator.size();

                out.writeUTF(mapName);
                out.writeUTF(cacheName);
                out.writeLong(getSequence(accumulator));
                out.writeInt(size);
                accumulator.poll(handler, 0, TimeUnit.SECONDS);
            }
        }


    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);

        artifacts = new ArrayList();
        int replicatedAccumulatorCount = in.readInt();

        artifacts.add(replicatedAccumulatorCount);

        for (int i = 0; i < replicatedAccumulatorCount; i++) {
            String mapName = in.readUTF();
            artifacts.add(mapName);
            String cacheName = in.readUTF();
            artifacts.add(cacheName);
            long sequence = in.readLong();
            artifacts.add(sequence);
            int accumulatorSize = in.readInt();
            artifacts.add(accumulatorSize);
            for (int j = 0; j < accumulatorSize; j++) {
                DefaultSingleEventData eventData = new DefaultSingleEventData();
                try {
                    eventData.readData(in);
                } catch (Throwable e) {
                    e.printStackTrace();
                }
                artifacts.add(eventData);
            }
        }
    }

    private void populateAccumulators() {
        int partitionId = getPartitionId();
        MapService mapService = (MapService) getService();
        MapPublisherRegistry mapPublisherRegistry = getMapPublisherRegistry(mapService);
        SerializationService serializationService = getSerializationService(mapService);

        int i = 0;
        Integer replicatedAccumulatorCount = (Integer) artifacts.get(i++);
        int processedAccumulatorCount = 0;

        while (true) {
            if (processedAccumulatorCount == replicatedAccumulatorCount) {
                break;
            }
            processedAccumulatorCount++;
            String mapName = (String) artifacts.get(i++);
            String cacheName = (String) artifacts.get(i++);
            Long sequence = (Long) artifacts.get(i++);
            Integer accumulatorSize = (Integer) artifacts.get(i++);

            // post join operations should create PublisherRegistry before replication.
            PublisherRegistry publisherRegistry = mapPublisherRegistry.getOrNull(mapName);
            if (publisherRegistry == null) {
                continue;
            }

            PartitionAccumulatorRegistry accumulatorRegistry = publisherRegistry.getOrCreate(cacheName);
            Accumulator accumulator = accumulatorRegistry.getOrCreate(partitionId);
            PartitionSequencer partitionSequencer = accumulator.getPartitionSequencer();
            // do this subtraction from sequence, since accumulator will give right sequence to event data.
            partitionSequencer.setSequence(sequence - accumulatorSize);

            for (int j = 0; j < accumulatorSize; j++) {
                SingleEventData eventData = (SingleEventData) artifacts.get(i++);
                eventData.setSerializationService(serializationService);

                accumulator.accumulate(eventData);
            }
        }
    }

    private SerializationService getSerializationService(MapService mapService) {
        MapServiceContext mapServiceContext = mapService.getMapServiceContext();
        NodeEngine nodeEngine = mapServiceContext.getNodeEngine();
        return nodeEngine.getSerializationService();
    }

    private Collection<PublisherRegistry> getPublisherRegistries() {
        MapPublisherRegistry mapPublisherRegistry = getMapPublisherRegistry(mapService);
        Map<String, PublisherRegistry> cachesOfMaps = mapPublisherRegistry.getAll();
        return cachesOfMaps.values();
    }

    private int getAccumulatorCountToBeReplicated() throws IOException {
        int count = 0;
        Collection<PublisherRegistry> publisherRegistries = getPublisherRegistries();
        for (PublisherRegistry publisherRegistry : publisherRegistries) {
            Collection<PartitionAccumulatorRegistry> partitionAccumulatorRegistries = publisherRegistry.getAll().values();
            for (PartitionAccumulatorRegistry accumulatorRegistry : partitionAccumulatorRegistries) {
                Map<Integer, Accumulator> partitionToAccumulator = accumulatorRegistry.getAll();
                Accumulator accumulator = partitionToAccumulator.get(partitionId);
                if (accumulator == null) {
                    continue;
                }
                count++;
            }
        }
        return count;
    }

    private long getSequence(Accumulator accumulator) {
        return accumulator.getPartitionSequencer().getSequence();
    }

    private MapPublisherRegistry getMapPublisherRegistry(MapService mapService) {
        EnterpriseMapServiceContext enterpriseMapServiceContext
                = (EnterpriseMapServiceContext) mapService.getMapServiceContext();
        QueryCacheContext queryCacheContext = enterpriseMapServiceContext.getQueryCacheContext();
        PublisherContext publisherContext = queryCacheContext.getPublisherContext();
        return publisherContext.getMapPublisherRegistry();
    }

    /**
     * Reads an {@link Accumulator} and serializes the read event immediately.
     */
    private static class SerializationHandler implements AccumulatorHandler<SingleEventData> {

        private ObjectDataOutput out;

        public SerializationHandler() {
        }

        public void setOut(ObjectDataOutput out) {
            this.out = out;
        }

        @Override
        public void handle(SingleEventData eventData, boolean lastElement) {
            try {
                eventData.writeData(out);
            } catch (IOException e) {
                throw ExceptionUtil.rethrow(e);
            }
        }

    }
}
