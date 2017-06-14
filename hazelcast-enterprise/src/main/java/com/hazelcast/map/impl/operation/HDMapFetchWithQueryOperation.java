package com.hazelcast.map.impl.operation;

import com.hazelcast.map.impl.query.Query;
import com.hazelcast.map.impl.query.QueryRunner;
import com.hazelcast.map.impl.query.ResultSegment;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;

import java.io.IOException;

/**
 * Fetches by query a batch of {@code fetchSize} items from a single partition ID for a map. The query is run by the query
 * engine which means it supports projections and filtering. The {@code lastTableIndex} denotes the position from which
 * to resume the query on the partition.
 * This is the HD version of the operation for maps configured with {@link com.hazelcast.config.InMemoryFormat#NATIVE} format.
 * For other formats see {@link MapFetchWithQueryOperation}.
 *
 * @since 3.9
 */
public class HDMapFetchWithQueryOperation extends HDMapOperation {
    private Query query;
    private int lastTableIndex;
    private int fetchSize;
    private transient ResultSegment response;

    public HDMapFetchWithQueryOperation() {
    }

    public HDMapFetchWithQueryOperation(String name, int lastTableIndex, int fetchSize, Query query) {
        super(name);
        this.lastTableIndex = lastTableIndex;
        this.fetchSize = fetchSize;
        this.query = query;
    }

    @Override
    protected void runInternal() {
        final QueryRunner runner = mapServiceContext.getMapQueryRunner(query.getMapName());
        response = runner.runPartitionScanQueryOnPartitionChunk(query, getPartitionId(), lastTableIndex, fetchSize);
    }

    @Override
    public Object getResponse() {
        return response;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        lastTableIndex = in.readInt();
        fetchSize = in.readInt();
        query = in.readObject();
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeInt(lastTableIndex);
        out.writeInt(fetchSize);
        out.writeObject(query);
    }

    @Override
    public int getId() {
        return EnterpriseMapDataSerializerHook.FETCH_WITH_QUERY;
    }
}
