package com.hazelcast.map.impl.querycache.subscriber.operation;

import com.hazelcast.map.impl.EnterpriseMapServiceContext;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.operation.MapOperation;
import com.hazelcast.map.impl.querycache.QueryCacheContext;
import com.hazelcast.map.impl.querycache.accumulator.Accumulator;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.spi.PartitionAwareOperation;

import java.io.IOException;

import static com.hazelcast.map.impl.querycache.utils.QueryCacheUtil.getAccumulatorOrNull;
import static com.hazelcast.util.Preconditions.checkHasText;
import static com.hazelcast.util.Preconditions.checkPositive;

/**
 * Sets read cursor of {@code Accumulator} in this partition to the supplied sequence number.
 *
 * @see Accumulator#setHead
 */
public class SetReadCursorOperation extends MapOperation implements PartitionAwareOperation {

    private long sequence;
    private String cacheName;

    private transient boolean result;

    public SetReadCursorOperation() {
    }

    public SetReadCursorOperation(String mapName, String cacheName, long sequence, int ignored) {
        super(checkHasText(mapName, "mapName"));
        checkPositive(sequence, "sequence");

        this.cacheName = checkHasText(cacheName, "cacheName");
        this.sequence = sequence;
    }

    @Override
    public void run() throws Exception {
        this.result = setReadCursor();
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeUTF(cacheName);
        out.writeLong(sequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        cacheName = in.readUTF();
        sequence = in.readLong();
    }

    private boolean setReadCursor() {
        QueryCacheContext context = getContext();
        Accumulator accumulator = getAccumulatorOrNull(context, name, cacheName, getPartitionId());
        if (accumulator == null) {
            return false;
        }
        return accumulator.setHead(sequence);
    }

    private QueryCacheContext getContext() {
        MapService service = (MapService) getService();
        EnterpriseMapServiceContext mapServiceContext = (EnterpriseMapServiceContext) service.getMapServiceContext();
        return mapServiceContext.getQueryCacheContext();
    }
}
