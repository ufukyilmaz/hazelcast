package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation;
import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.KeyedWindowResult;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;

public interface WindowGroupAggregateBuilder<K, R0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder was
     * obtained from. Use this tag to refer to this stage when extracting the
     * results from the aggregated stage.
     */
    @Nonnull
    public Tag<R0> tag0();

    /**
     * Adds another stage that will contribute its data to the windowed
     * group-and-aggregate stage being constructed. Returns the tag you'll use
     * to refer to this stage when building the {@code AggregateOperation} that
     * you'll pass to {@link #build build()}.
     */
    @Nonnull
    public <T, R> Tag<R> add(
            @Nonnull StreamStageWithKey<T, K> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Creates and returns a pipeline stage that performs a windowed
     * cogroup-and-aggregate operation on the stages registered with this
     * builder object. The composite aggregate operation places the results of
     * the individual aggregate operations in an {@code ItemsByTag}. Use the
     * tags you got from this builder to access the results.
     */
    @Nonnull
    public StreamStage<KeyedWindowResult<K, ItemsByTag>> build();
}
