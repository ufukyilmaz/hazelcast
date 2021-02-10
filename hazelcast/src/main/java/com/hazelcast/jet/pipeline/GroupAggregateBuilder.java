package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.aggregate.AggregateOperation1;
import com.hazelcast.jet.datamodel.ItemsByTag;
import com.hazelcast.jet.datamodel.Tag;

import javax.annotation.Nonnull;
import java.util.Map;

public interface GroupAggregateBuilder<K, R0> {

    /**
     * Returns the tag corresponding to the pipeline stage this builder
     * was obtained from. Use this tag to refer to this stage when building
     * the {@code AggregateOperation} that you'll pass to {@link #build
     * build(aggrOp)}.
     */
    @Nonnull
    public Tag<R0> tag0();

    /**
     * Adds another stage that will contribute its data to the aggregate
     * operation to be performed. Returns the tag you'll use to refer to this
     * stage when building the {@code AggregateOperation} that you'll pass to
     * {@link #build build()}.
     */
    @Nonnull
    public <T, R> Tag<R> add(
            @Nonnull BatchStageWithKey<T, K> stage,
            @Nonnull AggregateOperation1<? super T, ?, ? extends R> aggrOp
    );

    /**
     * Creates and returns a pipeline stage that performs the co-aggregation
     * of the stages registered with this builder object and emits a {@code
     * Map.Entry(key, resultsByTag)} for each distinct key. The composite
     * aggregate operation places the results of the individual aggregate
     * operations in an {@code ItemsByTag}. Use the tags you got from this
     * builder to access the results.
     *
     * @return a new stage representing the co-aggregation
     */
    @Nonnull
    public BatchStage<Map.Entry<K, ItemsByTag>> build();
}
