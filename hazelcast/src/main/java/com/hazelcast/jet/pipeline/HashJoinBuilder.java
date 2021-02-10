package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;

public interface HashJoinBuilder<T0> extends GeneralHashJoinBuilder<T0> {

    /**
     * Builds a new pipeline stage that performs the hash-join operation. Attaches
     * the stage to all the contributing stages.
     * <p>
     * The given function must be stateless and {@linkplain
     * Processor#isCooperative() cooperative}.
     *
     * @return the new hash-join pipeline stage
     */
    <R> BatchStage<R> build(BiFunctionEx<T0, ItemsByTag, R> mapToOutputFn);
}
