package com.hazelcast.jet.pipeline;

import com.hazelcast.jet.datamodel.Tag;


public interface GeneralHashJoinBuilder<T0> {

    /**
     * Adds another contributing pipeline stage to the hash-join operation.
     *
     * @param stage the contributing stage
     * @param joinClause specifies how to join the contributing stage
     * @param <K> the type of the join key
     * @param <T1_IN> the type of the contributing stage's data
     * @param <T1> the type of result after applying the projecting transformation
     *             to the contributing stage's data
     * @return the tag that refers to the contributing stage
     */
    <K, T1_IN, T1> Tag<T1> add(BatchStage<T1_IN> stage, JoinClause<K, T0, T1_IN, T1> joinClause);

    /**
     * Adds another contributing pipeline stage to the hash-join operation.
     *
     * If no matching items for returned {@linkplain Tag tag} is found, no
     * records for given key will be added.
     *
     * @param stage the contributing stage
     * @param joinClause specifies how to join the contributing stage
     * @param <K> the type of the join key
     * @param <T1_IN> the type of the contributing stage's data
     * @param <T1> the type of result after applying the projecting transformation
     *             to the contributing stage's data
     * @return the tag that refers to the contributing stage
     * @since 4.1
     */
    <K, T1_IN, T1> Tag<T1> addInner(BatchStage<T1_IN> stage, JoinClause<K, T0, T1_IN, T1> joinClause);
}
