/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.jet.pipeline;

import com.hazelcast.function.BiFunctionEx;
import com.hazelcast.jet.core.Processor;
import com.hazelcast.jet.datamodel.ItemsByTag;

public interface StreamHashJoinBuilder<T0> extends GeneralHashJoinBuilder<T0> {

    /**
     * Builds a new pipeline stage that performs the hash-join operation. Attaches
     * the stage to all the contributing stages.
     *
     * @param mapToOutputFn the function to map the output item. It must be
     *     stateless and {@linkplain Processor#isCooperative() cooperative}.
     * @return the new hash-join pipeline stage
     */
    <R> StreamStage<R> build(BiFunctionEx<T0, ItemsByTag, R> mapToOutputFn);

}
