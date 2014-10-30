/*
 * Copyright (c) 2008-2013, Hazelcast, Inc. All Rights Reserved.
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

package com.hazelcast.cache.hidensity;

import com.hazelcast.cache.impl.record.CacheRecordMap;
import com.hazelcast.elasticcollections.SlottableIterator;

/**
 * @param <K> Type of key for cache record stored in this cache record map
 * @param <V> Type of value for cache record stored in this cache record map
 *
 * @author sozal 14/10/14
 */
public interface HiDensityCacheRecordMap<K, V> extends CacheRecordMap<K, V> {

    /**
     * Returns an slottable iterator for this {@link HiDensityCacheRecordMap} to iterate over records.
     *
     * @param slot the slot number (or index) to start the <code>iterator</code>
     * @param <E>  the type of the entry iterated by the <code>iterator</code>
     * @return the slottable iterator for specified <code>slot</code>
     */
    <E> SlottableIterator<E> iterator(int slot);

}
