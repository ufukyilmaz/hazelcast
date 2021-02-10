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

package com.hazelcast.jet;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

public interface JobStateSnapshot {

    /**
     * Returns the snapshot name. This is the name that was given to {@link
     * Job#exportSnapshot(String)}.
     */
    @Nonnull
    String name();

    /**
     * Returns the time the snapshot was created.
     */
    long creationTime();

    /**
     * Returns the job ID of the job the snapshot was originally exported from.
     */
    long jobId();

    /**
     * Returns the job name of the job the snapshot was originally exported
     * from.
     */
    @Nullable
    String jobName();

    /**
     * Returns the size in bytes of the payload data of the state snapshot.
     * Doesn't include storage overhead and especially doesn't account for
     * backup copies.
     */
    long payloadSize();

    /**
     * Returns the JSON representation of the DAG of the job this snapshot was
     * created from.
     */
    @Nonnull
    String dagJsonString();

    /**
     * Destroy the underlying distributed object.
     */
    void destroy();
}
