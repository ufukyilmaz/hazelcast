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

import com.hazelcast.core.HazelcastInstance;

import com.hazelcast.jet.config.JetConfig;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.jet.core.DAG;
import com.hazelcast.jet.impl.AbstractJetInstance;
import com.hazelcast.jet.impl.JobRepository;
import com.hazelcast.jet.impl.SnapshotValidationRecord;
import com.hazelcast.jet.pipeline.Pipeline;
import com.hazelcast.map.IMap;
import com.hazelcast.map.impl.MapService;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.Collection;
import java.util.List;

import static com.hazelcast.jet.impl.JobRepository.exportedSnapshotMapName;
import static java.util.stream.Collectors.toList;

/**
 * Represents either an instance of a
 * instance that connects to a remote cluster.
 * @since 5.0
 */
public interface JetService {
    /**
     * Returns the underlying Hazelcast IMDG instance used by Jet. It will
     * be either a server node or a client, depending on the type of this
     * {@code JetInstance}.
     */
    @Nonnull
    HazelcastInstance getHazelcastInstance();


    /**
     * Returns the configuration for this Jet member. This method is not
     * available on client instances.
     */
    @Nonnull
    JetConfig getConfig();
    /**
     * Creates and returns a Jet job based on the supplied DAG. Jet will
     * asynchronously start executing the job.
     */
    @Nonnull
    default Job newJob(@Nonnull DAG dag) {
        return newJob(dag, new JobConfig());
    }

    /**
     * Creates and returns an executable job based on the supplied pipeline.
     * Jet will asynchronously start executing the job.
     */
    @Nonnull
    default Job newJob(@Nonnull Pipeline pipeline) {
        return newJob(pipeline, new JobConfig());
    }

    /**
     * Creates and returns a Jet job based on the supplied DAG and job
     * configuration. Jet will asynchronously start executing the job.
     * <p>
     * If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name, in which case it throws {@link
     * JobAlreadyExistsException}. Job is active if it is running,
     * suspended or waiting to be run; that is it has not completed or failed.
     * Thus there can be at most one active job with a given name at a time and
     * you can re-use the job name after the previous job completed.
     * <p>
     * See also {@link #newJobIfAbsent}.
     *
     * @throws JobAlreadyExistsException if there is an active job with
     *      an equal name
     */
    @Nonnull
    Job newJob(@Nonnull DAG dag, @Nonnull JobConfig config);

    /**
     * Creates and returns a Jet job based on the supplied pipeline and job
     * configuration. Jet will asynchronously start executing the job.
     * <p>
     * If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name, in which case it throws {@link
     * JobAlreadyExistsException}. Job is active if it is running,
     * suspended or waiting to be run; that is it has not completed or failed.
     * Thus there can be at most one active job with a given name at a time and
     * you can re-use the job name after the previous job completed.
     * <p>
     * See also {@link #newJobIfAbsent}.
     *
     * @throws JobAlreadyExistsException if there is an active job with
     *      an equal name
     */
    @Nonnull
    Job newJob(@Nonnull Pipeline pipeline, @Nonnull JobConfig config);

    /**
     * Creates and returns a Jet job based on the supplied DAG and job
     * configuration. Jet will asynchronously start executing the job.
     * <p>
     * If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name. If there is, it will join that job instead
     * of submitting a new one. Job is active if it is running, suspended or
     * waiting to be run; that is it has not completed or failed. In other
     * words, this method ensures that the job with this name is running and is
     * not running multiple times in parallel.
     * <p>
     * This method is useful for microservices deployment when each package
     * contains a jet member and the job and you want the job to run only once.
     * But if the job is a batch job and runs very quickly, it can happen that
     * it executes multiple times, because the job name can be reused after a
     * previous execution completed.
     * <p>
     * If the job name is null, a new job is always submitted.
     * <p>
     * See also {@link #newJob}.
     */
    @Nonnull
    Job newJobIfAbsent(@Nonnull DAG dag, @Nonnull JobConfig config);

    /**
     * Creates and returns a Jet job based on the supplied pipeline and job
     * configuration. Jet will asynchronously start executing the job.
     * <p>
     * If the name in the JobConfig is non-null, Jet checks if there is an
     * active job with equal name. If there is, it will join that job instead
     * of submitting a new one. Job is active if it is running, suspended or
     * waiting to be run; that is it has not completed or failed. In other
     * words, this method ensures that the job with this name is running and is
     * not running multiple times in parallel.
     * <p>
     * This method is useful for microservices deployment when each package
     * contains a jet member and the job and you want the job to run only once.
     * But if the job is a batch job and runs very quickly, it can happen that
     * it executes multiple times, because the job name can be reused after a
     * previous execution completed.
     * <p>
     * If the job name is null, a new job is always submitted.
     * <p>
     * See also {@link #newJob}.
     */
    @Nonnull
    Job newJobIfAbsent(@Nonnull Pipeline pipeline, @Nonnull JobConfig config);

    /**
     * Submits a light job for execution. This kind of job is focused on
     * reducing the job startup and teardown time: only a single operation is
     * used to deploy the job instead of 2 for normal jobs.
     *
     * Limitation of light jobs:
     * <ul>
     *     <li>no job configuration, that means no processing guarantee, no custom
     *         classes or job resources
     *
     *     <li>no metrics after job completion
     *
     *     <li>no visibility in {@link #getJobs()} or in Management Center (this
     *         will be added later)
     *
     *     <li>failures will be only reported to the caller and logged in the
     *         cluster logs, but no trace of the job will remain in the cluster after
     *         it's done
     *
     *     <li>{@link RestartableException} doesn't restart the job, but it will
     *         fail
     * </ul>
     * <p>
     * It substantially reduces the overhead for jobs that take milliseconds to
     * complete.
     * <p>
     * A light job will not be cancelled if the client disconnects. It's
     * potential failure will be only logged in member logs.
     */
    @Nonnull
    LightJob newLightJob(Pipeline p);

    /**
     * Submits a job defined in the Core API.
     * <p>
     * See {@link #newLightJob(Pipeline)}.
     */
    @Nonnull
    LightJob newLightJob(DAG dag);

    /**
     * Returns all submitted jobs including running and completed ones.
     */
    @Nonnull
    List<Job> getJobs();

    /**
     * Returns the job with the given id or {@code null} if no such job could
     * be found.
     */
    @Nullable
    Job getJob(long jobId);

    /**
     * Returns all jobs submitted with the given name, ordered in descending
     * order by submission time. The active job is always first. Empty list
     * will be returned if no job with the given name exists. The list includes
     * completed jobs.
     */
    @Nonnull
    List<Job> getJobs(@Nonnull String name);

    /**
     * Returns the active or last submitted job with the given name or {@code
     * null} if no such job could be found. The returned job can be already
     * completed.
     */
    @Nullable
    default Job getJob(@Nonnull String name) {
        return getJobs(name).stream().findFirst().orElse(null);
    }

    /**
     * Returns the {@link JobStateSnapshot} object representing an exported
     * snapshot with the given name. Returns {@code null} if no such snapshot
     * exists.
     */
    @Nullable
    default JobStateSnapshot getJobStateSnapshot(@Nonnull String name) {
        String mapName = exportedSnapshotMapName(name);
        if (!((AbstractJetInstance) this).existsDistributedObject(MapService.SERVICE_NAME, mapName)) {
            return null;
        }
        IMap<Object, Object> map = getHazelcastInstance().getMap(mapName);
        Object validationRecord = map.get(SnapshotValidationRecord.KEY);
        if (validationRecord instanceof SnapshotValidationRecord) {
            // update the cache - for robustness. For example after the map was copied
            getHazelcastInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE).set(name, validationRecord);
            return new JobStateSnapshot(getHazelcastInstance(), name, (SnapshotValidationRecord) validationRecord);
        } else {
            return null;
        }
    }

    /**
     * Returns the collection of exported job state snapshots stored in the
     * cluster.
     */
    @Nonnull
    default Collection<JobStateSnapshot> getJobStateSnapshots() {
        return getHazelcastInstance().getMap(JobRepository.EXPORTED_SNAPSHOTS_DETAIL_CACHE)
                .entrySet().stream()
                .map(entry -> new JobStateSnapshot(getHazelcastInstance(), (String) entry.getKey(),
                        (SnapshotValidationRecord) entry.getValue()))
                .collect(toList());
    }
}
