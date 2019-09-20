package com.hazelcast.nio.ssl;

import com.hazelcast.internal.networking.Channel;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import javax.net.ssl.SSLEngine;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.atomic.AtomicInteger;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;
import static javax.net.ssl.SSLEngineResult.HandshakeStatus.NEED_WRAP;

/**
 * An executor that executes tasks for TLS handshaking and
 * host verification.
 */
class TLSExecutor {

    private final Executor executor;

    TLSExecutor(Executor executor) {
        this.executor = checkNotNull(executor, "executor can't be null");
    }

    /**
     * Executes handshake tasks.
     *
     * Once the tasks are complete, depending on the state of the
     * SSLEngine, the inbound or outbound pipeline is woken up.
     */
    void executeHandshakeTasks(SSLEngine sslEngine, Channel channel) {
        List<Runnable> tasks = collectTasks(sslEngine);

        AtomicInteger remaining = new AtomicInteger(tasks.size());
        for (Runnable task : tasks) {
            executor.execute(new HandshakeTask(task, remaining, sslEngine, channel));
        }
    }

    void execute(Runnable task) {
        executor.execute(task);
    }

    private List<Runnable> collectTasks(SSLEngine sslEngine) {
        List<Runnable> tasks = new LinkedList<Runnable>();

        Runnable task;
        while ((task = sslEngine.getDelegatedTask()) != null) {
            tasks.add(task);
        }
        return tasks;
    }

    private static class HandshakeTask implements Runnable {
        private final Runnable task;
        private final AtomicInteger remaining;
        private final SSLEngine sslEngine;
        private final Channel channel;

        HandshakeTask(Runnable task, AtomicInteger remaining, SSLEngine sslEngine, Channel channel) {
            this.task = task;
            this.remaining = remaining;
            this.sslEngine = sslEngine;
            this.channel = channel;
        }

        @Override
        public void run() {
            try {
                task.run();
            } catch (Exception e) {
                ILogger logger = Logger.getLogger(HandshakeTask.class);
                logger.warning("Failed to execute handshake task for " + channel, e);
            } finally {
                onTaskCompletion();
            }
        }

        private void onTaskCompletion() {
            if (remaining.decrementAndGet() == 0) {
                if (sslEngine.getHandshakeStatus() == NEED_WRAP) {
                    channel.outboundPipeline().wakeup();
                } else {
                    channel.inboundPipeline().wakeup();
                }
            }
        }
    }
}
