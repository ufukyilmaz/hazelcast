package com.hazelcast.spi.impl.securestore.impl;

import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.executionservice.TaskScheduler;
import com.hazelcast.spi.impl.securestore.SecureStore;

import javax.annotation.Nonnull;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.util.Preconditions.checkNotNull;

abstract class AbstractSecureStore implements SecureStore, Disposable {
    protected final ILogger logger;
    protected final List<EncryptionKeyListener> listeners = new CopyOnWriteArrayList<>();
    private final int pollingInterval;
    private final TaskScheduler scheduler;
    private ScheduledFuture<?> watcher;

    AbstractSecureStore(int pollingInterval, @Nonnull Node node) {
        this.pollingInterval = pollingInterval;
        this.scheduler = node.getNodeEngine().getExecutionService().getTaskScheduler(getClass().getName());
        this.logger = node.getLogger(getClass());
    }

    @Override
    @SuppressWarnings("checkstyle:magicnumber")
    public void addEncryptionKeyListener(@Nonnull EncryptionKeyListener listener) {
        checkNotNull(listener, "Listener cannot be null!");
        listeners.add(listener);
        if (pollingInterval != 0) {
            synchronized (this) {
                if (watcher == null) {
                    watcher = scheduler.scheduleWithRepetition(getWatcher(), 0, pollingInterval, TimeUnit.SECONDS);
                }
            }
        }
    }

    @Override
    public void dispose() {
        synchronized (this) {
            if (watcher != null) {
                watcher.cancel(true);
                watcher = null;
            }
        }
    }

    void notifyEncryptionKeyListeners(@Nonnull byte[] key) {
        for (EncryptionKeyListener listener : listeners) {
            listener.onEncryptionKeyChange(key);
        }
    }

    protected abstract Runnable getWatcher();
}
