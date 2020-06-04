package com.hazelcast.internal.elastic.util;

import com.hazelcast.core.HazelcastException;
import com.hazelcast.internal.memory.MemoryAllocator;
import com.hazelcast.internal.memory.MemoryBlock;
import com.hazelcast.internal.nio.Disposable;
import com.hazelcast.internal.serialization.EnterpriseSerializationService;
import com.hazelcast.internal.serialization.impl.NativeMemoryData;
import com.hazelcast.internal.serialization.impl.NativeMemoryDataUtil;

/**
 * Helper class to include shared disposal functionality in one place.
 */
public final class DisposalUtil {

    private DisposalUtil() {
    }

    /**
     * Dispose a single object
     */
    public static void dispose(EnterpriseSerializationService ess,
                               MemoryAllocator malloc,
                               Object object) {
        if (object == null) {
            return;
        }

        try {
            if (object instanceof Disposable) {
                ((Disposable) object).dispose();
            } else if (object instanceof NativeMemoryData) {
                NativeMemoryDataUtil.dispose(ess, malloc, (NativeMemoryData) object);
            } else if (object instanceof MemoryBlock) {
                NativeMemoryDataUtil.dispose(ess, malloc, (MemoryBlock) object);
            } else {
                throw new IllegalStateException("Unidentifiable object, "
                        + "don't know how to dispose. May cause leaks.");
            }
        } catch (Exception exception) {
            throw new HazelcastException("Could not deallocate memory. "
                    + "There may be a native memory leak!", exception);
        }
    }

    /**
     * Dispose batch of objects
     */
    public static void dispose(EnterpriseSerializationService ess,
                               MemoryAllocator malloc,
                               Object... objects) {
        HazelcastException caught = null;

        for (Object object : objects) {
            try {
                dispose(ess, malloc, object);
            } catch (HazelcastException exception) {
                caught = exception;
            }

            if (caught != null) {
                throw caught;
            }
        }
    }
}
