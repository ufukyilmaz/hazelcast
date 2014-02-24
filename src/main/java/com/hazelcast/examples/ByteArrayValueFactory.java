package com.hazelcast.examples;

import java.util.Random;

/**
 * @author mdogan 30/12/13
 */
public abstract class ByteArrayValueFactory extends BaseValueFactory<byte[]> {

    public static ValueFactory<byte[]> newInstance(boolean cached) {
        System.err.println("Using ByteArrayValueFactory...");
        return cached ? new CachedValueFactory() : new NewValueFactory();
    }

    private static class NewValueFactory implements ValueFactory<byte[]> {
        public byte[] newValue(long seed) {
            return createRandomValue(seed);
        }
    }

    private static class CachedValueFactory implements ValueFactory<byte[]> {
        final byte[][] values = new byte[100][];

        private CachedValueFactory() {
            Random rand = new Random();
            for (int i = 0; i < values.length; i++) {
                values[i] = createRandomValue(rand.nextInt(999999999));
            }
        }

        public byte[] newValue(long longSeed) {
            int seed = Math.abs(forcedCast(longSeed));
            if (seed < 0) seed = 500;
            return values[seed % values.length];
        }
    }

    private static byte[] createRandomValue(long longSeed) {
        int seed = Math.abs(forcedCast(longSeed));
        int size = seed % 1000;
        return new byte[size];
    }

}
