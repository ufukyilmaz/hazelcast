package com.hazelcast.examples;

import java.util.Random;

/**
 * @author mdogan 30/12/13
 */
public abstract class SampleValueFactory extends BaseValueFactory<SampleValue> {

    public static ValueFactory<SampleValue> newInstance(boolean cached) {
        System.err.println("Using SampleValueFactory...");
        return cached ? new CachedValueFactory() : new NewValueFactory();
    }

    private static class NewValueFactory implements ValueFactory<SampleValue> {
        public SampleValue newValue(long seed) {
            return createRandomValue(seed);
        }
    }

    private static class CachedValueFactory implements ValueFactory<SampleValue> {
        final SampleValue[] values = new SampleValue[100];

        private CachedValueFactory() {
            Random rand = new Random();
            for (int i = 0; i < values.length; i++) {
                values[i] = createRandomValue(rand.nextInt(999999999));
            }
        }

        public SampleValue newValue(long longSeed) {
            int seed = Math.abs(forcedCast(longSeed));
            return values[seed % values.length];
        }
    }

    private static SampleValue createRandomValue(long longSeed) {
        int seed = Math.abs(forcedCast(longSeed));
        int loop = seed % 100;
        long[] ll = new long[loop];
        for (int j = 0; j < ll.length; j++) {
            ll[j] = System.currentTimeMillis();
        }
        int id = seed;
        String s = "Some string for value object[" + id + "]";
        loop = seed % 50;
        for (int j = 0; j < loop; j++) {
            s += " + additional string value " + j;
        }
        return new SampleValue(id, longSeed, s, ll);
    }
}
