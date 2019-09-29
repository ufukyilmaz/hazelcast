package com.hazelcast.internal.hotrestart.impl.testsupport;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.config.NativeMemoryConfig.DEFAULT_METADATA_SPACE_PERCENTAGE;

public abstract class TestProfile {

    static final long[] EMPTY_PREFIXES = {};

    public long exerciseTimeSeconds = 20;
    public int testCycleCount = 2;
    public int prefixCount = 10;
    public int keysetSize = 1024;
    public int hotSetFraction = 4;
    public int logItersHotSetChange = 10;
    public int logMinSize = 7;
    public int sizeIncreaseSteps = 5;
    public int logStepSize = 3;
    public boolean powerLawSizeDistribution = true;
    public int valueOverhead = 1;
    public int clearIntervalSeconds = 6;
    public int offHeapMb = 1024;
    public float offHeapMetadataPercentage = DEFAULT_METADATA_SPACE_PERCENTAGE;
    public int restartCount;

    public int[] hotKeys;
    public byte[] valueData;

    private long hotSetChangeMask;
    private long opCount;
    protected Random rnd = new Random();

    public void build() {
        this.hotSetChangeMask = (1L << logItersHotSetChange) - 1;
        this.valueData = generateValueData(powerLawSizeDistribution
                ? (1 << logMinSize) << (logStepSize * sizeIncreaseSteps)
                : (1 << logMinSize) + (1 << logStepSize) * sizeIncreaseSteps
        );
        this.hotKeys = initialHotKeys();
    }

    public int randomPrefix() {
        return rnd.nextInt(prefixCount) + 1;
    }

    public int randomKey(long iteration) {
        if ((iteration & hotSetChangeMask) == 0) {
            hotKeys[rnd.nextInt(hotKeys.length)] = rnd.nextInt(keysetSize) + 1;
        }
        return hotKeys[rnd.nextInt(hotKeys.length)];
    }

    public byte[] randomValue() {
        return powerLawSizeDistribution ? randomValuePowerLaw() : randomValueUniform();
    }

    public void performOp(MockStoreRegistry reg) {
        performOp(reg, ++opCount);
    }

    protected abstract void performOp(MockStoreRegistry reg, long opCount);

    public abstract long[] prefixesToClear(long lastCleared);

    private byte[] randomValuePowerLaw() {
        int size = 1 << logMinSize;
        for (int i = 0; i < sizeIncreaseSteps && rnd.nextInt(1 << logStepSize) == 0; i++) {
            size <<= logStepSize;
        }
        return buildValue(size);
    }

    private byte[] randomValueUniform() {
        return buildValue((1 << logMinSize) + (1 << logStepSize) * (rnd.nextInt(1 + sizeIncreaseSteps)));
    }

    private int[] initialHotKeys() {
        final int[] hotKeys = new int[Math.max(1, keysetSize / hotSetFraction)];
        for (int i = 0; i < hotKeys.length; i++) {
            hotKeys[i] = rnd.nextInt(keysetSize) + 1;
        }
        return hotKeys;
    }

    private byte[] generateValueData(int maxSize) {
        final byte[] valueData = new byte[maxSize << 1];
        rnd.nextBytes(valueData);
        //for (int i = 0; i < valueData.length; i++) {
        //    valueData[i] = (byte) ('A' + (i % 64));
        //}
        return valueData;
    }

    private byte[] buildValue(int size) {
        final int adjustedSize = size - valueOverhead;
        if (adjustedSize < 1) {
            throw new IllegalArgumentException("(1 << logMinSize) - valueOverhead must be at least 1");
        }
        final byte[] value = new byte[adjustedSize];
        final int dataOff = rnd.nextInt(valueData.length - adjustedSize);
        System.arraycopy(valueData, dataOff, value, 0, value.length);
        return value;
    }

    private void diagnoseSizeDistribution() {
        final Map<Integer, Integer> freqs = new HashMap<Integer, Integer>();
        for (int i = 0; i < 100 * 1000; i++) {
            final int size = randomValue().length;
            final Integer cur = freqs.get(size);
            freqs.put(size, cur == null ? 1 : cur + 1);
        }
        final List<Integer> sizes = new ArrayList<Integer>(freqs.keySet());
        Collections.sort(sizes);
        for (int key : sizes) {
            System.out.format("%,9d x %,9d = %,9d%n", key, freqs.get(key), key * freqs.get(key));
        }
    }

    public static class Default extends TestProfile {

        private final List<Long> prefixes = new ArrayList<Long>();

        @Override
        public void build() {
            super.build();
            for (long i = 0; i < prefixCount; i++) {
                prefixes.add(i + 1);
            }
        }

        @Override
        protected void performOp(MockStoreRegistry reg, long opCount) {
            final int key = randomKey(opCount);
            final int prefix = randomPrefix();
            if (rnd.nextInt(100) < 80) {
                reg.put(prefix, key, randomValue());
            } else {
                reg.remove(prefix, key);
            }
        }

        @Override
        public long[] prefixesToClear(long lastCleared) {
            if (System.nanoTime() - lastCleared <= TimeUnit.SECONDS.toNanos(clearIntervalSeconds)) {
                return EMPTY_PREFIXES;
            }
            Collections.shuffle(prefixes);
            final long[] toClear = new long[2];
            for (int i = 0; i < toClear.length; i++) {
                toClear[i] = prefixes.get(i);
            }
            return toClear;
        }
    }
}
