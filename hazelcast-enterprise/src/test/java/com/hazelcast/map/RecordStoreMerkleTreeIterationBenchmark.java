package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.InMemoryFormat;
import com.hazelcast.config.NativeMemoryConfig.MemoryAllocatorType;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.SampleLicense;
import com.hazelcast.map.impl.MapService;
import com.hazelcast.map.impl.recordstore.RecordStore;
import com.hazelcast.memory.MemorySize;
import com.hazelcast.memory.MemoryUnit;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.infra.Blackhole;
import org.openjdk.jmh.results.format.ResultFormatType;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.Random;

import static com.hazelcast.HDTestSupport.getHDConfig;
import static com.hazelcast.instance.impl.TestUtil.getNode;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

@OutputTimeUnit(MILLISECONDS)
@BenchmarkMode({Mode.AverageTime})
@Warmup(iterations = 5, time = 5, timeUnit = SECONDS)
@Measurement(iterations = 10, time = 5, timeUnit = SECONDS)
@Fork(value = 5, jvmArgsAppend = {"-Xms8G", "-Xmx8G"})
@State(Scope.Benchmark)
public class RecordStoreMerkleTreeIterationBenchmark {
    public static final String MAP_NAME = "map";

    @Benchmark
    public void test_iteration(BenchmarkContext context, Blackhole blackhole) {
        context.recordStore.iterator().forEachRemaining(e -> blackhole.consume(e.hashCode()));
    }

    @State(Scope.Benchmark)
    public static class BenchmarkContext {

        @Param({"10000", "100000"})
        protected int entryCount;

        // keys are always serialized so no point in benchmarking OBJECT format
        @Param({"NATIVE", "BINARY"})
        protected InMemoryFormat inMemoryFormat;

        @Param({"10", "128", "1024"})
        protected int keySize;

        private HazelcastInstance instance;
        private RecordStore recordStore;
        private Random random;

        @Setup(Level.Trial)
        public void setUp() {
            Config config = HazelcastTestSupport.smallInstanceConfig()
                                                .setLicenseKey(SampleLicense.UNLIMITED_LICENSE)
                                                .setProperty(ClusterProperty.PARTITION_COUNT.getName(), "1");
            config.getMapConfig("default").setInMemoryFormat(inMemoryFormat);
            if (inMemoryFormat == InMemoryFormat.NATIVE) {
                config = getHDConfig(config, MemoryAllocatorType.STANDARD, new MemorySize(128, MemoryUnit.MEGABYTES));
            }

            this.instance = Hazelcast.newHazelcastInstance(config);
            this.random = new Random();
            IMap<Object, Object> map = instance.getMap(MAP_NAME);

            byte[] value = new byte[0];
            for (int i = 0; i < entryCount; i++) {
                map.put(generateKey(), value);
            }
            MapService mapService = getNode(instance).nodeEngine.getService(MapService.SERVICE_NAME);

            this.recordStore = mapService.getMapServiceContext().getRecordStore(0, "map");

            System.out.println("Key size: " + getNode(instance).getSerializationService().toData(generateKey()).dataSize()
                    + " bytes, record store size: " + recordStore.size());
        }

        private Object generateKey() {
            byte[] keyBytes = new byte[keySize];
            random.nextBytes(keyBytes);
            return keyBytes;
        }

        @TearDown(Level.Trial)
        public void tearDown() {
            instance.shutdown();
        }
    }


    public static void main(String[] args) throws RunnerException {
        Options opt = new OptionsBuilder()
                .include(RecordStoreMerkleTreeIterationBenchmark.class.getSimpleName())
                .resultFormat(ResultFormatType.JSON)
                //                .addProfiler(GCProfiler.class)
                //                .addProfiler(LinuxPerfProfiler.class)
                //                .addProfiler(HotspotMemoryProfiler.class)
                //                .shouldDoGC(true)
                //                .verbosity(VerboseMode.SILENT)
                .build();

        new Runner(opt).run();
    }

}
