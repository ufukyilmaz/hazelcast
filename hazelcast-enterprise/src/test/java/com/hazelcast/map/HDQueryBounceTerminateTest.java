package com.hazelcast.map;

import com.hazelcast.config.Config;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.internal.elastic.tree.impl.RedBlackTreeStore;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.query.Predicate;
import com.hazelcast.query.impl.Index;
import com.hazelcast.query.impl.QueryContext;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.query.impl.predicates.AbstractIndexAwarePredicate;
import com.hazelcast.test.annotation.SlowTest;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Set;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(SlowTest.class)
public class HDQueryBounceTerminateTest extends HDQueryBounceTest {

    private static boolean rebBlackTreeStoreAssertionStatus;

    @BeforeClass
    public static void beforeClass() {
        // disable expensive consistency checks in RedBlackTreeStore
        rebBlackTreeStoreAssertionStatus = RedBlackTreeStore.class.desiredAssertionStatus();
        RedBlackTreeStore.class.getClassLoader().setClassAssertionStatus(RedBlackTreeStore.class.getName(), false);
    }

    @AfterClass
    public static void afterClass() {
        // re-enable consistency checks in RedBlackTreeStore
        RedBlackTreeStore.class.getClassLoader().setClassAssertionStatus(RedBlackTreeStore.class.getName(),
                rebBlackTreeStoreAssertionStatus);
    }

    @Override
    protected Config getConfig() {
        Config config = super.getConfig();
        config.getSerializationConfig().addDataSerializableFactory(42, typeId -> new PredicateImpl());
        return config;
    }

    @Override
    protected boolean useTerminate() {
        return true;
    }

    @Override
    protected Predicate makePredicate(String attribute, int min, int max, boolean withIndexes) {
        if (withIndexes) {
            return new PredicateImpl(attribute, min, max);
        } else {
            return super.makePredicate(attribute, min, max, false);
        }
    }

    public static class PredicateImpl extends AbstractIndexAwarePredicate {

        private int from;
        private int to;

        public PredicateImpl() {
        }

        public PredicateImpl(String attribute, int from, int to) {
            super(attribute);
            this.from = from;
            this.to = to;
        }

        @Override
        public int getFactoryId() {
            return 42;
        }

        @Override
        public int getClassId() {
            return 0;
        }

        @Override
        public Set<QueryableEntry> filter(QueryContext queryContext, int ownedPartitionCount) {
            Index index = matchIndex(queryContext, QueryContext.IndexMatchHint.PREFER_ORDERED, ownedPartitionCount);
            return index.getRecords(from, true, to, false);
        }

        @Override
        protected boolean applyForSingleAttributeValue(Comparable attributeValue) {
            throw new IllegalStateException("indexed HD predicates should be always evaluated using indexes");
        }

        @Override
        public void writeData(ObjectDataOutput out) throws IOException {
            super.writeData(out);
            out.writeObject(to);
            out.writeObject(from);
        }

        @Override
        public void readData(ObjectDataInput in) throws IOException {
            super.readData(in);
            to = in.readObject();
            from = in.readObject();
        }

    }

}
