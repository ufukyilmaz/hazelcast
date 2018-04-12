package com.hazelcast.quorum.cardinality;

import com.hazelcast.cardinality.CardinalityEstimator;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.EnterpriseSerialJUnitClassRunner;
import com.hazelcast.quorum.AbstractQuorumCompatibilityTest;
import com.hazelcast.test.annotation.CompatibilityTest;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(EnterpriseSerialJUnitClassRunner.class)
@Category(CompatibilityTest.class)
public class CardinalityEstimatorQuorumCompatibilityTest extends AbstractQuorumCompatibilityTest {

    @Override
    protected void prepareDataStructure(HazelcastInstance previousVersionMember) {
        CardinalityEstimator estimator = previousVersionMember.getCardinalityEstimator(name);
        estimator.add("1");
    }

    @Override
    protected void assertOnCurrentMembers_whilePreviousClusterVersion(HazelcastInstance member) {
        CardinalityEstimator estimator = member.getCardinalityEstimator(name);
        estimator.add("2");
    }

    @Override
    protected void assertOnCurrent_whileQuorumAbsent(HazelcastInstance member) {
        CardinalityEstimator estimator = member.getCardinalityEstimator(name);
        estimator.add("3");
    }

    @Override
    protected void assertOnCurrent_whileQuorumPresent(HazelcastInstance member) {
        CardinalityEstimator estimator = member.getCardinalityEstimator(name);
        estimator.add("4");
    }

    @Override
    protected Config getQuorumProtectedConfig() {
        return getConfigWithQuorum().addCardinalityEstimatorConfig(new CardinalityEstimatorConfig(name).setQuorumName("pq"));
    }
}
