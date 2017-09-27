package com.hazelcast.enterprise.management;

import com.hazelcast.instance.HazelcastInstanceImpl;
import com.hazelcast.instance.Node;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.internal.management.TimedMemberStateFactory;
import com.hazelcast.monitor.TimedMemberState;
import com.hazelcast.monitor.impl.MemberStateImpl;
import com.hazelcast.monitor.impl.NodeStateImpl;
import com.hazelcast.security.impl.WeakSecretError;
import com.hazelcast.security.impl.WeakSecretsConfigChecker;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A Factory for creating {@link TimedMemberState} instances.
 */
public class EnterpriseTimedMemberStateFactory extends TimedMemberStateFactory {

    private final Map<String, List<String>> weakSecretsReport = new HashMap<String, List<String>>();

    public EnterpriseTimedMemberStateFactory(HazelcastInstanceImpl instance) {
        super(instance);
        evaluateWeakSecretsInConfig();
    }

    @Override
    protected void createNodeState(MemberStateImpl memberState) {
        Node node = instance.node;
        ClusterService cluster = instance.node.clusterService;
        NodeStateImpl nodeState = new NodeStateImpl(cluster.getClusterState(), node.getState(),
                cluster.getClusterVersion(), node.getVersion(), weakSecretsReport);
        memberState.setNodeState(nodeState);
    }

    private void evaluateWeakSecretsInConfig() {
        WeakSecretsConfigChecker weakSecretsConfigChecker = new WeakSecretsConfigChecker(instance.getConfig());
        Map<String, EnumSet<WeakSecretError>> report = weakSecretsConfigChecker.evaluate();
        for (Map.Entry<String, EnumSet<WeakSecretError>> entry : report.entrySet()) {
            List<String> errors = new ArrayList<String>();
            for (WeakSecretError error : entry.getValue()) {
                errors.add(error.name());
            }

            weakSecretsReport.put(entry.getKey(), errors);
        }
    }
}
