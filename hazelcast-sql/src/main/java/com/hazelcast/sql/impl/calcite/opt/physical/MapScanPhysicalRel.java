/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.sql.impl.calcite.opt.physical;

import com.hazelcast.sql.impl.calcite.opt.physical.visitor.PhysicalRelVisitor;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * Physical scan over partitioned map.
 * <p>
 * Traits:
 * <ul>
 *     <li><b>Collation</b>: empty, as map is not sorted</li>
 *     <li><b>Distribution</b>: PARTITIONED or REPLICATED depending on the map type</li>
 * </ul>
 */
public class MapScanPhysicalRel extends AbstractMapScanPhysicalRel {
    public MapScanPhysicalRel(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelOptTable table
    ) {
        super(cluster, traitSet, table);
    }

    @Override
    public final RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MapScanPhysicalRel(getCluster(), traitSet, getTable());
    }

    @Override
    public void visit(PhysicalRelVisitor visitor) {
        visitor.onMapScan(this);
    }
}
