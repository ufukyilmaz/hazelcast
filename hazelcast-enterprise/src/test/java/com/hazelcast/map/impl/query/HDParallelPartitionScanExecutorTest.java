package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.query.impl.QueryableEntry;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.executor.CompletedFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDParallelPartitionScanExecutorTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private HDParallelPartitionScanExecutor executor(PartitionScanRunner runner) {
        OperationService operationService = mock(OperationService.class);
        InvocationBuilder builder = mock(InvocationBuilder.class);
        when(builder.invoke()).thenReturn(new CompletedFuture<Object>(null, new ArrayList(), null));

        when(operationService.createInvocationBuilder(anyString(), any(Operation.class), anyInt())).thenReturn(builder);
        return new HDParallelPartitionScanExecutor(runner, operationService, 60000);
    }

    @Test
    public void execute_success() throws Exception {
        PartitionScanRunner runner = mock(PartitionScanRunner.class);
        HDParallelPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        List<QueryableEntry> result = executor.execute("Map", predicate, asList(1, 2, 3));
        assertEquals(0, result.size());
    }

}