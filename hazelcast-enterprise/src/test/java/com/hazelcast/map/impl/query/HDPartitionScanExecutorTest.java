package com.hazelcast.map.impl.query;

import com.hazelcast.query.Predicate;
import com.hazelcast.query.Predicates;
import com.hazelcast.spi.InvocationBuilder;
import com.hazelcast.spi.Operation;
import com.hazelcast.spi.impl.operationservice.OperationService;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.ParallelTest;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.util.IterationType;
import com.hazelcast.util.executor.CompletedFuture;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.Collection;

import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(HazelcastParallelClassRunner.class)
@Category({QuickTest.class, ParallelTest.class})
public class HDPartitionScanExecutorTest {

    @Rule
    public ExpectedException expected = ExpectedException.none();

    private HDPartitionScanExecutor executor(HDPartitionScanRunner runner) {
        OperationService operationService = mock(OperationService.class);
        InvocationBuilder builder = mock(InvocationBuilder.class);
        when(builder.invoke()).thenReturn(new CompletedFuture<Object>(null, new ArrayList(), null));

        when(operationService.createInvocationBuilder(anyString(), any(Operation.class), anyInt())).thenReturn(builder);
        return new HDPartitionScanExecutor(runner);
    }

    @Test
    public void execute_success() {
        HDPartitionScanRunner runner = mock(HDPartitionScanRunner.class);
        HDPartitionScanExecutor executor = executor(runner);
        Predicate predicate = Predicates.equal("attribute", 1);

        QueryResult queryResult = new QueryResult(IterationType.ENTRY, null, null, Long.MAX_VALUE, false);
        executor.execute("Map", predicate, singletonList(1), queryResult);
        Collection<QueryResultRow> result = queryResult.getRows();
        assertEquals(0, result.size());
    }
}
