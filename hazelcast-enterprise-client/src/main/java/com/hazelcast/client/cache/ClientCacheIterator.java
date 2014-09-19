//package com.hazelcast.client.cache;
//
//import com.hazelcast.cache.AbstractCacheIterator;
//import com.hazelcast.cache.CacheIterationResult;
//import com.hazelcast.cache.ICache;
//import com.hazelcast.cache.client.CacheIterateRequest;
//import com.hazelcast.client.spi.ClientContext;
//import com.hazelcast.client.spi.ClientInvocationService;
//import com.hazelcast.core.ICompletableFuture;
//import com.hazelcast.nio.serialization.SerializationService;
//import com.hazelcast.util.ExceptionUtil;
//
///**
// * @author mdogan 15/05/14
// */
//public class ClientCacheIterator<K, V> extends AbstractCacheIterator<K, V> {
//
//    private final ClientContext context;
//
//    public ClientCacheIterator(ICache<K, V> cache, int batchCount, ClientContext context) {
//        super(cache, batchCount);
//        this.context = context;
//    }
//
//    protected CacheIterationResult fetch() {
//        CacheIterateRequest request = new CacheIterateRequest(cache.getName(), partitionId, lastSlot, batchCount);
//        ClientInvocationService invocationService = context.getInvocationService();
//        try {
//            ICompletableFuture<Object> f = invocationService
//                    .invokeOnPartitionOwner(request, partitionId);
//            return getSerializationService().toObject(f.get());
//        } catch (Exception e) {
//            throw ExceptionUtil.rethrow(e);
//        }
//    }
//
//    @Override
//    protected int getPartitionCount() {
//        return context.getPartitionService().getPartitionCount();
//    }
//
//    @Override
//    protected SerializationService getSerializationService() {
//        return context.getSerializationService();
//    }
//}
