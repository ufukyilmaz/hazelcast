//package com.hazelcast.client.standalone.apple.test;
//
//import com.hazelcast.cache.CacheStats;
//import com.hazelcast.cache.ICache;
//import com.hazelcast.client.HazelcastClient;
//import com.hazelcast.client.cache.HazelcastClientCachingProvider;
//import com.hazelcast.client.config.ClientConfig;
//import com.hazelcast.client.config.UrlXmlClientConfig;
//import com.hazelcast.core.HazelcastInstance;
//import com.hazelcast.logging.ILogger;
//import com.hazelcast.logging.Logger;
//import com.hazelcast.monitor.NearCacheStats;
//
//import java.util.concurrent.TimeUnit;
//
//public class HzEngine {
//
//	private static final ILogger logger = Logger.getLogger(HzEngine.class);
//	private static final HzEngine engine = new HzEngine();
//	private HazelcastInstance instance;
//	private HazelcastClientCachingProvider cachingProvider;
//	private ICache<String, String> iCache;
//
//	private HzEngine() {
//		// Do nothing
//	}
//
//	public static HzEngine getInstance() {
//		return engine;
//	}
//
//	@SuppressWarnings("unchecked")
//	public void init(String cacheName) {
//        ClientConfig config;
//        try {
//            config = new UrlXmlClientConfig("http://storage.googleapis.com/hazelcast/hazelcast-client-elastic.xml");
//        } catch (Throwable e) {
//            throw new RuntimeException(e);
//        }
//        instance = HazelcastClient.newHazelcastClient(config);
//		cachingProvider = new HazelcastClientCachingProvider(instance);
//		iCache = cachingProvider.getCacheManager().getCache(cacheName).unwrap(ICache.class);
//	}
//
//	public void put(String key, String value, long ttl) {
////		iCache.put(key, value, ttl, TimeUnit.SECONDS);
//		iCache.putAsync(key, value, ttl, TimeUnit.SECONDS);
//	}
//
//	public void get(String key) {
//		iCache.get(key);
////		iCache.getAsync(key);
//	}
//
//	public void delete(String key) {
//		iCache.remove(key);
////		iCache.removeAsync(key);
//	}
//
//	public void clear() {
//		iCache.clear();
//	}
//
//	public void fetchCacheStats(StringBuilder builder) {
//		CacheStats cacheStats = iCache.getStats();
//		builder.append(" TYPE=CacheStats");
//		if (cacheStats != null) {
//			builder.append(" STATUS=FOUND");
//			builder.append(" AVG_GET_LATENCY=" + cacheStats.getAverageGetLatency());
//			builder.append(" AVG_PUT_LATENCY=" + cacheStats.getAveragePutLatency());
////			builder.append(" AVG_REMOVE_LATENCY=" + cacheStats.getAverageRemoveLatency());
//			builder.append(" PUT_COUNT=" + cacheStats.getPuts());
//			builder.append(" GET_COUNT=" + cacheStats.getGets());
////			builder.append(" DELETE_COUNT=" + cacheStats.getRemoves());
//			builder.append(" HIT_COUNT=" + cacheStats.getHits());
//			builder.append(" MISS_COUNT=" + cacheStats.getMisses());
//		} else {
//			builder.append(" STATUS=NOTFOUND");
//			logger.severe("No CacheStats Data!");
//		}
//	}
//
//	public void fetchNearCacheStats(StringBuilder builder) {
//		CacheStats stats = iCache.getStats();
//		NearCacheStats nearCacheStats = stats == null ? null : stats.getNearCacheStats();
//		builder.append(" TYPE=NearCacheStats");
//		if (nearCacheStats != null) {
//			builder.append(" STATUS=FOUND");
//			builder.append(" NC_HIT_COUNT=" + nearCacheStats.getHits());
//			builder.append(" NC_MISS_COUNT=" + nearCacheStats.getMisses());
//			builder.append(" NC_OWNED_ENTRY_MEMORY=" + nearCacheStats.getOwnedEntryMemoryCost());
//			builder.append(" NC_OWNED_ENTRY_COUNT=" + nearCacheStats.getOwnedEntryCount());
//		} else {
//			builder.append(" STATUS=NOTFOUND");
//			logger.severe("No NearCacheStats Data!");
//		}
//	}
//
//	public void release() {
//		if (cachingProvider != null) {
//			cachingProvider.close();
//		}
//		if (instance != null) {
//			instance.shutdown();
//		}
//	}
//
//	public HazelcastInstance getHazelcastInstance() {
//		return instance;
//	}
//}
