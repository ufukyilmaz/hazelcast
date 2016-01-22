package com.hazelcast.client;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.PredicateConfig;
import com.hazelcast.config.QueryCacheConfig;
import com.hazelcast.core.*;
import com.hazelcast.map.QueryCache;
import com.hazelcast.map.listener.EntryUpdatedListener;
import com.hazelcast.query.Predicate;

import java.io.Serializable;
import java.util.Date;
import java.util.Map;



// todo: print stacktrace predicate to see where the event listener is triggered

public class NearCacheReplacement implements Serializable{

    String licenseKey="HazelcastEnterprise#2Nodes#HDMemory:1024GB#7NTklfabBjcuiYm51O3ErU110L000e2029v0gZ1Lg099p1P0LW1s041P920G";
    String mapName = "mapName";
    String cacheName = "cqc";

    public static void main(String[] args) {
        NearCacheReplacement example = new NearCacheReplacement();
        example.run();
    }

    private void run() {
        final HazelcastInstance server = createHazelcastServerInstance();
        final HazelcastInstance client = createHazelcastClientInstance();

        // Add Listener
        IEnterpriseMap<Integer, Integer> clientMap = (IEnterpriseMap) client.getMap(mapName);
        QueryCache<Integer, Integer> queryCache = clientMap.getQueryCache(cacheName);
        queryCache.addEntryListener(new MyEntryUpdatedListener(), true);

        // Run Updater inside Server
        new Thread(new Runnable() {
            @Override
            public void run() {
                while(true){
                    IMap<Object, Object> map = server.getMap(mapName);
                    String date = new Date().toString();
                    map.set("1", date);
                    System.out.println("Putting : " + date);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();

    }

    private HazelcastInstance createHazelcastServerInstance(){
        Config config = new Config();
        config.setLicenseKey(licenseKey);
        return Hazelcast.newHazelcastInstance(config);
    }

    private HazelcastInstance createHazelcastClientInstance(){

        QueryCacheConfig queryCacheConfig = new QueryCacheConfig(cacheName);

        // No need to pre populate the CQC.
        queryCacheConfig.setPopulate(true);
        queryCacheConfig.setIncludeValue(true);
        queryCacheConfig.setCoalesce(false);

        // Create a predicate to allow everything.
        PredicateConfig predicateConfig = new PredicateConfig().setImplementation(new Predicate() {
            @Override
            public boolean apply(Map.Entry entry) {
//                try{
//                    throw new Exception("key:"+entry.getKey()+" value:"+entry.getValue());
//                }catch (Exception e){
//                    e.printStackTrace();
//                }
                return true;
            }
        });
        queryCacheConfig.setPredicateConfig(predicateConfig);

        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setLicenseKey(licenseKey);
        clientConfig.addQueryCacheConfig(mapName, queryCacheConfig);

        return HazelcastClient.newHazelcastClient(clientConfig);
    }


    private static class MyEntryUpdatedListener implements EntryUpdatedListener<String,String> {
        @Override
        public void entryUpdated(EntryEvent<String, String> entryEvent) {
            System.out.println("Replaced Value : " + entryEvent.getOldValue());
            System.out.println("New Value : " + entryEvent.getValue());
        }
    }
}