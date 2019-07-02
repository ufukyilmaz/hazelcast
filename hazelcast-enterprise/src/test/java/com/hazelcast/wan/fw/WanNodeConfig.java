package com.hazelcast.wan.fw;

import com.hazelcast.config.AdvancedNetworkConfig;
import com.hazelcast.config.AtomicLongConfig;
import com.hazelcast.config.AtomicReferenceConfig;
import com.hazelcast.config.CRDTReplicationConfig;
import com.hazelcast.config.CacheSimpleConfig;
import com.hazelcast.config.CardinalityEstimatorConfig;
import com.hazelcast.config.Config;
import com.hazelcast.config.ConfigPatternMatcher;
import com.hazelcast.config.CountDownLatchConfig;
import com.hazelcast.config.DurableExecutorConfig;
import com.hazelcast.config.EventJournalConfig;
import com.hazelcast.config.ExecutorConfig;
import com.hazelcast.config.FlakeIdGeneratorConfig;
import com.hazelcast.config.GroupConfig;
import com.hazelcast.config.HotRestartPersistenceConfig;
import com.hazelcast.config.ListConfig;
import com.hazelcast.config.ListenerConfig;
import com.hazelcast.config.LockConfig;
import com.hazelcast.config.ManagementCenterConfig;
import com.hazelcast.config.MapConfig;
import com.hazelcast.config.MemberAttributeConfig;
import com.hazelcast.config.MultiMapConfig;
import com.hazelcast.config.NativeMemoryConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.PNCounterConfig;
import com.hazelcast.config.PartitionGroupConfig;
import com.hazelcast.config.QueueConfig;
import com.hazelcast.config.QuorumConfig;
import com.hazelcast.config.ReliableTopicConfig;
import com.hazelcast.config.ReplicatedMapConfig;
import com.hazelcast.config.RingbufferConfig;
import com.hazelcast.config.ScheduledExecutorConfig;
import com.hazelcast.config.SecurityConfig;
import com.hazelcast.config.SemaphoreConfig;
import com.hazelcast.config.SerializationConfig;
import com.hazelcast.config.ServicesConfig;
import com.hazelcast.config.SetConfig;
import com.hazelcast.config.TopicConfig;
import com.hazelcast.config.UserCodeDeploymentConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.config.cp.CPSubsystemConfig;
import com.hazelcast.core.ManagedContext;

import java.io.File;
import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentMap;

/**
 * Extended {@link Config} class for WAN tests. Supports sharing a master
 * config between multiple nodes while overriding some of the configs.
 */
class WanNodeConfig extends Config {
    private final Config wrappedConfig;
    private String instanceName;

    WanNodeConfig(Config wrappedConfig) {
        this.wrappedConfig = wrappedConfig;
        this.instanceName = wrappedConfig.getInstanceName();
    }

    @Override
    public String getInstanceName() {
        return instanceName;
    }

    @Override
    public Config setInstanceName(String instanceName) {
        this.instanceName = instanceName;
        return this;
    }

    @Override
    public ClassLoader getClassLoader() {
        return wrappedConfig.getClassLoader();
    }

    @Override
    public Config setClassLoader(ClassLoader classLoader) {
        return wrappedConfig.setClassLoader(classLoader);
    }

    @Override
    public ConfigPatternMatcher getConfigPatternMatcher() {
        return wrappedConfig.getConfigPatternMatcher();
    }

    @Override
    public void setConfigPatternMatcher(ConfigPatternMatcher configPatternMatcher) {
        wrappedConfig.setConfigPatternMatcher(configPatternMatcher);
    }

    @Override
    public String getProperty(String name) {
        return wrappedConfig.getProperty(name);
    }

    @Override
    public Config setProperty(String name, String value) {
        return wrappedConfig.setProperty(name, value);
    }

    @Override
    public MemberAttributeConfig getMemberAttributeConfig() {
        return wrappedConfig.getMemberAttributeConfig();
    }

    @Override
    public void setMemberAttributeConfig(MemberAttributeConfig memberAttributeConfig) {
        wrappedConfig.setMemberAttributeConfig(memberAttributeConfig);
    }

    @Override
    public Properties getProperties() {
        return wrappedConfig.getProperties();
    }

    @Override
    public Config setProperties(Properties properties) {
        return wrappedConfig.setProperties(properties);
    }

    @Override
    public GroupConfig getGroupConfig() {
        return wrappedConfig.getGroupConfig();
    }

    @Override
    public Config setGroupConfig(GroupConfig groupConfig) {
        return wrappedConfig.setGroupConfig(groupConfig);
    }

    @Override
    public NetworkConfig getNetworkConfig() {
        return wrappedConfig.getNetworkConfig();
    }

    @Override
    public Config setNetworkConfig(NetworkConfig networkConfig) {
        return wrappedConfig.setNetworkConfig(networkConfig);
    }

    @Override
    public MapConfig findMapConfig(String name) {
        return wrappedConfig.findMapConfig(name);
    }

    @Override
    public MapConfig getMapConfigOrNull(String name) {
        return wrappedConfig.getMapConfigOrNull(name);
    }

    @Override
    public MapConfig getMapConfig(String name) {
        return wrappedConfig.getMapConfig(name);
    }

    @Override
    public Config addMapConfig(MapConfig mapConfig) {
        return wrappedConfig.addMapConfig(mapConfig);
    }

    @Override
    public Map<String, MapConfig> getMapConfigs() {
        return wrappedConfig.getMapConfigs();
    }

    @Override
    public Config setMapConfigs(Map<String, MapConfig> mapConfigs) {
        return wrappedConfig.setMapConfigs(mapConfigs);
    }

    @Override
    public CacheSimpleConfig findCacheConfig(String name) {
        return wrappedConfig.findCacheConfig(name);
    }

    @Override
    public CacheSimpleConfig findCacheConfigOrNull(String name) {
        return wrappedConfig.findCacheConfigOrNull(name);
    }

    @Override
    public CacheSimpleConfig getCacheConfig(String name) {
        return wrappedConfig.getCacheConfig(name);
    }

    @Override
    public Config addCacheConfig(CacheSimpleConfig cacheConfig) {
        return wrappedConfig.addCacheConfig(cacheConfig);
    }

    @Override
    public Map<String, CacheSimpleConfig> getCacheConfigs() {
        return wrappedConfig.getCacheConfigs();
    }

    @Override
    public Config setCacheConfigs(Map<String, CacheSimpleConfig> cacheConfigs) {
        return wrappedConfig.setCacheConfigs(cacheConfigs);
    }

    @Override
    public QueueConfig findQueueConfig(String name) {
        return wrappedConfig.findQueueConfig(name);
    }

    @Override
    public QueueConfig getQueueConfig(String name) {
        return wrappedConfig.getQueueConfig(name);
    }

    @Override
    public Config addQueueConfig(QueueConfig queueConfig) {
        return wrappedConfig.addQueueConfig(queueConfig);
    }

    @Override
    public Map<String, QueueConfig> getQueueConfigs() {
        return wrappedConfig.getQueueConfigs();
    }

    @Override
    public Config setQueueConfigs(Map<String, QueueConfig> queueConfigs) {
        return wrappedConfig.setQueueConfigs(queueConfigs);
    }

    @Override
    public LockConfig findLockConfig(String name) {
        return wrappedConfig.findLockConfig(name);
    }

    @Override
    public LockConfig getLockConfig(String name) {
        return wrappedConfig.getLockConfig(name);
    }

    @Override
    public Config addLockConfig(LockConfig lockConfig) {
        return wrappedConfig.addLockConfig(lockConfig);
    }

    @Override
    public Map<String, LockConfig> getLockConfigs() {
        return wrappedConfig.getLockConfigs();
    }

    @Override
    public Config setLockConfigs(Map<String, LockConfig> lockConfigs) {
        return wrappedConfig.setLockConfigs(lockConfigs);
    }

    @Override
    public ListConfig findListConfig(String name) {
        return wrappedConfig.findListConfig(name);
    }

    @Override
    public ListConfig getListConfig(String name) {
        return wrappedConfig.getListConfig(name);
    }

    @Override
    public Config addListConfig(ListConfig listConfig) {
        return wrappedConfig.addListConfig(listConfig);
    }

    @Override
    public Map<String, ListConfig> getListConfigs() {
        return wrappedConfig.getListConfigs();
    }

    @Override
    public Config setListConfigs(Map<String, ListConfig> listConfigs) {
        return wrappedConfig.setListConfigs(listConfigs);
    }

    @Override
    public SetConfig findSetConfig(String name) {
        return wrappedConfig.findSetConfig(name);
    }

    @Override
    public SetConfig getSetConfig(String name) {
        return wrappedConfig.getSetConfig(name);
    }

    @Override
    public Config addSetConfig(SetConfig setConfig) {
        return wrappedConfig.addSetConfig(setConfig);
    }

    @Override
    public Map<String, SetConfig> getSetConfigs() {
        return wrappedConfig.getSetConfigs();
    }

    @Override
    public Config setSetConfigs(Map<String, SetConfig> setConfigs) {
        return wrappedConfig.setSetConfigs(setConfigs);
    }

    @Override
    public MultiMapConfig findMultiMapConfig(String name) {
        return wrappedConfig.findMultiMapConfig(name);
    }

    @Override
    public MultiMapConfig getMultiMapConfig(String name) {
        return wrappedConfig.getMultiMapConfig(name);
    }

    @Override
    public Config addMultiMapConfig(MultiMapConfig multiMapConfig) {
        return wrappedConfig.addMultiMapConfig(multiMapConfig);
    }

    @Override
    public Map<String, MultiMapConfig> getMultiMapConfigs() {
        return wrappedConfig.getMultiMapConfigs();
    }

    @Override
    public Config setMultiMapConfigs(Map<String, MultiMapConfig> multiMapConfigs) {
        return wrappedConfig.setMultiMapConfigs(multiMapConfigs);
    }

    @Override
    public ReplicatedMapConfig findReplicatedMapConfig(String name) {
        return wrappedConfig.findReplicatedMapConfig(name);
    }

    @Override
    public ReplicatedMapConfig getReplicatedMapConfig(String name) {
        return wrappedConfig.getReplicatedMapConfig(name);
    }

    @Override
    public Config addReplicatedMapConfig(ReplicatedMapConfig replicatedMapConfig) {
        return wrappedConfig.addReplicatedMapConfig(replicatedMapConfig);
    }

    @Override
    public Map<String, ReplicatedMapConfig> getReplicatedMapConfigs() {
        return wrappedConfig.getReplicatedMapConfigs();
    }

    @Override
    public Config setReplicatedMapConfigs(
            Map<String, ReplicatedMapConfig> replicatedMapConfigs) {
        return wrappedConfig.setReplicatedMapConfigs(replicatedMapConfigs);
    }

    @Override
    public RingbufferConfig findRingbufferConfig(String name) {
        return wrappedConfig.findRingbufferConfig(name);
    }

    @Override
    public RingbufferConfig getRingbufferConfig(String name) {
        return wrappedConfig.getRingbufferConfig(name);
    }

    @Override
    public Config addRingBufferConfig(RingbufferConfig ringbufferConfig) {
        return wrappedConfig.addRingBufferConfig(ringbufferConfig);
    }

    @Override
    public Map<String, RingbufferConfig> getRingbufferConfigs() {
        return wrappedConfig.getRingbufferConfigs();
    }

    @Override
    public Config setRingbufferConfigs(Map<String, RingbufferConfig> ringbufferConfigs) {
        return wrappedConfig.setRingbufferConfigs(ringbufferConfigs);
    }

    @Override
    public AtomicLongConfig findAtomicLongConfig(String name) {
        return wrappedConfig.findAtomicLongConfig(name);
    }

    @Override
    public AtomicLongConfig getAtomicLongConfig(String name) {
        return wrappedConfig.getAtomicLongConfig(name);
    }

    @Override
    public Config addAtomicLongConfig(AtomicLongConfig atomicLongConfig) {
        return wrappedConfig.addAtomicLongConfig(atomicLongConfig);
    }

    @Override
    public Map<String, AtomicLongConfig> getAtomicLongConfigs() {
        return wrappedConfig.getAtomicLongConfigs();
    }

    @Override
    public Config setAtomicLongConfigs(Map<String, AtomicLongConfig> atomicLongConfigs) {
        return wrappedConfig.setAtomicLongConfigs(atomicLongConfigs);
    }

    @Override
    public AtomicReferenceConfig findAtomicReferenceConfig(String name) {
        return wrappedConfig.findAtomicReferenceConfig(name);
    }

    @Override
    public AtomicReferenceConfig getAtomicReferenceConfig(String name) {
        return wrappedConfig.getAtomicReferenceConfig(name);
    }

    @Override
    public Config addAtomicReferenceConfig(AtomicReferenceConfig atomicReferenceConfig) {
        return wrappedConfig.addAtomicReferenceConfig(atomicReferenceConfig);
    }

    @Override
    public Map<String, AtomicReferenceConfig> getAtomicReferenceConfigs() {
        return wrappedConfig.getAtomicReferenceConfigs();
    }

    @Override
    public Config setAtomicReferenceConfigs(
            Map<String, AtomicReferenceConfig> atomicReferenceConfigs) {
        return wrappedConfig.setAtomicReferenceConfigs(atomicReferenceConfigs);
    }

    @Override
    public CountDownLatchConfig findCountDownLatchConfig(String name) {
        return wrappedConfig.findCountDownLatchConfig(name);
    }

    @Override
    public CountDownLatchConfig getCountDownLatchConfig(String name) {
        return wrappedConfig.getCountDownLatchConfig(name);
    }

    @Override
    public Config addCountDownLatchConfig(CountDownLatchConfig countDownLatchConfig) {
        return wrappedConfig.addCountDownLatchConfig(countDownLatchConfig);
    }

    @Override
    public Map<String, CountDownLatchConfig> getCountDownLatchConfigs() {
        return wrappedConfig.getCountDownLatchConfigs();
    }

    @Override
    public Config setCountDownLatchConfigs(
            Map<String, CountDownLatchConfig> countDownLatchConfigs) {
        return wrappedConfig.setCountDownLatchConfigs(countDownLatchConfigs);
    }

    @Override
    public TopicConfig findTopicConfig(String name) {
        return wrappedConfig.findTopicConfig(name);
    }

    @Override
    public TopicConfig getTopicConfig(String name) {
        return wrappedConfig.getTopicConfig(name);
    }

    @Override
    public Config addTopicConfig(TopicConfig topicConfig) {
        return wrappedConfig.addTopicConfig(topicConfig);
    }

    @Override
    public ReliableTopicConfig findReliableTopicConfig(String name) {
        return wrappedConfig.findReliableTopicConfig(name);
    }

    @Override
    public ReliableTopicConfig getReliableTopicConfig(String name) {
        return wrappedConfig.getReliableTopicConfig(name);
    }

    @Override
    public Map<String, ReliableTopicConfig> getReliableTopicConfigs() {
        return wrappedConfig.getReliableTopicConfigs();
    }

    @Override
    public Config addReliableTopicConfig(ReliableTopicConfig topicConfig) {
        return wrappedConfig.addReliableTopicConfig(topicConfig);
    }

    @Override
    public Config setReliableTopicConfigs(
            Map<String, ReliableTopicConfig> reliableTopicConfigs) {
        return wrappedConfig.setReliableTopicConfigs(reliableTopicConfigs);
    }

    @Override
    public Map<String, TopicConfig> getTopicConfigs() {
        return wrappedConfig.getTopicConfigs();
    }

    @Override
    public Config setTopicConfigs(Map<String, TopicConfig> topicConfigs) {
        return wrappedConfig.setTopicConfigs(topicConfigs);
    }

    @Override
    public ExecutorConfig findExecutorConfig(String name) {
        return wrappedConfig.findExecutorConfig(name);
    }

    @Override
    public DurableExecutorConfig findDurableExecutorConfig(String name) {
        return wrappedConfig.findDurableExecutorConfig(name);
    }

    @Override
    public ScheduledExecutorConfig findScheduledExecutorConfig(String name) {
        return wrappedConfig.findScheduledExecutorConfig(name);
    }

    @Override
    public CardinalityEstimatorConfig findCardinalityEstimatorConfig(String name) {
        return wrappedConfig.findCardinalityEstimatorConfig(name);
    }

    @Override
    public PNCounterConfig findPNCounterConfig(String name) {
        return wrappedConfig.findPNCounterConfig(name);
    }

    @Override
    public ExecutorConfig getExecutorConfig(String name) {
        return wrappedConfig.getExecutorConfig(name);
    }

    @Override
    public DurableExecutorConfig getDurableExecutorConfig(String name) {
        return wrappedConfig.getDurableExecutorConfig(name);
    }

    @Override
    public ScheduledExecutorConfig getScheduledExecutorConfig(String name) {
        return wrappedConfig.getScheduledExecutorConfig(name);
    }

    @Override
    public CardinalityEstimatorConfig getCardinalityEstimatorConfig(String name) {
        return wrappedConfig.getCardinalityEstimatorConfig(name);
    }

    @Override
    public PNCounterConfig getPNCounterConfig(String name) {
        return wrappedConfig.getPNCounterConfig(name);
    }

    @Override
    public Config addExecutorConfig(ExecutorConfig executorConfig) {
        return wrappedConfig.addExecutorConfig(executorConfig);
    }

    @Override
    public Config addDurableExecutorConfig(DurableExecutorConfig durableExecutorConfig) {
        return wrappedConfig.addDurableExecutorConfig(durableExecutorConfig);
    }

    @Override
    public Config addScheduledExecutorConfig(ScheduledExecutorConfig scheduledExecutorConfig) {
        return wrappedConfig.addScheduledExecutorConfig(scheduledExecutorConfig);
    }

    @Override
    public Config addCardinalityEstimatorConfig(CardinalityEstimatorConfig cardinalityEstimatorConfig) {
        return wrappedConfig.addCardinalityEstimatorConfig(cardinalityEstimatorConfig);
    }

    @Override
    public Config addPNCounterConfig(PNCounterConfig pnCounterConfig) {
        return wrappedConfig.addPNCounterConfig(pnCounterConfig);
    }

    @Override
    public Map<String, ExecutorConfig> getExecutorConfigs() {
        return wrappedConfig.getExecutorConfigs();
    }

    @Override
    public Config setExecutorConfigs(Map<String, ExecutorConfig> executorConfigs) {
        return wrappedConfig.setExecutorConfigs(executorConfigs);
    }

    @Override
    public Map<String, DurableExecutorConfig> getDurableExecutorConfigs() {
        return wrappedConfig.getDurableExecutorConfigs();
    }

    @Override
    public Config setDurableExecutorConfigs(
            Map<String, DurableExecutorConfig> durableExecutorConfigs) {
        return wrappedConfig.setDurableExecutorConfigs(durableExecutorConfigs);
    }

    @Override
    public Map<String, ScheduledExecutorConfig> getScheduledExecutorConfigs() {
        return wrappedConfig.getScheduledExecutorConfigs();
    }

    @Override
    public Config setScheduledExecutorConfigs(
            Map<String, ScheduledExecutorConfig> scheduledExecutorConfigs) {
        return wrappedConfig.setScheduledExecutorConfigs(scheduledExecutorConfigs);
    }

    @Override
    public Map<String, CardinalityEstimatorConfig> getCardinalityEstimatorConfigs() {
        return wrappedConfig.getCardinalityEstimatorConfigs();
    }

    @Override
    public Config setCardinalityEstimatorConfigs(
            Map<String, CardinalityEstimatorConfig> cardinalityEstimatorConfigs) {
        return wrappedConfig.setCardinalityEstimatorConfigs(cardinalityEstimatorConfigs);
    }

    @Override
    public Map<String, PNCounterConfig> getPNCounterConfigs() {
        return wrappedConfig.getPNCounterConfigs();
    }

    @Override
    public Config setPNCounterConfigs(Map<String, PNCounterConfig> pnCounterConfigs) {
        return wrappedConfig.setPNCounterConfigs(pnCounterConfigs);
    }

    @Override
    public SemaphoreConfig findSemaphoreConfig(String name) {
        return wrappedConfig.findSemaphoreConfig(name);
    }

    @Override
    public SemaphoreConfig getSemaphoreConfig(String name) {
        return wrappedConfig.getSemaphoreConfig(name);
    }

    @Override
    public Config addSemaphoreConfig(SemaphoreConfig semaphoreConfig) {
        return wrappedConfig.addSemaphoreConfig(semaphoreConfig);
    }

    @Override
    public Collection<SemaphoreConfig> getSemaphoreConfigs() {
        return wrappedConfig.getSemaphoreConfigs();
    }

    @Override
    public Map<String, SemaphoreConfig> getSemaphoreConfigsAsMap() {
        return wrappedConfig.getSemaphoreConfigsAsMap();
    }

    @Override
    public Config setSemaphoreConfigs(Map<String, SemaphoreConfig> semaphoreConfigs) {
        return wrappedConfig.setSemaphoreConfigs(semaphoreConfigs);
    }

    @Override
    public WanReplicationConfig getWanReplicationConfig(String name) {
        return wrappedConfig.getWanReplicationConfig(name);
    }

    @Override
    public Config addWanReplicationConfig(WanReplicationConfig wanReplicationConfig) {
        return wrappedConfig.addWanReplicationConfig(wanReplicationConfig);
    }

    @Override
    public Map<String, WanReplicationConfig> getWanReplicationConfigs() {
        return wrappedConfig.getWanReplicationConfigs();
    }

    @Override
    public Config setWanReplicationConfigs(
            Map<String, WanReplicationConfig> wanReplicationConfigs) {
        return wrappedConfig.setWanReplicationConfigs(wanReplicationConfigs);
    }

    @Override
    public Map<String, QuorumConfig> getQuorumConfigs() {
        return wrappedConfig.getQuorumConfigs();
    }

    @Override
    public QuorumConfig getQuorumConfig(String name) {
        return wrappedConfig.getQuorumConfig(name);
    }

    @Override
    public QuorumConfig findQuorumConfig(String name) {
        return wrappedConfig.findQuorumConfig(name);
    }

    @Override
    public Config setQuorumConfigs(Map<String, QuorumConfig> quorumConfigs) {
        return wrappedConfig.setQuorumConfigs(quorumConfigs);
    }

    @Override
    public Config addQuorumConfig(QuorumConfig quorumConfig) {
        return wrappedConfig.addQuorumConfig(quorumConfig);
    }

    @Override
    public ManagementCenterConfig getManagementCenterConfig() {
        return wrappedConfig.getManagementCenterConfig();
    }

    @Override
    public Config setManagementCenterConfig(ManagementCenterConfig managementCenterConfig) {
        return wrappedConfig.setManagementCenterConfig(managementCenterConfig);
    }

    @Override
    public ServicesConfig getServicesConfig() {
        return wrappedConfig.getServicesConfig();
    }

    @Override
    public Config setServicesConfig(ServicesConfig servicesConfig) {
        return wrappedConfig.setServicesConfig(servicesConfig);
    }

    @Override
    public SecurityConfig getSecurityConfig() {
        return wrappedConfig.getSecurityConfig();
    }

    @Override
    public Config setSecurityConfig(SecurityConfig securityConfig) {
        return wrappedConfig.setSecurityConfig(securityConfig);
    }

    @Override
    public Config addListenerConfig(ListenerConfig listenerConfig) {
        return wrappedConfig.addListenerConfig(listenerConfig);
    }

    @Override
    public List<ListenerConfig> getListenerConfigs() {
        return wrappedConfig.getListenerConfigs();
    }

    @Override
    public Config setListenerConfigs(List<ListenerConfig> listenerConfigs) {
        return wrappedConfig.setListenerConfigs(listenerConfigs);
    }

    @Override
    public EventJournalConfig findMapEventJournalConfig(String name) {
        return wrappedConfig.findMapEventJournalConfig(name);
    }

    @Override
    public EventJournalConfig findCacheEventJournalConfig(String name) {
        return wrappedConfig.findCacheEventJournalConfig(name);
    }

    @Override
    public EventJournalConfig getMapEventJournalConfig(String name) {
        return wrappedConfig.getMapEventJournalConfig(name);
    }

    @Override
    public EventJournalConfig getCacheEventJournalConfig(String name) {
        return wrappedConfig.getCacheEventJournalConfig(name);
    }

    @Override
    public Config addEventJournalConfig(EventJournalConfig eventJournalConfig) {
        return wrappedConfig.addEventJournalConfig(eventJournalConfig);
    }

    @Override
    public Map<String, FlakeIdGeneratorConfig> getFlakeIdGeneratorConfigs() {
        return wrappedConfig.getFlakeIdGeneratorConfigs();
    }

    @Override
    public FlakeIdGeneratorConfig findFlakeIdGeneratorConfig(String name) {
        return wrappedConfig.findFlakeIdGeneratorConfig(name);
    }

    @Override
    public FlakeIdGeneratorConfig getFlakeIdGeneratorConfig(String name) {
        return wrappedConfig.getFlakeIdGeneratorConfig(name);
    }

    @Override
    public Config addFlakeIdGeneratorConfig(FlakeIdGeneratorConfig config) {
        return wrappedConfig.addFlakeIdGeneratorConfig(config);
    }

    @Override
    public Config setFlakeIdGeneratorConfigs(Map<String, FlakeIdGeneratorConfig> map) {
        return wrappedConfig.setFlakeIdGeneratorConfigs(map);
    }

    @Override
    public Map<String, EventJournalConfig> getMapEventJournalConfigs() {
        return wrappedConfig.getMapEventJournalConfigs();
    }

    @Override
    public Map<String, EventJournalConfig> getCacheEventJournalConfigs() {
        return wrappedConfig.getCacheEventJournalConfigs();
    }

    @Override
    public Config setMapEventJournalConfigs(
            Map<String, EventJournalConfig> eventJournalConfigs) {
        return wrappedConfig.setMapEventJournalConfigs(eventJournalConfigs);
    }

    @Override
    public Config setCacheEventJournalConfigs(
            Map<String, EventJournalConfig> eventJournalConfigs) {
        return wrappedConfig.setCacheEventJournalConfigs(eventJournalConfigs);
    }

    @Override
    public SerializationConfig getSerializationConfig() {
        return wrappedConfig.getSerializationConfig();
    }

    @Override
    public Config setSerializationConfig(SerializationConfig serializationConfig) {
        return wrappedConfig.setSerializationConfig(serializationConfig);
    }

    @Override
    public PartitionGroupConfig getPartitionGroupConfig() {
        return wrappedConfig.getPartitionGroupConfig();
    }

    @Override
    public Config setPartitionGroupConfig(PartitionGroupConfig partitionGroupConfig) {
        return wrappedConfig.setPartitionGroupConfig(partitionGroupConfig);
    }

    @Override
    public HotRestartPersistenceConfig getHotRestartPersistenceConfig() {
        return wrappedConfig.getHotRestartPersistenceConfig();
    }

    @Override
    public Config setHotRestartPersistenceConfig(HotRestartPersistenceConfig hrConfig) {
        return wrappedConfig.setHotRestartPersistenceConfig(hrConfig);
    }

    @Override
    public CRDTReplicationConfig getCRDTReplicationConfig() {
        return wrappedConfig.getCRDTReplicationConfig();
    }

    @Override
    public Config setCRDTReplicationConfig(CRDTReplicationConfig crdtReplicationConfig) {
        return wrappedConfig.setCRDTReplicationConfig(crdtReplicationConfig);
    }

    @Override
    public ManagedContext getManagedContext() {
        return wrappedConfig.getManagedContext();
    }

    @Override
    public Config setManagedContext(ManagedContext managedContext) {
        return wrappedConfig.setManagedContext(managedContext);
    }

    @Override
    public ConcurrentMap<String, Object> getUserContext() {
        return wrappedConfig.getUserContext();
    }

    @Override
    public Config setUserContext(ConcurrentMap<String, Object> userContext) {
        return wrappedConfig.setUserContext(userContext);
    }

    @Override
    public NativeMemoryConfig getNativeMemoryConfig() {
        return wrappedConfig.getNativeMemoryConfig();
    }

    @Override
    public Config setNativeMemoryConfig(NativeMemoryConfig nativeMemoryConfig) {
        return wrappedConfig.setNativeMemoryConfig(nativeMemoryConfig);
    }

    @Override
    public URL getConfigurationUrl() {
        return wrappedConfig.getConfigurationUrl();
    }

    @Override
    public Config setConfigurationUrl(URL configurationUrl) {
        return wrappedConfig.setConfigurationUrl(configurationUrl);
    }

    @Override
    public File getConfigurationFile() {
        return wrappedConfig.getConfigurationFile();
    }

    @Override
    public Config setConfigurationFile(File configurationFile) {
        return wrappedConfig.setConfigurationFile(configurationFile);
    }

    @Override
    public String getLicenseKey() {
        return wrappedConfig.getLicenseKey();
    }

    @Override
    public Config setLicenseKey(String licenseKey) {
        return wrappedConfig.setLicenseKey(licenseKey);
    }

    @Override
    public boolean isLiteMember() {
        return wrappedConfig.isLiteMember();
    }

    @Override
    public Config setLiteMember(boolean liteMember) {
        return wrappedConfig.setLiteMember(liteMember);
    }

    @Override
    public UserCodeDeploymentConfig getUserCodeDeploymentConfig() {
        return wrappedConfig.getUserCodeDeploymentConfig();
    }

    @Override
    public Config setUserCodeDeploymentConfig(UserCodeDeploymentConfig userCodeDeploymentConfig) {
        return wrappedConfig.setUserCodeDeploymentConfig(userCodeDeploymentConfig);
    }

    @Override
    public AdvancedNetworkConfig getAdvancedNetworkConfig() {
        return wrappedConfig.getAdvancedNetworkConfig();
    }

    @Override
    public Config setAdvancedNetworkConfig(AdvancedNetworkConfig advancedNetworkConfig) {
        return wrappedConfig.setAdvancedNetworkConfig(advancedNetworkConfig);
    }

    @Override
    public CPSubsystemConfig getCPSubsystemConfig() {
        return wrappedConfig.getCPSubsystemConfig();
    }

    @Override
    public Config setCPSubsystemConfig(CPSubsystemConfig cpSubsystemConfig) {
        return wrappedConfig.setCPSubsystemConfig(cpSubsystemConfig);
    }
}
