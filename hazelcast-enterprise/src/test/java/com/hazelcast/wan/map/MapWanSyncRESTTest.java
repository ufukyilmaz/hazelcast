package com.hazelcast.wan.map;

import com.hazelcast.config.Config;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.config.WanAcknowledgeType;
import com.hazelcast.config.WanPublisherConfig;
import com.hazelcast.config.WanReplicationConfig;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.enterprise.wan.impl.replication.WanReplicationProperties;
import com.hazelcast.enterprise.wan.impl.sync.SyncFailedException;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.internal.ascii.HTTPCommunicator;
import com.hazelcast.internal.json.Json;
import com.hazelcast.internal.json.JsonObject;
import com.hazelcast.internal.management.dto.WanReplicationConfigDTO;
import com.hazelcast.spi.properties.GroupProperty;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import com.hazelcast.wan.impl.AddWanConfigResult;
import com.hazelcast.wan.impl.WanReplicationService;
import com.hazelcast.wan.WanServiceMockingNodeContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static java.util.UUID.randomUUID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(HazelcastSerialClassRunner.class)
@Category(QuickTest.class)
public class MapWanSyncRESTTest extends HazelcastTestSupport {

    private WanReplicationService wanServiceMock;
    private HTTPCommunicator communicator;

    @Before
    public void initInstance() {
        wanServiceMock = mock(WanReplicationService.class);
        HazelcastInstance instance = HazelcastInstanceFactory.newHazelcastInstance(getConfig(), randomName(),
                new WanServiceMockingNodeContext(wanServiceMock));
        communicator = new HTTPCommunicator(instance);
    }

    @Test
    public void syncSuccess() throws Exception {
        UUID expectedUuid = randomUUID();
        when(wanServiceMock.syncMap(any(), any(), any())).thenReturn(expectedUuid);

        JsonObject responseObject = assertSuccess(communicator.syncMapOverWAN("atob", "B", "map"));
        assertNotNull(responseObject.getString("message", null));
        assertEquals(expectedUuid.toString(), responseObject.getString("uuid", null));
        verify(wanServiceMock, times(1)).syncMap("atob", "B", "map");
    }

    @Test
    public void syncFail() throws IOException {
        String msg = "Error occurred";
        doThrow(new RuntimeException(msg))
                .when(wanServiceMock)
                .syncMap("atob", "B", "map");
        JsonObject responseObject = assertFail(communicator.syncMapOverWAN("atob", "B", "map"));
        assertEquals(msg, responseObject.getString("message", null));
        verify(wanServiceMock, times(1)).syncMap("atob", "B", "map");
    }

    @Test
    public void syncAllSuccess() throws IOException {
        UUID expectedUuid = randomUUID();
        when(wanServiceMock.syncAllMaps(any(), any())).thenReturn(expectedUuid);

        JsonObject responseObject = assertSuccess(communicator.syncMapsOverWAN("atob", "B"));
        assertNotNull(responseObject.getString("message", null));
        assertEquals(expectedUuid.toString(), responseObject.getString("uuid", null));
        verify(wanServiceMock, times(1)).syncAllMaps("atob", "B");
    }

    @Test
    public void syncAllFail() throws IOException {
        String msg = "Error occurred";
        doThrow(new RuntimeException(msg))
                .when(wanServiceMock)
                .syncAllMaps("atob", "B");
        JsonObject responseObject = assertFail(communicator.syncMapsOverWAN("atob", "B"));
        assertEquals(msg, responseObject.getString("message", null));
        verify(wanServiceMock, times(1)).syncAllMaps("atob", "B");
    }

    @Test
    public void consistencyCheckSuccess() throws Exception {
        UUID expectedUuid = randomUUID();
        when(wanServiceMock.consistencyCheck(any(), any(), any())).thenReturn(expectedUuid);

        JsonObject responseObject = assertSuccess(communicator.wanMapConsistencyCheck("atob", "B", "map"));
        assertNotNull(responseObject.getString("message", null));
        assertEquals(expectedUuid.toString(), responseObject.getString("uuid", null));
        verify(wanServiceMock, times(1)).consistencyCheck("atob", "B", "map");
    }

    @Test
    public void consistencyCheckFail() throws IOException {
        String msg = "Error occurred";
        doThrow(new RuntimeException(msg))
                .when(wanServiceMock)
                .consistencyCheck("atob", "B", "map");
        JsonObject responseObject = assertFail(communicator.wanMapConsistencyCheck("atob", "B", "map"));
        assertEquals(msg, responseObject.getString("message", null));
        verify(wanServiceMock, times(1)).consistencyCheck("atob", "B", "map");
    }

    @Test
    public void addWanConfigSuccess() throws IOException {
        WanReplicationConfig wanConfig = getExampleWanConfig();
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanConfig);

        when(wanServiceMock.addWanReplicationConfig(wanConfig))
                .thenReturn(new AddWanConfigResult(Collections.singleton("A"), Collections.singleton("B")));

        JsonObject responseObject = assertSuccess(communicator.addWanConfig(dto.toJson().toString()));

        assertEquals(1, responseObject.get("addedPublisherIds").asArray().size());
        assertEquals(1, responseObject.get("ignoredPublisherIds").asArray().size());
        verify(wanServiceMock, times(1)).addWanReplicationConfig(wanConfig);
    }

    @Test
    public void addWanConfigFail() throws IOException {
        WanReplicationConfig wanConfig = getExampleWanConfig();
        WanReplicationConfigDTO dto = new WanReplicationConfigDTO(wanConfig);

        String msg = "Error occurred";
        doThrow(new RuntimeException(msg))
                .when(wanServiceMock)
                .addWanReplicationConfig(wanConfig);

        JsonObject responseObject = assertFail(communicator.addWanConfig(dto.toJson().toString()));
        assertEquals(msg, responseObject.getString("message", null));
        verify(wanServiceMock, times(1)).addWanReplicationConfig(wanConfig);
    }

    @Test
    public void sendMultipleSyncRequestsWithREST() throws IOException {
        doThrow(new SyncFailedException("message"))
                .when(wanServiceMock)
                .syncAllMaps("atob", "B");
        assertFail(communicator.syncMapsOverWAN("atob", "B"));
        verify(wanServiceMock, times(1)).syncAllMaps("atob", "B");
    }

    private WanReplicationConfig getExampleWanConfig() {
        WanPublisherConfig newPublisherConfig = new WanPublisherConfig()
                .setGroupName("B")
                .setClassName("ClassName");
        Map<String, Comparable> props = newPublisherConfig.getProperties();
        props.put(WanReplicationProperties.GROUP_PASSWORD.key(), "password");
        props.put(WanReplicationProperties.ENDPOINTS.key(), "1.1.1.1:5701");
        props.put(WanReplicationProperties.ACK_TYPE.key(), WanAcknowledgeType.ACK_ON_OPERATION_COMPLETE.name());

        return new WanReplicationConfig()
                .setName("newWRConfig")
                .addWanPublisherConfig(newPublisherConfig);
    }

    @Override
    protected Config getConfig() {
        Config config = smallInstanceConfig()
                .setProperty(GroupProperty.REST_ENABLED.getName(), "true");
        JoinConfig joinConfig = config.getNetworkConfig().getJoin();
        joinConfig.getMulticastConfig()
                  .setEnabled(false);
        joinConfig.getTcpIpConfig()
                  .setEnabled(true)
                  .addMember("127.0.0.1");
        return config;
    }

    @After
    public void cleanup() {
        HazelcastInstanceFactory.shutdownAll();
    }

    private JsonObject assertFail(String jsonResult) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals("fail", result.getString("status", null));
        return result;
    }

    private JsonObject assertSuccess(String jsonResult) {
        JsonObject result = Json.parse(jsonResult).asObject();
        assertEquals("success", result.getString("status", null));
        return result;
    }
}
