package com.hazelcast.client.impl.protocol;

import com.hazelcast.instance.Node;
import com.hazelcast.spi.NodeEngine;
import com.hazelcast.nio.Connection;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.client.impl.protocol.task.MessageTask;

public class EnterpriseMessageTaskFactoryProvider implements MessageTaskFactoryProvider {

    private final MessageTaskFactory[] factories = new MessageTaskFactory[Short.MAX_VALUE];


    private final Node node;

    public EnterpriseMessageTaskFactoryProvider(NodeEngine nodeEngine) {
        this.node = ((NodeEngineImpl) nodeEngine).getNode();
        initFactories();
    }

    @SuppressWarnings("checkstyle:linelength")
    public void initFactories() {
//region ----------  REGISTRATION FOR com.hazelcast.client.impl.protocol.task.enterprisemap
        factories[com.hazelcast.client.impl.protocol.codec.EnterpriseMapDestroyCacheCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.enterprisemap.MapDestroyCacheMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.EnterpriseMapPublisherCreateCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.enterprisemap.MapPublisherCreateMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.EnterpriseMapSetReadCursorCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.enterprisemap.MapSetReadCursorMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.EnterpriseMapAddListenerCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.enterprisemap.MapAddListenerMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.EnterpriseMapMadePublishableCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.enterprisemap.MapMadePublishableMessageTask(clientMessage, node, connection);
            }
        };
        factories[com.hazelcast.client.impl.protocol.codec.EnterpriseMapPublisherCreateWithValueCodec.RequestParameters.TYPE.id()] = new MessageTaskFactory() {
            public MessageTask create(ClientMessage clientMessage, Connection connection) {
                return new com.hazelcast.client.impl.protocol.task.enterprisemap.MapPublisherCreateWithValueMessageTask(clientMessage, node, connection);
            }
        };
//endregion

    }

    @edu.umd.cs.findbugs.annotations.SuppressWarnings({"MS_EXPOSE_REP", "EI_EXPOSE_REP"})
    @Override
    public MessageTaskFactory[] getFactories() {
        return factories;
    }
}


