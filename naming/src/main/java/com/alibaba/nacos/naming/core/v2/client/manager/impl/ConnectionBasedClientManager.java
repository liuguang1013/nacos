/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.nacos.naming.core.v2.client.manager.impl;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.core.remote.ClientConnectionEventListener;
import com.alibaba.nacos.core.remote.Connection;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientVerifyInfo;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactoryHolder;
import com.alibaba.nacos.naming.core.v2.client.impl.ConnectionBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The manager of {@code ConnectionBasedClient}.
 *
 * @author xiweng.yy
 */
@Component("connectionBasedClientManager")
public class ConnectionBasedClientManager extends ClientConnectionEventListener implements ClientManager {

    /**
     * 缓存连接到该服务端的客户端连接信息
     * 也缓存 其他的服务端负责的客户端信息
     * key ConnectionId 连接id
     * value ConnectionBasedClient 对象
     */
    private final ConcurrentMap<String, ConnectionBasedClient> clients = new ConcurrentHashMap<>();
    
    public ConnectionBasedClientManager() {
        // 初始无延迟，5s 执行一次，过期客户端清理
        GlobalExecutor
                .scheduleExpiredClientCleaner(new ExpiredClientCleaner(this), 0, Constants.DEFAULT_HEART_BEAT_INTERVAL,
                        TimeUnit.MILLISECONDS);
    }
    
    @Override
    public void clientConnected(Connection connect) {
        // 当 连接的标签中，模块不是 naming 直接返回
        // 意味着 只有命名服务才是 基于连接的客户端
        if (!RemoteConstants.LABEL_MODULE_NAMING.equals(connect.getMetaInfo().getLabel(RemoteConstants.LABEL_MODULE))) {
            return;
        }
        ClientAttributes attributes = new ClientAttributes();
        // 设置客户端属性：连接类型是 grpc
        attributes.addClientAttribute(ClientConstants.CONNECTION_TYPE, connect.getMetaInfo().getConnectType());
        // 设置客户端属性：连接元数据
        attributes.addClientAttribute(ClientConstants.CONNECTION_METADATA, connect.getMetaInfo());
        // 客户端连接： 创建 ConnectionBasedClient 客户端对象
        clientConnected(connect.getMetaInfo().getConnectionId(), attributes);
    }

    /**
     * 使用工厂类，创建 ConnectionBasedClient 客户端对象
     * @param clientId new client id        客户端 ip
     * @param attributes client attributes, which can help create client    客户端属性
     * @return
     */
    @Override
    public boolean clientConnected(String clientId, ClientAttributes attributes) {
        String type = attributes.getClientAttribute(ClientConstants.CONNECTION_TYPE);
        // 单例 ClientFactoryHolder 查找客户端工厂类
        ClientFactory clientFactory = ClientFactoryHolder.getInstance().findClientFactory(type);
        // 工厂类创建客户端
        return clientConnected(clientFactory.newClient(clientId, attributes));
    }
    
    @Override
    public boolean clientConnected(final Client client) {
        // 向 map 缓存中添加 客户端对象
        clients.computeIfAbsent(client.getClientId(), s -> {
            Loggers.SRV_LOG.info("Client connection {} connect", client.getClientId());
            return (ConnectionBasedClient) client;
        });
        return true;
    }
    
    @Override
    public boolean syncClientConnected(String clientId, ClientAttributes attributes) {
        // 获取 客户端属性中 connectionType 连接类型，
        // 响应返回时候，未带该参数：详见 AbstractClient#generateSyncData()方法
        String type = attributes.getClientAttribute(ClientConstants.CONNECTION_TYPE);
        // 传参为null ，返回默认 ConnectionBasedClientFactory
        ClientFactory clientFactory = ClientFactoryHolder.getInstance().findClientFactory(type);
        // 创建本服务端不负责的客户端对象，触发客户度连接流程，放入 this 管理者缓存
        return clientConnected(clientFactory.newSyncedClient(clientId, attributes));
    }
    
    @Override
    public void clientDisConnected(Connection connect) {
        clientDisconnected(connect.getMetaInfo().getConnectionId());
    }
    
    @Override
    public boolean clientDisconnected(String clientId) {
        Loggers.SRV_LOG.info("Client connection {} disconnect, remove instances and subscribers", clientId);
        ConnectionBasedClient client = clients.remove(clientId);
        if (null == client) {
            return true;
        }
        // 客户端释放：处理指标监控数据
        client.release();
        //
        NotifyCenter.publishEvent(new ClientEvent.ClientDisconnectEvent(client, isResponsibleClient(client)));
        return true;
    }
    
    @Override
    public Client getClient(String clientId) {
        return clients.get(clientId);
    }
    
    @Override
    public boolean contains(String clientId) {
        return clients.containsKey(clientId);
    }
    
    @Override
    public Collection<String> allClientId() {
        return clients.keySet();
    }
    
    @Override
    public boolean isResponsibleClient(Client client) {
        return (client instanceof ConnectionBasedClient) && ((ConnectionBasedClient) client).isNative();
    }
    
    @Override
    public boolean verifyClient(DistroClientVerifyInfo verifyData) {
        // 获取客户端
        ConnectionBasedClient client = clients.get(verifyData.getClientId());
        if (null != client) {
            // remote node of old version will always verify with zero revision
            // 旧版本的远程节点将始终以零修订进行验证
            if (0 == verifyData.getRevision() || client.getRevision() == verifyData.getRevision()) {
                // 设置
                client.setLastRenewTime();
                return true;
            } else {
                Loggers.DISTRO.info("[DISTRO-VERIFY-FAILED] ConnectionBasedClient[{}] revision local={}, remote={}",
                        client.getClientId(), client.getRevision(), verifyData.getRevision());
            }
        }
        return false;
    }
    
    private static class ExpiredClientCleaner implements Runnable {
        
        private final ConnectionBasedClientManager clientManager;
        
        public ExpiredClientCleaner(ConnectionBasedClientManager clientManager) {
            this.clientManager = clientManager;
        }
        
        @Override
        public void run() {
            long currentTime = System.currentTimeMillis();
            for (String each : clientManager.allClientId()) {
                ConnectionBasedClient client = (ConnectionBasedClient) clientManager.getClient(each);
                if (null != client && client.isExpire(currentTime)) {
                    clientManager.clientDisconnected(each);
                }
            }
        }
    }
}
