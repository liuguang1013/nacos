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
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientVerifyInfo;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.DistroMapper;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactoryHolder;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.healthcheck.heartbeat.ClientBeatUpdateTask;
import com.alibaba.nacos.naming.misc.ClientConfig;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.misc.NamingExecuteTaskDispatcher;
import com.alibaba.nacos.naming.misc.SwitchDomain;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

/**
 * The manager of {@code IpPortBasedClient} and ephemeral.
 * 基于 ip 和端口的 manager 并且会短暂的
 * @author xiweng.yy
 */
@DependsOn("clientServiceIndexesManager")
@Component("ephemeralIpPortClientManager")
public class EphemeralIpPortClientManager implements ClientManager {
    
    private final ConcurrentMap<String, IpPortBasedClient> clients = new ConcurrentHashMap<>();
    
    private final DistroMapper distroMapper;
    
    private final ClientFactory<IpPortBasedClient> clientFactory;

    /**
     * 唯一构造器
     * @param distroMapper distro 协议 mapper
     * @param switchDomain 开关对象
     */
    public EphemeralIpPortClientManager(DistroMapper distroMapper, SwitchDomain switchDomain) {
        this.distroMapper = distroMapper;
        // 定时清理 客户端，初始无延迟，5秒执行一次
        GlobalExecutor.scheduleExpiredClientCleaner(new ExpiredClientCleaner(this, switchDomain), 0,
                Constants.DEFAULT_HEART_BEAT_INTERVAL, TimeUnit.MILLISECONDS);
        // 执行 客户端工厂 EphemeralIpPortClientFactory
        clientFactory = ClientFactoryHolder.getInstance().findClientFactory(ClientConstants.EPHEMERAL_IP_PORT);
    }
    
    @Override
    public boolean clientConnected(String clientId, ClientAttributes attributes) {
        return clientConnected(clientFactory.newClient(clientId, attributes));
    }
    
    @Override
    public boolean clientConnected(final Client client) {
        clients.computeIfAbsent(client.getClientId(), s -> {
            Loggers.SRV_LOG.info("Client connection {} connect", client.getClientId());
            IpPortBasedClient ipPortBasedClient = (IpPortBasedClient) client;
            ipPortBasedClient.init();
            return ipPortBasedClient;
        });
        return true;
    }
    
    @Override
    public boolean syncClientConnected(String clientId, ClientAttributes attributes) {
        return clientConnected(clientFactory.newSyncedClient(clientId, attributes));
    }
    
    @Override
    public boolean clientDisconnected(String clientId) {
        Loggers.SRV_LOG.info("Client connection {} disconnect, remove instances and subscribers", clientId);
        IpPortBasedClient client = clients.remove(clientId);
        if (null == client) {
            return true;
        }
        // 通知中心发布 客户端事件，客户端断开连接事件
        NotifyCenter.publishEvent(new ClientEvent.ClientDisconnectEvent(client, isResponsibleClient(client)));
        // 客户端释放：抽象父类中 减少监控指标数量、取消健康检查
        client.release();
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

    /**
     * 判断是否该服务器是否负责客户端
     * @param client client
     * @return
     */
    @Override
    public boolean isResponsibleClient(Client client) {
        if (client instanceof IpPortBasedClient) {
            return distroMapper.responsible(((IpPortBasedClient) client).getResponsibleId());
        }
        return false;
    }
    
    @Override
    public boolean verifyClient(DistroClientVerifyInfo verifyData) {
        String clientId = verifyData.getClientId();
        IpPortBasedClient client = clients.get(clientId);
        if (null != client) {
            // remote node of old version will always verify with zero revision
            if (0 == verifyData.getRevision() || client.getRevision() == verifyData.getRevision()) {
                NamingExecuteTaskDispatcher.getInstance()
                        .dispatchAndExecuteTask(clientId, new ClientBeatUpdateTask(client));
                return true;
            } else {
                Loggers.DISTRO.info("[DISTRO-VERIFY-FAILED] IpPortBasedClient[{}] revision local={}, remote={}",
                        client.getClientId(), client.getRevision(), verifyData.getRevision());
            }
        }
        return false;
    }
    
    private static class ExpiredClientCleaner implements Runnable {
        
        private final EphemeralIpPortClientManager clientManager;
        
        private final SwitchDomain switchDomain;
        
        public ExpiredClientCleaner(EphemeralIpPortClientManager clientManager, SwitchDomain switchDomain) {
            this.clientManager = clientManager;
            this.switchDomain = switchDomain;
        }
        
        @Override
        public void run() {
            // 遍历各个客户端，判断是否过期
            long currentTime = System.currentTimeMillis();
            for (String each : clientManager.allClientId()) {
                IpPortBasedClient client = (IpPortBasedClient) clientManager.getClient(each);
                if (null != client && isExpireClient(currentTime, client)) {
                    // 发布客户端断开连接事件、客户端取消心跳检查
                    clientManager.clientDisconnected(each);
                }
            }
        }
        
        private boolean isExpireClient(long currentTime, IpPortBasedClient client) {
            long noUpdatedTime = currentTime - client.getLastUpdatedTime();
            return client.isEphemeral() && (
                    isExpirePublishedClient(noUpdatedTime, client) && isExpireSubscriberClient(noUpdatedTime, client)
                            // 超过默认的超时时间 3分钟
                            || noUpdatedTime > ClientConfig.getInstance().getClientExpiredTime());
        }
        
        private boolean isExpirePublishedClient(long noUpdatedTime, IpPortBasedClient client) {
            //  发布者服务为空 并且 超过默认的超时时间 30s
            return client.getAllPublishedService().isEmpty() && noUpdatedTime > Constants.DEFAULT_IP_DELETE_TIMEOUT;
        }
        
        private boolean isExpireSubscriberClient(long noUpdatedTime, IpPortBasedClient client) {
            //  订阅者服务为空 并且 超过默认的超时时间 10s
            return client.getAllSubscribeService().isEmpty() || noUpdatedTime > switchDomain.getDefaultPushCacheMillis();
        }
    }
}
