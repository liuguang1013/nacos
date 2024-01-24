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

package com.alibaba.nacos.naming.consistency.ephemeral.distro.v2;

import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.SmartSubscriber;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataStorage;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.naming.cluster.transport.Serializer;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.ServiceManager;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncData;
import com.alibaba.nacos.naming.core.v2.client.ClientSyncDatumSnapshot;
import com.alibaba.nacos.naming.core.v2.client.manager.ClientManager;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.core.v2.event.client.ClientOperationEvent;
import com.alibaba.nacos.naming.core.v2.event.publisher.NamingEventPublisherFactory;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstanceData;
import com.alibaba.nacos.naming.core.v2.pojo.BatchInstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.InstancePublishInfo;
import com.alibaba.nacos.naming.core.v2.pojo.Service;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

/**
 * Distro processor for v2.
 * 客户端数据处理器，监听客户端事件，将事件同步到其他服务端
 * @author xiweng.yy
 */
public class DistroClientDataProcessor extends SmartSubscriber implements DistroDataStorage, DistroDataProcessor {
    
    public static final String TYPE = "Nacos:Naming:v2:ClientData";
    
    private final ClientManager clientManager;
    
    private final DistroProtocol distroProtocol;
    
    private volatile boolean isFinishInitial;

    /**
     * 通过 DistroClientComponentRegistry 加载
     * @param clientManager 客户端管理者代理
     * @param distroProtocol Distro 协议对象
     */
    public DistroClientDataProcessor(ClientManager clientManager, DistroProtocol distroProtocol) {
        this.clientManager = clientManager;
        this.distroProtocol = distroProtocol;
        NotifyCenter.registerSubscriber(this, NamingEventPublisherFactory.getInstance());
    }
    
    @Override
    public void finishInitial() {
        isFinishInitial = true;
    }
    
    @Override
    public boolean isFinishInitial() {
        return isFinishInitial;
    }

    /**
     * 订阅事件
     * @return
     */
    @Override
    public List<Class<? extends Event>> subscribeTypes() {
        List<Class<? extends Event>> result = new LinkedList<>();
        // 客户端事件：客户端改变
        result.add(ClientEvent.ClientChangedEvent.class);
        // 客户端事件：客户端断开连接
        result.add(ClientEvent.ClientDisconnectEvent.class);
        // 客户端事件：客户端验证失败
        result.add(ClientEvent.ClientVerifyFailedEvent.class);
        return result;
    }

    /**
     *
     * @param event {@link Event}
     */
    @Override
    public void onEvent(Event event) {
        // 单机部署，直接返回
        if (EnvUtil.getStandaloneMode()) {
            return;
        }
        if (event instanceof ClientEvent.ClientVerifyFailedEvent) {
            syncToVerifyFailedServer((ClientEvent.ClientVerifyFailedEvent) event);
        } else {
            // 客户端增加/删除实例信息都会触发：同步到所有服务端
            syncToAllServer((ClientEvent) event);
        }
    }
    
    private void syncToVerifyFailedServer(ClientEvent.ClientVerifyFailedEvent event) {
        Client client = clientManager.getClient(event.getClientId());
        if (null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client)) {
            return;
        }
        DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
        // Verify failed data should be sync directly.
        distroProtocol.syncToTarget(distroKey, DataOperation.ADD, event.getTargetServer(), 0L);
    }
    
    private void syncToAllServer(ClientEvent event) {
        Client client = event.getClient();
        // Only ephemeral data sync by Distro, persist client should sync by raft.
        // 只有 临时数据 通过 Distro 协议同步，持久的客户端应该通过 raft 协议同步
        // 判断当前服务端是否负责客户端
        if (null == client || !client.isEphemeral() || !clientManager.isResponsibleClient(client)) {
            return;
        }
        // distro 协议同步
        if (event instanceof ClientEvent.ClientDisconnectEvent) {
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            distroProtocol.sync(distroKey, DataOperation.DELETE);
        } else if (event instanceof ClientEvent.ClientChangedEvent) {
            DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
            // distro 协议同步 增加/删除服务实例（实际就是客户端连接对象的变化）
            // 遍历其他服务端，通过 distro 的延迟任务引擎执行，
            // DistroDelayTaskProcessor 延时任务引擎向立刻执行引擎中添加任务
            // 通过 服务端间的传输代理发送 DistroDataRequest 异步请求
            // 最终进入其他服务端的 this.processData() 方法
            distroProtocol.sync(distroKey, DataOperation.CHANGE);
        }
    }
    
    @Override
    public String processType() {
        return TYPE;
    }
    
    @Override
    public boolean processData(DistroData distroData) {
        switch (distroData.getType()) {
            case ADD:
            case CHANGE:
                ClientSyncData clientSyncData = ApplicationUtils.getBean(Serializer.class)
                        .deserialize(distroData.getContent(), ClientSyncData.class);
                // 处理其他服务期发来的：客户端同步数据
                handlerClientSyncData(clientSyncData);
                return true;
            case DELETE:
                String deleteClientId = distroData.getDistroKey().getResourceKey();
                Loggers.DISTRO.info("[Client-Delete] Received distro client sync data {}", deleteClientId);
                clientManager.clientDisconnected(deleteClientId);
                return true;
            default:
                return false;
        }
    }

    /**
     * ！！！！ 重要方法：处理客户端同步数据
     * 当有一个服务的实例信息发生变化，就会向其他的所有服务端同步信息，
     * 其他服务端收到消息后再次进入该方法，向其他服务端同步，最终达到数据一致
     * @param clientSyncData
     */
    private void handlerClientSyncData(ClientSyncData clientSyncData) {
        Loggers.DISTRO
                .info("[Client-Add] Received distro client sync data {}, revision={}", clientSyncData.getClientId(),
                        clientSyncData.getAttributes().getClientAttribute(ClientConstants.REVISION, 0L));
        // 客户端管理者，同步 客户端连接信息：创建本服务端不负责的客户端对象，放入 客户端管理者缓存中
        clientManager.syncClientConnected(clientSyncData.getClientId(), clientSyncData.getAttributes());
        Client client = clientManager.getClient(clientSyncData.getClientId());
        // 升级客户端：
        upgradeClient(client, clientSyncData);
    }
    
    private void upgradeClient(Client client, ClientSyncData clientSyncData) {
        Set<Service> syncedService = new HashSet<>();
        // process batch instance sync logic
        // 处理批处理实例同步逻辑
        // itodo：待看的
        processBatchInstanceDistroData(syncedService, client, clientSyncData);

        List<String> namespaces = clientSyncData.getNamespaces();
        List<String> groupNames = clientSyncData.getGroupNames();
        List<String> serviceNames = clientSyncData.getServiceNames();
        List<InstancePublishInfo> instances = clientSyncData.getInstancePublishInfos();
        // 遍历命名空间
        for (int i = 0; i < namespaces.size(); i++) {
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            InstancePublishInfo instancePublishInfo = instances.get(i);
            // 比对：从其他服务端来的 服务的实例信息，和 本机客户端保存的 服务实例信息
            // 对于通过注册到服务端的客户端连接，会向 AbstractClient 缓存中添加 addServiceInstance 实例信息
            // 对于 通过从其他客户端同步来的客户端连接信息，没有添加的服务的实例信息
            // 对于加载第一个其他服务器，肯定是不一样的，但是对于后续的相同的实例信息
            if (!instancePublishInfo.equals(client.getInstancePublishInfo(singleton))) {
                // 客户端添加服务的 实例信息 ：触发 ClientEvent.ClientChangedEvent 事件
                client.addServiceInstance(singleton, instancePublishInfo);
                // 发布客户端注册服务事件：向 ClientServiceIndexesManager 中的发布者缓存中添加数据
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
        // 检查不包含的移除
        for (Service each : client.getAllPublishedService()) {
            if (!syncedService.contains(each)) {
                // 客户端移除服务实例信息：触发 ClientEvent.ClientChangedEvent 事件
                client.removeServiceInstance(each);
                // 发布客户端取消注册服务事件： ClientServiceIndexesManager 中的发布者缓存中移除数据
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientDeregisterServiceEvent(each, client.getClientId()));
            }
        }
    }
    
    private static void processBatchInstanceDistroData(Set<Service> syncedService, Client client,
            ClientSyncData clientSyncData) {
        BatchInstanceData batchInstanceData = clientSyncData.getBatchInstanceData();
        if (batchInstanceData == null || CollectionUtils.isEmpty(batchInstanceData.getNamespaces())) {
            Loggers.DISTRO.info("[processBatchInstanceDistroData] BatchInstanceData is null , clientId is :{}",
                    client.getClientId());
            return;
        }
        List<String> namespaces = batchInstanceData.getNamespaces();
        List<String> groupNames = batchInstanceData.getGroupNames();
        List<String> serviceNames = batchInstanceData.getServiceNames();
        List<BatchInstancePublishInfo> batchInstancePublishInfos = batchInstanceData.getBatchInstancePublishInfos();
        // 遍历命名空间
        for (int i = 0; i < namespaces.size(); i++) {
            // 创建 Service 对象
            Service service = Service.newService(namespaces.get(i), groupNames.get(i), serviceNames.get(i));
            Service singleton = ServiceManager.getInstance().getSingleton(service);
            syncedService.add(singleton);
            BatchInstancePublishInfo batchInstancePublishInfo = batchInstancePublishInfos.get(i);
            BatchInstancePublishInfo targetInstanceInfo = (BatchInstancePublishInfo) client
                    .getInstancePublishInfo(singleton);
            boolean result = false;
            if (targetInstanceInfo != null) {
                result = batchInstancePublishInfo.equals(targetInstanceInfo);
            }
            if (!result) {
                client.addServiceInstance(service, batchInstancePublishInfo);
                NotifyCenter.publishEvent(
                        new ClientOperationEvent.ClientRegisterServiceEvent(singleton, client.getClientId()));
            }
        }
        client.setRevision(clientSyncData.getAttributes().<Integer>getClientAttribute(ClientConstants.REVISION, 0));
    }
    
    @Override
    public boolean processVerifyData(DistroData distroData, String sourceAddress) {
        // 反序列化
        DistroClientVerifyInfo verifyData = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), DistroClientVerifyInfo.class);
        // 客户端管理者 验证客户端（其他服务端）的数据 ： 主要是验证 缓存的客户端 和 其他服务端的 revision
        if (clientManager.verifyClient(verifyData)) {
            return true;
        }
        Loggers.DISTRO.info("client {} is invalid, get new client from {}", verifyData.getClientId(), sourceAddress);
        return false;
    }

    /**
     *  与 getDatumSnapshot() 方法对应
     * @param distroData snapshot data
     * @return
     */
    @Override
    public boolean processSnapshot(DistroData distroData) {
        // 反序列化
        ClientSyncDatumSnapshot snapshot = ApplicationUtils.getBean(Serializer.class)
                .deserialize(distroData.getContent(), ClientSyncDatumSnapshot.class);
        for (ClientSyncData each : snapshot.getClientSyncDataList()) {
            // 处理其他服务端同步的客户端实例信息
            handlerClientSyncData(each);
        }
        return true;
    }
    
    @Override
    public DistroData getDistroData(DistroKey distroKey) {
        Client client = clientManager.getClient(distroKey.getResourceKey());
        if (null == client) {
            return null;
        }
        // 序列化
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(client.generateSyncData());
        return new DistroData(distroKey, data);
    }
    
    @Override
    public DistroData getDatumSnapshot() {
        List<ClientSyncData> datum = new LinkedList<>();
        // 获取集群所有临时类型的客户端对象
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            // 客户端数据列表中添加：某客户端下注册提供的所有的服务信息
            // itodo：此处目前来看 一个客户端只提供一种服务（即一个service 对应一个 instant 对象）
            datum.add(client.generateSyncData());
        }
        ClientSyncDatumSnapshot snapshot = new ClientSyncDatumSnapshot();
        snapshot.setClientSyncDataList(datum);
        // 序列化
        byte[] data = ApplicationUtils.getBean(Serializer.class).serialize(snapshot);
        return new DistroData(new DistroKey(DataOperation.SNAPSHOT.name(), TYPE), data);
    }
    
    @Override
    public List<DistroData> getVerifyData() {
        List<DistroData> result = null;
        // 遍历所有 客户端连接 id
        for (String each : clientManager.allClientId()) {
            Client client = clientManager.getClient(each);
            // 未获取客户端/不是临时客户端
            if (null == client || !client.isEphemeral()) {
                continue;
            }
            // 只处理 本服务端负责的客户端
            if (clientManager.isResponsibleClient(client)) {
                // 创建 Distro 客户端认证信息
                DistroClientVerifyInfo verifyData = new DistroClientVerifyInfo(client.getClientId(),
                        client.getRevision());
                DistroKey distroKey = new DistroKey(client.getClientId(), TYPE);
                //  Distro 数据： 将 DistroClientVerifyInfo 客户端认证信息 序列化成 字节数组
                // itodo： 此处为什么要进行序列化？
                DistroData data = new DistroData(distroKey,
                        ApplicationUtils.getBean(Serializer.class).serialize(verifyData));
                data.setType(DataOperation.VERIFY);
                if (result == null) {
                    result = new LinkedList<>();
                }
                result.add(data);
            }
        }
        return result;
    }
}
