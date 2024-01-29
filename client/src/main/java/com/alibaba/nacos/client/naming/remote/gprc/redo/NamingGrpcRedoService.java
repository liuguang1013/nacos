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

package com.alibaba.nacos.client.naming.remote.gprc.redo;

import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.naming.remote.gprc.NamingGrpcClientProxy;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.BatchInstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.InstanceRedoData;
import com.alibaba.nacos.client.naming.remote.gprc.redo.data.SubscriberRedoData;
import com.alibaba.nacos.client.utils.LogUtils;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.remote.client.ConnectionEventListener;

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Naming client gprc redo service.
 * 命名客户端重连服务
 * <p>When connection reconnect to server, redo the register and subscribe.
 * 当连接重新连接到服务器时，重做注册和订阅。
 * @author xiweng.yy
 */
public class NamingGrpcRedoService implements ConnectionEventListener {
    
    private static final String REDO_THREAD_NAME = "com.alibaba.nacos.client.naming.grpc.redo";
    
    private static final int REDO_THREAD = 1;
    
    /**
     * TODO get redo delay from config.
     */
    private static final long DEFAULT_REDO_DELAY = 3000L;

    /**
     * 已注册的实例 数据，实际就是 提供的服务
     * key: groupName + @@ + serviceName
     */
    private final ConcurrentMap<String, InstanceRedoData> registeredInstances = new ConcurrentHashMap<>();

    /**
     * 服务订阅者订阅者
     * key:  ${groupName}@@${serviceName}@@${clusters}
     *
     */
    private final ConcurrentMap<String, SubscriberRedoData> subscribes = new ConcurrentHashMap<>();
    
    private final ScheduledExecutorService redoExecutor;
    
    private volatile boolean connected = false;
    
    public NamingGrpcRedoService(NamingGrpcClientProxy clientProxy) {
        // 指定 线程池的核心线程数 1
        this.redoExecutor = new ScheduledThreadPoolExecutor(REDO_THREAD, new NameThreadFactory(REDO_THREAD_NAME));
        // 初始 3s 延迟，之后 每3s 执行一次
        this.redoExecutor.scheduleWithFixedDelay(new RedoScheduledTask(clientProxy, this), DEFAULT_REDO_DELAY,
                DEFAULT_REDO_DELAY, TimeUnit.MILLISECONDS);
    }
    
    public boolean isConnected() {
        return connected;
    }

    /**
     * 客户端初次启动成功 或者 重新连接服务端的时候
     * 触发 连接事件
     */
    @Override
    public void onConnected() {
        connected = true;
        LogUtils.NAMING_LOGGER.info("Grpc connection connect");
    }

    /**
     * 在客户端关闭 RpcClientStatus.SHUTDOWN 或者 重新连接服务端成功后关闭之前连接
     * 会触发 断开连接事件
     */
    @Override
    public void onDisConnect() {
        connected = false;
        LogUtils.NAMING_LOGGER.warn("Grpc connection disconnect, mark to redo");
        // 将为客户端提供服务的实例，标记为：未注册
        synchronized (registeredInstances) {
            registeredInstances.values().forEach(instanceRedoData -> instanceRedoData.setRegistered(false));
        }
        // 将订阅客户端的服务实例，标记为：未注册
        synchronized (subscribes) {
            subscribes.values().forEach(subscriberRedoData -> subscriberRedoData.setRegistered(false));
        }
        LogUtils.NAMING_LOGGER.warn("mark to redo completed");
    }
    
    /**
     * Cache registered instance for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param instance    registered instance
     */
    public void cacheInstanceForRedo(String serviceName, String groupName, Instance instance) {
        // groupName + @@ + serviceName
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        InstanceRedoData redoData = InstanceRedoData.build(serviceName, groupName, instance);
        synchronized (registeredInstances) {
            registeredInstances.put(key, redoData);
        }
    }
    
    /**
     * Cache registered instance for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param instances    batch registered instance
     */
    public void cacheInstanceForRedo(String serviceName, String groupName, List<Instance> instances) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        BatchInstanceRedoData redoData = BatchInstanceRedoData.build(serviceName, groupName, instances);
        synchronized (registeredInstances) {
            registeredInstances.put(key, redoData);
        }
    }
    
    /**
     * Instance register successfully, mark registered status as {@code true}.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void instanceRegistered(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.registered();
            }
        }
    }
    
    /**
     * Instance deregister, mark unregistering status as {@code true}.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void instanceDeregister(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
                redoData.setExpectedRegistered(false);
            }
        }
    }
    
    /**
     * Instance deregister finished, mark unregistered status.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void instanceDeregistered(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData) {
                redoData.unregistered();
            }
        }
    }
    
    /**
     * Remove registered instance for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     */
    public void removeInstanceForRedo(String serviceName, String groupName) {
        String key = NamingUtils.getGroupedName(serviceName, groupName);
        synchronized (registeredInstances) {
            InstanceRedoData redoData = registeredInstances.get(key);
            if (null != redoData && !redoData.isExpectedRegistered()) {
                registeredInstances.remove(key);
            }
        }
    }
    
    /**
     * Find all instance redo data which need do redo.
     * 找到所有需要重连的服务数据
     *
     * @return set of {@code InstanceRedoData} need to do redo.
     */
    public Set<InstanceRedoData> findInstanceRedoData() {
        Set<InstanceRedoData> result = new HashSet<>();
        synchronized (registeredInstances) {
            for (InstanceRedoData each : registeredInstances.values()) {
                if (each.isNeedRedo()) {
                    result.add(each);
                }
            }
        }
        return result;
    }
    
    /**
     * Cache subscriber for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void cacheSubscriberForRedo(String serviceName, String groupName, String cluster) {
        // ${groupName}@@${serviceName}@@${clusters}
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        // 构建重连对象
        SubscriberRedoData redoData = SubscriberRedoData.build(serviceName, groupName, cluster);
        synchronized (subscribes) {
            subscribes.put(key, redoData);
        }
    }
    
    /**
     * Subscriber register successfully, mark registered status as {@code true}.
     * 订阅者注册成功，将注册状态标记为{@code true}。
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void subscriberRegistered(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData) {
                redoData.setRegistered(true);
            }
        }
    }
    
    /**
     * Subscriber deregister, mark unregistering status as {@code true}.
     * 订阅者注销，将注销状态标记为{@code true}。
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void subscriberDeregister(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData) {
                redoData.setUnregistering(true);
                redoData.setExpectedRegistered(false);
            }
        }
    }
    
    /**
     * Judge subscriber has registered to server.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     * @return {@code true} if subscribed, otherwise {@code false}
     */
    public boolean isSubscriberRegistered(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            return null != redoData && redoData.isRegistered();
        }
    }
    
    /**
     * Remove subscriber for redo.
     *
     * @param serviceName service name
     * @param groupName   group name
     * @param cluster     cluster
     */
    public void removeSubscriberForRedo(String serviceName, String groupName, String cluster) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), cluster);
        synchronized (subscribes) {
            SubscriberRedoData redoData = subscribes.get(key);
            if (null != redoData && !redoData.isExpectedRegistered()) {
                subscribes.remove(key);
            }
        }
    }
    
    /**
     * Find all subscriber redo data which need do redo.
     *
     * @return set of {@code SubscriberRedoData} need to do redo.
     */
    public Set<SubscriberRedoData> findSubscriberRedoData() {
        Set<SubscriberRedoData> result = new HashSet<>();
        synchronized (subscribes) {
            for (SubscriberRedoData each : subscribes.values()) {
                if (each.isNeedRedo()) {
                    result.add(each);
                }
            }
        }
        return result;
    }
    
    /**
     * get Cache service.
     * @return cache service
     */
    public InstanceRedoData getRegisteredInstancesByKey(String combinedServiceName) {
        return registeredInstances.get(combinedServiceName);
    }
    
    /**
     * Shutdown redo service.
     */
    public void shutdown() {
        LogUtils.NAMING_LOGGER.info("Shutdown grpc redo service executor " + redoExecutor);
        registeredInstances.clear();
        subscribes.clear();
        redoExecutor.shutdownNow();
    }
    
}
