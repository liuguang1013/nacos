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

package com.alibaba.nacos.client.naming.event;

import com.alibaba.nacos.api.naming.listener.AbstractEventListener;
import com.alibaba.nacos.api.naming.listener.EventListener;
import com.alibaba.nacos.api.naming.listener.NamingEvent;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * A subscriber to notify eventListener callback.
 *  实例改变 通知器
 *  是 实例改变的事件 订阅者
 * @author horizonzy
 * @since 1.4.1
 */
public class InstancesChangeNotifier extends Subscriber<InstancesChangeEvent> {
    /**
     * UUID
     */
    private final String eventScope;
    /**
     * key ：serviceInfo 的key  ${groupName}@@${serviceName}@@${clusters}
     * value：服务的事件监听者
     */
    private final Map<String, ConcurrentHashSet<EventListener>> listenerMap = new ConcurrentHashMap<>();
    
    @JustForTest
    public InstancesChangeNotifier() {
        this.eventScope = UUID.randomUUID().toString();
    }
    
    public InstancesChangeNotifier(String eventScope) {
        this.eventScope = eventScope;
    }
    
    /**
     * register listener.
     * 注册监听者：在服务订阅的时候进行注册
     *
     * @param groupName   group name
     * @param serviceName serviceName
     * @param clusters    clusters, concat by ','. such as 'xxx,yyy'
     * @param listener    custom listener
     */
    public void registerListener(String groupName, String serviceName, String clusters, EventListener listener) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.computeIfAbsent(key, keyInner -> new ConcurrentHashSet<>());
        eventListeners.add(listener);
    }
    
    /**
     * deregister listener.
     *
     * @param groupName   group name
     * @param serviceName serviceName
     * @param clusters    clusters, concat by ','. such as 'xxx,yyy'
     * @param listener    custom listener
     */
    public void deregisterListener(String groupName, String serviceName, String clusters, EventListener listener) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.get(key);
        if (eventListeners == null) {
            return;
        }
        eventListeners.remove(listener);
        if (CollectionUtils.isEmpty(eventListeners)) {
            listenerMap.remove(key);
        }
    }
    
    /**
     * check serviceName,clusters is subscribed.
     * 检查服务是被订阅
     *
     * @param groupName   group name
     * @param serviceName serviceName
     * @param clusters    clusters, concat by ','. such as 'xxx,yyy'
     * @return is serviceName,clusters subscribed
     */
    public boolean isSubscribed(String groupName, String serviceName, String clusters) {
        String key = ServiceInfo.getKey(NamingUtils.getGroupedName(serviceName, groupName), clusters);
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.get(key);
        return CollectionUtils.isNotEmpty(eventListeners);
    }
    
    public List<ServiceInfo> getSubscribeServices() {
        List<ServiceInfo> serviceInfos = new ArrayList<>();
        for (String key : listenerMap.keySet()) {
            serviceInfos.add(ServiceInfo.fromKey(key));
        }
        return serviceInfos;
    }
    
    @Override
    public void onEvent(InstancesChangeEvent event) {
        String key = ServiceInfo
                .getKey(NamingUtils.getGroupedName(event.getServiceName(), event.getGroupName()), event.getClusters());
        ConcurrentHashSet<EventListener> eventListeners = listenerMap.get(key);
        if (CollectionUtils.isEmpty(eventListeners)) {
            return;
        }
        for (final EventListener listener : eventListeners) {
            // InstancesChangeEvent 事件 转换为 NamingEvent 事件
            final com.alibaba.nacos.api.naming.listener.Event namingEvent = transferToNamingEvent(event);
            // 监听者中 有执行器的 异步执行，否则直接执行
            if (listener instanceof AbstractEventListener && ((AbstractEventListener) listener).getExecutor() != null) {
                ((AbstractEventListener) listener).getExecutor().execute(() -> listener.onEvent(namingEvent));
            } else {
                listener.onEvent(namingEvent);
            }
        }
    }
    
    private com.alibaba.nacos.api.naming.listener.Event transferToNamingEvent(
            InstancesChangeEvent instancesChangeEvent) {
        return new NamingEvent(instancesChangeEvent.getServiceName(), instancesChangeEvent.getGroupName(),
                instancesChangeEvent.getClusters(), instancesChangeEvent.getHosts());
    }
    
    @Override
    public Class<? extends Event> subscribeType() {
        return InstancesChangeEvent.class;
    }
    
    @Override
    public boolean scopeMatches(InstancesChangeEvent event) {
        return this.eventScope.equals(event.scope());
    }
}
