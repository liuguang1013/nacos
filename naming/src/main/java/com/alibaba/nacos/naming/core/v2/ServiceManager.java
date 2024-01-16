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

package com.alibaba.nacos.naming.core.v2;

import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.naming.core.v2.event.metadata.MetadataEvent;
import com.alibaba.nacos.naming.core.v2.pojo.Service;

import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Nacos service manager for v2.
 *
 * @author xiweng.yy
 */
public class ServiceManager {
    
    private static final ServiceManager INSTANCE = new ServiceManager();

    /**
     * 保存 服务对象
     * key 和 value 是一个对象
     * 放入 value 的时，触发 MetadataEvent.ServiceMetadataEvent
     */
    private final ConcurrentHashMap<Service, Service> singletonRepository;

    /**
     * 缓存 namespace 命名空间 对应的服务信息
     */
    private final ConcurrentHashMap<String, Set<Service>> namespaceSingletonMaps;

    /**
     * 初始化 服务管理者
     */
    private ServiceManager() {
        // 单例仓库 初始化大小 1024
        singletonRepository = new ConcurrentHashMap<>(1 << 10);
        // 命名空间单例 map 初始化大小 4
        namespaceSingletonMaps = new ConcurrentHashMap<>(1 << 2);
    }
    
    public static ServiceManager getInstance() {
        return INSTANCE;
    }
    
    public Set<Service> getSingletons(String namespace) {
        return namespaceSingletonMaps.getOrDefault(namespace, new HashSet<>(1));
    }
    
    /**
     * Get singleton service. Put to manager if no singleton.
     * 获取单例对象，如果没有，交给管理者
     *
     * @param service new service
     * @return if service is exist, return exist service, otherwise return new service
     */
    public Service getSingleton(Service service) {
        // 发布服务元数据事件：实际就是更新服务状态为：未过期
        singletonRepository.computeIfAbsent(service, key -> {
            NotifyCenter.publishEvent(new MetadataEvent.ServiceMetadataEvent(service, false));
            return service;
        });
        Service result = singletonRepository.get(service);
        // 向 命名空间的服务单例 map 中增加缓存
        namespaceSingletonMaps.computeIfAbsent(result.getNamespace(), namespace -> new ConcurrentHashSet<>());
        namespaceSingletonMaps.get(result.getNamespace()).add(result);
        return result;
    }
    
    /**
     * Get singleton service if Exist.
     *
     * @param namespace namespace of service
     * @param group     group of service
     * @param name      name of service
     * @return singleton service if exist, otherwise null optional
     */
    public Optional<Service> getSingletonIfExist(String namespace, String group, String name) {
        return getSingletonIfExist(Service.newService(namespace, group, name));
    }
    
    /**
     * Get singleton service if Exist.
     *
     * @param service service template
     * @return singleton service if exist, otherwise null optional
     */
    public Optional<Service> getSingletonIfExist(Service service) {
        return Optional.ofNullable(singletonRepository.get(service));
    }
    
    public Set<String> getAllNamespaces() {
        return namespaceSingletonMaps.keySet();
    }
    
    /**
     * Remove singleton service.
     *
     * @param service service need to remove
     * @return removed service
     */
    public Service removeSingleton(Service service) {
        if (namespaceSingletonMaps.containsKey(service.getNamespace())) {
            namespaceSingletonMaps.get(service.getNamespace()).remove(service);
        }
        return singletonRepository.remove(service);
    }
    
    public boolean containSingleton(Service service) {
        return singletonRepository.containsKey(service);
    }
    
    public int size() {
        return singletonRepository.size();
    }
}
