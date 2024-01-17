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

package com.alibaba.nacos.client.naming.cache;

import com.alibaba.nacos.api.PropertyKeyConst;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.Instance;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.api.naming.utils.NamingUtils;
import com.alibaba.nacos.client.env.NacosClientProperties;
import com.alibaba.nacos.client.monitor.MetricsMonitor;
import com.alibaba.nacos.client.naming.backups.FailoverReactor;
import com.alibaba.nacos.client.naming.event.InstancesChangeEvent;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.utils.ConvertUtils;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Naming client service information holder.
 *
 * @author xiweng.yy
 */
public class ServiceInfoHolder implements Closeable {
    
    private static final String JM_SNAPSHOT_PATH_PROPERTY = "JM.SNAPSHOT.PATH";
    
    private static final String FILE_PATH_NACOS = "nacos";
    
    private static final String FILE_PATH_NAMING = "naming";
    
    private static final String USER_HOME_PROPERTY = "user.home";
    /**
     * 服务信息缓存
     * key: ${groupName}@@${service}@@${clusters}
     */
    private final ConcurrentMap<String, ServiceInfo> serviceInfoMap;
    
    private final FailoverReactor failoverReactor;

    /**
     * 推送空值保护 开关
     */
    private final boolean pushEmptyProtection;
    
    private String cacheDir;
    
    private String notifierEventScope;
    
    public ServiceInfoHolder(String namespace, String notifierEventScope, NacosClientProperties properties) {
        // 初始化缓存路径
        initCacheDir(namespace, properties);
        // 判断是否在开始时候 加载缓存
        if (isLoadCacheAtStart(properties)) {
            this.serviceInfoMap = new ConcurrentHashMap<>(DiskCache.read(this.cacheDir));
        } else {
            this.serviceInfoMap = new ConcurrentHashMap<>(16);
        }
        // 创建失败恢复器
        // itodo：失败恢复器什么时候起作用？解决：失败恢复开关打开的时候 使用这个对象返回服务信息对象
        this.failoverReactor = new FailoverReactor(this, cacheDir);
        // 获取配置 推送空保护
        // itodo：推送空保护什么时候起作用？
        //  解决：有新客户端注册到服务端，服务端会推送服务信息，推送的数据可能存在错误数据，开启开关则会进行检验
        this.pushEmptyProtection = isPushEmptyProtect(properties);
        this.notifierEventScope = notifierEventScope;
    }
    
    private void initCacheDir(String namespace, NacosClientProperties properties) {
        // JM.SNAPSHOT.PATH 参数
        String jmSnapshotPath = properties.getProperty(JM_SNAPSHOT_PATH_PROPERTY);
    
        String namingCacheRegistryDir = "";
        if (properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR) != null) {
            namingCacheRegistryDir = File.separator + properties.getProperty(PropertyKeyConst.NAMING_CACHE_REGISTRY_DIR);
        }
        
        if (!StringUtils.isBlank(jmSnapshotPath)) {
            //  缓存地址： ${jmSnapshotPath}/nacos/${namingCacheRegistryDir}/naming/${namespace}
            cacheDir = jmSnapshotPath + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        } else {
            //  缓存地址： ${user.home}/nacos/${namingCacheRegistryDir}/naming/${namespace}
            cacheDir = properties.getProperty(USER_HOME_PROPERTY) + File.separator + FILE_PATH_NACOS + namingCacheRegistryDir
                    + File.separator + FILE_PATH_NAMING + File.separator + namespace;
        }
    }

    /**
     * 获取属性：开始的时候加载缓存 namingLoadCacheAtStart
     */
    private boolean isLoadCacheAtStart(NacosClientProperties properties) {
        boolean loadCacheAtStart = false;
        if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START))) {
            loadCacheAtStart = ConvertUtils
                    .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_LOAD_CACHE_AT_START));
        }
        return loadCacheAtStart;
    }

    /**
     * 获取属性：开始的时候加载缓存
     */
    private boolean isPushEmptyProtect(NacosClientProperties properties) {
        boolean pushEmptyProtection = false;
        if (properties != null && StringUtils
                .isNotEmpty(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION))) {
            pushEmptyProtection = ConvertUtils
                    .toBoolean(properties.getProperty(PropertyKeyConst.NAMING_PUSH_EMPTY_PROTECTION));
        }
        return pushEmptyProtection;
    }
    
    public Map<String, ServiceInfo> getServiceInfoMap() {
        return serviceInfoMap;
    }
    
    public ServiceInfo getServiceInfo(final String serviceName, final String groupName, final String clusters) {
        NAMING_LOGGER.debug("failover-mode: {}", failoverReactor.isFailoverSwitch());
        String groupedServiceName = NamingUtils.getGroupedName(serviceName, groupName);
        String key = ServiceInfo.getKey(groupedServiceName, clusters);
        // 判断失败恢复开关是否打开
        if (failoverReactor.isFailoverSwitch()) {
            return failoverReactor.getService(key);
        }
        return serviceInfoMap.get(key);
    }
    
    /**
     * Process service json.
     *
     * @param json service json
     * @return service info
     */
    public ServiceInfo processServiceInfo(String json) {
        ServiceInfo serviceInfo = JacksonUtils.toObj(json, ServiceInfo.class);
        serviceInfo.setJsonFromServer(json);
        return processServiceInfo(serviceInfo);
    }
    
    /**
     * Process service info.
     *
     * @param serviceInfo new service info
     * @return service info
     */
    public ServiceInfo processServiceInfo(ServiceInfo serviceInfo) {
        //获取 ${groupName}@@${serviceName}@@${clusters}
        String serviceKey = serviceInfo.getKey();
        if (serviceKey == null) {
            return null;
        }
        ServiceInfo oldService = serviceInfoMap.get(serviceInfo.getKey());
        if (isEmptyOrErrorPush(serviceInfo)) {
            //empty or error push, just ignore
            // 空或错误推送，忽略即可
            return oldService;
        }
        // 更新缓存信息
        serviceInfoMap.put(serviceInfo.getKey(), serviceInfo);
        // 判断 服务信息是否改变
        boolean changed = isChangedServiceInfo(oldService, serviceInfo);
        // 补充 字段信息
        if (StringUtils.isBlank(serviceInfo.getJsonFromServer())) {
            serviceInfo.setJsonFromServer(JacksonUtils.toJson(serviceInfo));
        }
        MetricsMonitor.getServiceInfoMapSizeMonitor().set(serviceInfoMap.size());
        // 如果服务信息发生改变，发布 InstancesChangeEvent 实例变化事件
        if (changed) {
            NAMING_LOGGER.info("current ips:({}) service: {} -> {}", serviceInfo.ipCount(), serviceInfo.getKey(),
                    JacksonUtils.toJson(serviceInfo.getHosts()));
            //发布 InstancesChangeEvent 实例变化事件：
            NotifyCenter.publishEvent(new InstancesChangeEvent(notifierEventScope, serviceInfo.getName(), serviceInfo.getGroupName(),
                    serviceInfo.getClusters(), serviceInfo.getHosts()));
            // 持久化到磁盘  ${user.home}/nacos/${namingCacheRegistryDir}/naming/${namespace}
            DiskCache.write(serviceInfo, cacheDir);
        }
        return serviceInfo;
    }
    
    private boolean isEmptyOrErrorPush(ServiceInfo serviceInfo) {
        return null == serviceInfo.getHosts() || (pushEmptyProtection && !serviceInfo.validate());
    }
    
    private boolean isChangedServiceInfo(ServiceInfo oldService, ServiceInfo newService) {
        // 之前不存在 该服务信息
        if (null == oldService) {
            NAMING_LOGGER.info("init new ips({}) service: {} -> {}", newService.ipCount(), newService.getKey(),
                    JacksonUtils.toJson(newService.getHosts()));
            return true;
        }
        // 缓存服务的刷新时间  >  推送服务的刷新时间，认为推送的是过期的数据
        if (oldService.getLastRefTime() > newService.getLastRefTime()) {
            NAMING_LOGGER.warn("out of date data received, old-t: {}, new-t: {}", oldService.getLastRefTime(),
                    newService.getLastRefTime());
            return false;
        }
        boolean changed = false;
        // key：  ip:port
        Map<String, Instance> oldHostMap = new HashMap<>(oldService.getHosts().size());
        for (Instance host : oldService.getHosts()) {
            oldHostMap.put(host.toInetAddr(), host);
        }
        Map<String, Instance> newHostMap = new HashMap<>(newService.getHosts().size());
        for (Instance host : newService.getHosts()) {
            newHostMap.put(host.toInetAddr(), host);
        }
        
        Set<Instance> modHosts = new HashSet<>();
        Set<Instance> newHosts = new HashSet<>();
        Set<Instance> remvHosts = new HashSet<>();
        
        List<Map.Entry<String, Instance>> newServiceHosts = new ArrayList<>(
                newHostMap.entrySet());
        for (Map.Entry<String, Instance> entry : newServiceHosts) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (oldHostMap.containsKey(key) && !StringUtils.equals(host.toString(), oldHostMap.get(key).toString())) {
                modHosts.add(host);
                continue;
            }
            
            if (!oldHostMap.containsKey(key)) {
                newHosts.add(host);
            }
        }
        
        for (Map.Entry<String, Instance> entry : oldHostMap.entrySet()) {
            Instance host = entry.getValue();
            String key = entry.getKey();
            if (newHostMap.containsKey(key)) {
                continue;
            }

            //add to remove hosts
            remvHosts.add(host);
        }

        if (newHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("new ips({}) service: {} -> {}", newHosts.size(), newService.getKey(),
                    JacksonUtils.toJson(newHosts));
        }
        
        if (remvHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("removed ips({}) service: {} -> {}", remvHosts.size(), newService.getKey(),
                    JacksonUtils.toJson(remvHosts));
        }
        
        if (modHosts.size() > 0) {
            changed = true;
            NAMING_LOGGER.info("modified ips({}) service: {} -> {}", modHosts.size(), newService.getKey(),
                    JacksonUtils.toJson(modHosts));
        }
        return changed;
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        failoverReactor.shutdown();
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }
}
