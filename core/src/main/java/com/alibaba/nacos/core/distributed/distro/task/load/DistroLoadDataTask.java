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

package com.alibaba.nacos.core.distributed.distro.task.load;

import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.distributed.distro.DistroConfig;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroComponentHolder;
import com.alibaba.nacos.core.distributed.distro.component.DistroDataProcessor;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * Distro load data task.
 *
 * @author xiweng.yy
 */
public class DistroLoadDataTask implements Runnable {
    
    private final ServerMemberManager memberManager;
    
    private final DistroComponentHolder distroComponentHolder;
    
    private final DistroConfig distroConfig;
    
    private final DistroCallback loadCallback;

    /**
     * 缓存 DataStorageTypes 数据加载状况  任务只执行一次，不存在并发问题
     * key： 数据存储类型 Str ： 已知 Nacos:Naming:v2:ClientData
     * value： 是否已经加载完成
     */
    private final Map<String, Boolean> loadCompletedMap;
    
    public DistroLoadDataTask(ServerMemberManager memberManager, DistroComponentHolder distroComponentHolder,
            DistroConfig distroConfig, DistroCallback loadCallback) {
        this.memberManager = memberManager;
        this.distroComponentHolder = distroComponentHolder;
        this.distroConfig = distroConfig;
        this.loadCallback = loadCallback;
        loadCompletedMap = new HashMap<>(1);
    }
    
    @Override
    public void run() {
        try {
            // 开始加载
            load();

            if (!checkCompleted()) {
                // 延迟 再次执行一次，默认时间：30s
                GlobalExecutor.submitLoadDataTask(this, distroConfig.getLoadDataRetryDelayMillis());
            } else {
                loadCallback.onSuccess();
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot data success");
            }
        } catch (Exception e) {
            loadCallback.onFailed(e);
            Loggers.DISTRO.error("[DISTRO-INIT] load snapshot data failed. ", e);
        }
    }
    
    private void load() throws Exception {
        // 等待 服务端成员管理者 完成初始化
        while (memberManager.allMembersWithoutSelf().isEmpty()) {
            Loggers.DISTRO.info("[DISTRO-INIT] waiting server list init...");
            TimeUnit.SECONDS.sleep(1);
        }
        // 等待 distro 组件完成初始化
        while (distroComponentHolder.getDataStorageTypes().isEmpty()) {
            Loggers.DISTRO.info("[DISTRO-INIT] waiting distro data storage register...");
            TimeUnit.SECONDS.sleep(1);
        }
        // 遍历：数据存储类型，已知：Nacos:Naming:v2:ClientData
        for (String each : distroComponentHolder.getDataStorageTypes()) {
            // 未加载完成
            if (!loadCompletedMap.containsKey(each) || !loadCompletedMap.get(each)) {
                // 加载 其他服务端所有 客户端快照信息
                loadCompletedMap.put(each, loadAllDataSnapshotFromRemote(each));
            }
        }
    }
    
    private boolean loadAllDataSnapshotFromRemote(String resourceType) {
        DistroTransportAgent transportAgent = distroComponentHolder.findTransportAgent(resourceType);
        DistroDataProcessor dataProcessor = distroComponentHolder.findDataProcessor(resourceType);
        // 基本检查
        if (null == transportAgent || null == dataProcessor) {
            Loggers.DISTRO.warn("[DISTRO-INIT] Can't find component for type {}, transportAgent: {}, dataProcessor: {}",
                    resourceType, transportAgent, dataProcessor);
            return false;
        }
        // 遍历其他 服务端成员
        for (Member each : memberManager.allMembersWithoutSelf()) {
            long startTime = System.currentTimeMillis();
            try {
                Loggers.DISTRO.info("[DISTRO-INIT] load snapshot {} from {}", resourceType, each.getAddress());
                // 通过 DistroClientTransportAgent 代理发送同步请求， 获取 目标服务端的所有客户端信息
                DistroData distroData = transportAgent.getDatumSnapshot(each.getAddress());

                Loggers.DISTRO.info("[DISTRO-INIT] it took {} ms to load snapshot {} from {} and snapshot size is {}.",
                        System.currentTimeMillis() - startTime, resourceType, each.getAddress(),
                        getDistroDataLength(distroData));

                // 处理所有的客户端数据
                boolean result = dataProcessor.processSnapshot(distroData);
                Loggers.DISTRO
                        .info("[DISTRO-INIT] load snapshot {} from {} result: {}", resourceType, each.getAddress(),
                                result);
                if (result) {
                    distroComponentHolder.findDataStorage(resourceType).finishInitial();
                    return true;
                }
            } catch (Exception e) {
                Loggers.DISTRO.error("[DISTRO-INIT] load snapshot {} from {} failed.", resourceType, each.getAddress(), e);
            }
        }
        return false;
    }
    
    private static int getDistroDataLength(DistroData distroData) {
        return distroData != null && distroData.getContent() != null ? distroData.getContent().length : 0;
    }
    
    private boolean checkCompleted() {
        if (distroComponentHolder.getDataStorageTypes().size() != loadCompletedMap.size()) {
            return false;
        }
        for (Boolean each : loadCompletedMap.values()) {
            if (!each) {
                return false;
            }
        }
        return true;
    }
}
