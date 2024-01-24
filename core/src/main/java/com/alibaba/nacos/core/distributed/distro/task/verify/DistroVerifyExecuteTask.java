/*
 * Copyright 1999-2020 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.core.distributed.distro.task.verify;

import com.alibaba.nacos.common.task.AbstractExecuteTask;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.monitor.DistroRecord;
import com.alibaba.nacos.core.distributed.distro.monitor.DistroRecordsHolder;
import com.alibaba.nacos.core.utils.Loggers;

import java.util.List;

/**
 * Execute distro verify task.
 *
 * @author xiweng.yy
 */
public class DistroVerifyExecuteTask extends AbstractExecuteTask {
    /**
     * 传输代理
     */
    private final DistroTransportAgent transportAgent;

    /**
     * 当前客户端负责的客户端对象
     */
    private final List<DistroData> verifyData;

    /**
     * 目标服务端：ip：port
     */
    private final String targetServer;

    /**
     *  value：Nacos:Naming:v2:ClientData
     */
    private final String resourceType;
    
    public DistroVerifyExecuteTask(DistroTransportAgent transportAgent, List<DistroData> verifyData,
            String targetServer, String resourceType) {
        this.transportAgent = transportAgent;
        this.verifyData = verifyData;
        this.targetServer = targetServer;
        this.resourceType = resourceType;
    }
    
    @Override
    public void run() {
        // 遍历：客户端连接信息
        for (DistroData each : verifyData) {
            try {
                // 判断是否支持回调，默认支持
                if (transportAgent.supportCallbackTransport()) {
                    doSyncVerifyDataWithCallback(each);
                } else {
                    doSyncVerifyData(each);
                }
            } catch (Exception e) {
                Loggers.DISTRO
                        .error("[DISTRO-FAILED] verify data for type {} to {} failed.", resourceType, targetServer, e);
            }
        }
    }
    
    private void doSyncVerifyDataWithCallback(DistroData data) {
        // 同步 认证数据
        transportAgent.syncVerifyData(data, targetServer, new DistroVerifyCallback());
    }
    
    private void doSyncVerifyData(DistroData data) {
        transportAgent.syncVerifyData(data, targetServer);
    }
    
    private class DistroVerifyCallback implements DistroCallback {
        
        @Override
        public void onSuccess() {
            // 日志输出
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO.debug("[DISTRO] verify data for type {} to {} success", resourceType, targetServer);
            }
        }
        
        @Override
        public void onFailed(Throwable throwable) {
            // 记录失败次数
            DistroRecord distroRecord = DistroRecordsHolder.getInstance().getRecord(resourceType);
            distroRecord.verifyFail();
            if (Loggers.DISTRO.isDebugEnabled()) {
                Loggers.DISTRO
                        .debug("[DISTRO-FAILED] verify data for type {} to {} failed.", resourceType, targetServer,
                                throwable);
            }
        }
    }
}
