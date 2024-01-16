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

package com.alibaba.nacos.naming.push.v2.executor;

import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.pojo.Subscriber;
import com.alibaba.nacos.naming.push.v2.PushDataWrapper;
import com.alibaba.nacos.naming.push.v2.task.NamingPushCallback;
import org.springframework.stereotype.Component;

import java.util.Optional;

/**
 * Delegate for push execute service.
 *
 * @author xiweng.yy
 */
@SuppressWarnings("PMD.ServiceOrDaoClassShouldEndWithImplRule")
@Component
public class PushExecutorDelegate implements PushExecutor {
    
    private final PushExecutorRpcImpl rpcPushExecuteService;
    
    private final PushExecutorUdpImpl udpPushExecuteService;
    
    public PushExecutorDelegate(PushExecutorRpcImpl rpcPushExecuteService, PushExecutorUdpImpl udpPushExecuteService) {
        this.rpcPushExecuteService = rpcPushExecuteService;
        this.udpPushExecuteService = udpPushExecuteService;
    }
    
    @Override
    public void doPush(String clientId, Subscriber subscriber, PushDataWrapper data) {
        getPushExecuteService(clientId, subscriber).doPush(clientId, subscriber, data);
    }

    /**
     * 执行推送，带有回调信息
     * @param clientId   client id 客户端id
     * @param subscriber subscriber 订阅者
     * @param data       push data 推送数据
     * @param callBack   callback 回调
     */
    @Override
    public void doPushWithCallback(String clientId, Subscriber subscriber, PushDataWrapper data,
            NamingPushCallback callBack) {
        getPushExecuteService(clientId, subscriber).doPushWithCallback(clientId, subscriber, data, callBack);
    }
    
    private PushExecutor getPushExecuteService(String clientId, Subscriber subscriber) {
        Optional<SpiPushExecutor> result = SpiImplPushExecutorHolder.getInstance()
                .findPushExecutorSpiImpl(clientId, subscriber);
        // 通过 spi 查找 SpiPushExecutor.class 的实现类
        if (result.isPresent()) {
            return result.get();
        }
        // use nacos default push executor
        // 为查询到 spi 的实现类，使用 默认的 推送执行器
        // 客户端id 包含 # 使用 udp 的推送，否则使用 grpc 推送
        return clientId.contains(IpPortBasedClient.ID_DELIMITER) ? udpPushExecuteService : rpcPushExecuteService;
    }
}
