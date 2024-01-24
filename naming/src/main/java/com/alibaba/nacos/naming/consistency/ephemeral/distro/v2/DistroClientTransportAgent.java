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

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.consistency.DataOperation;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.NodeState;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.cluster.remote.ClusterRpcClientProxy;
import com.alibaba.nacos.core.distributed.distro.DistroConfig;
import com.alibaba.nacos.core.distributed.distro.component.DistroCallback;
import com.alibaba.nacos.core.distributed.distro.component.DistroTransportAgent;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.distributed.distro.exception.DistroException;
import com.alibaba.nacos.naming.cluster.remote.request.DistroDataRequest;
import com.alibaba.nacos.naming.cluster.remote.response.DistroDataResponse;
import com.alibaba.nacos.naming.core.v2.event.client.ClientEvent;
import com.alibaba.nacos.naming.misc.GlobalExecutor;
import com.alibaba.nacos.naming.misc.Loggers;
import com.alibaba.nacos.naming.monitor.NamingTpsMonitor;

import java.util.concurrent.Executor;

/**
 * Distro transport agent for v2.
 * Distro 服务端请求代理
 *  1、发送 distro 协议启动时的延迟的认证请求
 *  2、发送 distro 协议启动时的获取其他服务端的快照的请求
 *  3、发送 服务实例的新增/删除 的同步数据请求
 * @author xiweng.yy
 */
public class DistroClientTransportAgent implements DistroTransportAgent {

    /**
     * 集群间的 rpc 客户端代理
     */
    private final ClusterRpcClientProxy clusterRpcClientProxy;

    /**
     * 服务端成员管理对象
     */
    private final ServerMemberManager memberManager;
    
    public DistroClientTransportAgent(ClusterRpcClientProxy clusterRpcClientProxy,
            ServerMemberManager serverMemberManager) {
        this.clusterRpcClientProxy = clusterRpcClientProxy;
        this.memberManager = serverMemberManager;
    }
    
    @Override
    public boolean supportCallbackTransport() {
        return true;
    }

    /**
     * 发送 同步数据的 同步请求
     * @param data         data
     * @param targetServer target server
     * @return
     */
    @Override
    public boolean syncData(DistroData data, String targetServer) {
        if (isNoExistTarget(targetServer)) {
            return true;
        }
        DistroDataRequest request = new DistroDataRequest(data, data.getType());
        Member member = memberManager.find(targetServer);
        if (checkTargetServerStatusUnhealthy(member)) {
            Loggers.DISTRO
                    .warn("[DISTRO] Cancel distro sync caused by target server {} unhealthy, key: {}", targetServer,
                            data.getDistroKey());
            return false;
        }
        try {
            Response response = clusterRpcClientProxy.sendRequest(member, request);
            return checkResponse(response);
        } catch (NacosException e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] Sync distro data failed! key: {}", data.getDistroKey(), e);
        }
        return false;
    }

    /**
     * 发送 同步数据的 异步请求： 通过 distro 立即执行引擎发送该类请求
     * @param data         data
     * @param targetServer target server
     * @param callback     callback
     */
    @Override
    public void syncData(DistroData data, String targetServer, DistroCallback callback) {
        if (isNoExistTarget(targetServer)) {
            callback.onSuccess();
            return;
        }
        DistroDataRequest request = new DistroDataRequest(data, data.getType());
        Member member = memberManager.find(targetServer);
        if (checkTargetServerStatusUnhealthy(member)) {
            Loggers.DISTRO
                    .warn("[DISTRO] Cancel distro sync caused by target server {} unhealthy, key: {}", targetServer,
                            data.getDistroKey());
            callback.onFailed(null);
            return;
        }
        try {
            clusterRpcClientProxy.asyncRequest(member, request, new DistroRpcCallbackWrapper(callback, member));
        } catch (NacosException nacosException) {
            callback.onFailed(nacosException);
        }
    }
    
    @Override
    public boolean syncVerifyData(DistroData verifyData, String targetServer) {
        if (isNoExistTarget(targetServer)) {
            return true;
        }
        // replace target server as self server so that can callback.
        verifyData.getDistroKey().setTargetServer(memberManager.getSelf().getAddress());
        DistroDataRequest request = new DistroDataRequest(verifyData, DataOperation.VERIFY);
        Member member = memberManager.find(targetServer);
        if (checkTargetServerStatusUnhealthy(member)) {
            Loggers.DISTRO
                    .warn("[DISTRO] Cancel distro verify caused by target server {} unhealthy, key: {}", targetServer,
                            verifyData.getDistroKey());
            return false;
        }
        try {
            Response response = clusterRpcClientProxy.sendRequest(member, request);
            return checkResponse(response);
        } catch (NacosException e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] Verify distro data failed! key: {} ", verifyData.getDistroKey(), e);
        }
        return false;
    }

    /**
     * 同步认证数据 异步请求：
     * @param verifyData   verify data
     * @param targetServer target server
     * @param callback     callback
     */
    @Override
    public void syncVerifyData(DistroData verifyData, String targetServer, DistroCallback callback) {
        // 发送认证数据前，检查服务端成员是否存在
        if (isNoExistTarget(targetServer)) {
            callback.onSuccess();
            return;
        }
        // 创建 DistroDataRequest 认证类型请求
        DistroDataRequest request = new DistroDataRequest(verifyData, DataOperation.VERIFY);
        Member member = memberManager.find(targetServer);
        // 检查 服务端成员是否处于不健康状态
        if (checkTargetServerStatusUnhealthy(member)) {
            Loggers.DISTRO
                    .warn("[DISTRO] Cancel distro verify caused by target server {} unhealthy, key: {}", targetServer,
                            verifyData.getDistroKey());
            callback.onFailed(null);
            return;
        }
        try {
            // 对回调进行包装：
            DistroVerifyCallbackWrapper wrapper = new DistroVerifyCallbackWrapper(targetServer,
                    verifyData.getDistroKey().getResourceKey(), callback, member);
            // 发送异步请求
            clusterRpcClientProxy.asyncRequest(member, request, wrapper);
        } catch (NacosException nacosException) {
            callback.onFailed(nacosException);
        }
    }
    
    @Override
    public DistroData getData(DistroKey key, String targetServer) {
        Member member = memberManager.find(targetServer);
        if (checkTargetServerStatusUnhealthy(member)) {
            throw new DistroException(
                    String.format("[DISTRO] Cancel get snapshot caused by target server %s unhealthy", targetServer));
        }
        DistroDataRequest request = new DistroDataRequest();
        DistroData distroData = new DistroData();
        distroData.setDistroKey(key);
        distroData.setType(DataOperation.QUERY);
        request.setDistroData(distroData);
        request.setDataOperation(DataOperation.QUERY);
        try {
            Response response = clusterRpcClientProxy.sendRequest(member, request);
            if (checkResponse(response)) {
                return ((DistroDataResponse) response).getDistroData();
            } else {
                throw new DistroException(
                        String.format("[DISTRO-FAILED] Get data request to %s failed, code: %d, message: %s",
                                targetServer, response.getErrorCode(), response.getMessage()));
            }
        } catch (NacosException e) {
            throw new DistroException("[DISTRO-FAILED] Get distro data failed! ", e);
        }
    }

    /**
     * 发送获取其他服务端的客户端数据的 同步请求：Distro 类初始化加载时调用
     * @param targetServer target server.
     * @return
     */
    @Override
    public DistroData getDatumSnapshot(String targetServer) {
        // 获取成员对象
        Member member = memberManager.find(targetServer);
        // 检查 请求服务端的 健康状态：服务端之间的连接也是通过 grpc 长连接，会有心跳请求，当 3次请求失败后，会标记为不健康状态
        if (checkTargetServerStatusUnhealthy(member)) {
            throw new DistroException(
                    String.format("[DISTRO] Cancel get snapshot caused by target server %s unhealthy", targetServer));
        }
        // 创建 DistroDataRequest 请求，类型是 快照获取
        DistroDataRequest request = new DistroDataRequest();
        request.setDataOperation(DataOperation.SNAPSHOT);
        try {
            // 发送同步请求
            Response response = clusterRpcClientProxy
                    .sendRequest(member, request, DistroConfig.getInstance().getLoadDataTimeoutMillis());
            if (checkResponse(response)) {
                return ((DistroDataResponse) response).getDistroData();
            } else {
                throw new DistroException(
                        String.format("[DISTRO-FAILED] Get snapshot request to %s failed, code: %d, message: %s",
                                targetServer, response.getErrorCode(), response.getMessage()));
            }
        } catch (NacosException e) {
            throw new DistroException("[DISTRO-FAILED] Get distro snapshot failed! ", e);
        }
    }
    
    private boolean isNoExistTarget(String target) {
        return !memberManager.hasMember(target);
    }
    
    private boolean checkTargetServerStatusUnhealthy(Member member) {
        return null == member || !NodeState.UP.equals(member.getState()) || !clusterRpcClientProxy.isRunning(member);
    }
    
    private boolean checkResponse(Response response) {
        return ResponseCode.SUCCESS.getCode() == response.getResultCode();
    }
    
    private class DistroRpcCallbackWrapper implements RequestCallBack<Response> {
        
        private final DistroCallback distroCallback;
        
        private final Member member;
        
        public DistroRpcCallbackWrapper(DistroCallback distroCallback, Member member) {
            this.distroCallback = distroCallback;
            this.member = member;
        }
        
        @Override
        public Executor getExecutor() {
            return GlobalExecutor.getCallbackExecutor();
        }
        
        @Override
        public long getTimeout() {
            return DistroConfig.getInstance().getSyncTimeoutMillis();
        }
        
        @Override
        public void onResponse(Response response) {
            // 记录成功或者失败的次数
            if (checkResponse(response)) {
                NamingTpsMonitor.distroSyncSuccess(member.getAddress(), member.getIp());
                distroCallback.onSuccess();
            } else {
                NamingTpsMonitor.distroSyncFail(member.getAddress(), member.getIp());
                distroCallback.onFailed(null);
            }
        }
        
        @Override
        public void onException(Throwable e) {
            distroCallback.onFailed(e);
        }
    }
    
    private class DistroVerifyCallbackWrapper implements RequestCallBack<Response> {
        
        private final String targetServer;
        
        private final String clientId;
        
        private final DistroCallback distroCallback;
        
        private final Member member;
        
        private DistroVerifyCallbackWrapper(String targetServer, String clientId, DistroCallback distroCallback,
                Member member) {
            this.targetServer = targetServer;
            this.clientId = clientId;
            this.distroCallback = distroCallback;
            this.member = member;
        }
        
        @Override
        public Executor getExecutor() {
            return GlobalExecutor.getCallbackExecutor();
        }
        
        @Override
        public long getTimeout() {
            return DistroConfig.getInstance().getVerifyTimeoutMillis();
        }
        
        @Override
        public void onResponse(Response response) {
            if (checkResponse(response)) {
                NamingTpsMonitor.distroVerifySuccess(member.getAddress(), member.getIp());
                distroCallback.onSuccess();
            } else {
                Loggers.DISTRO.info("Target {} verify client {} failed, sync new client", targetServer, clientId);
                NotifyCenter.publishEvent(new ClientEvent.ClientVerifyFailedEvent(clientId, targetServer));
                NamingTpsMonitor.distroVerifyFail(member.getAddress(), member.getIp());
                distroCallback.onFailed(null);
            }
        }
        
        @Override
        public void onException(Throwable e) {
            distroCallback.onFailed(e);
        }
    }
}
