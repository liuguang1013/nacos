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

package com.alibaba.nacos.naming.remote.rpc.handler;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.core.distributed.distro.DistroProtocol;
import com.alibaba.nacos.core.distributed.distro.entity.DistroData;
import com.alibaba.nacos.core.distributed.distro.entity.DistroKey;
import com.alibaba.nacos.core.remote.RequestHandler;
import com.alibaba.nacos.naming.cluster.remote.request.DistroDataRequest;
import com.alibaba.nacos.naming.cluster.remote.response.DistroDataResponse;
import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientDataProcessor;
import com.alibaba.nacos.naming.misc.Loggers;
import org.springframework.stereotype.Component;

/**
 * Distro data request handler.
 * 服务端之间 Distro 协议 数据请求处理器
 * @author xiweng.yy
 */
@Component
public class DistroDataRequestHandler extends RequestHandler<DistroDataRequest, DistroDataResponse> {
    
    private final DistroProtocol distroProtocol;
    
    public DistroDataRequestHandler(DistroProtocol distroProtocol) {
        this.distroProtocol = distroProtocol;
    }
    
    @Override
    public DistroDataResponse handle(DistroDataRequest request, RequestMeta meta) throws NacosException {
        try {
            switch (request.getDataOperation()) {
                case VERIFY:
                    // 处理验证：Distro 协议类启动后，定时任务：处理验证 其他服务端负责的客户端数据
                    return handleVerify(request.getDistroData(), meta);
                case SNAPSHOT:
                    // 处理快照：Distro 协议类启动时，加载数据使用
                    return handleSnapshot();
                case ADD:
                case CHANGE:
                case DELETE:
                    // 处理同步客户端数据请求
                    return handleSyncData(request.getDistroData());
                case QUERY:
                    return handleQueryData(request.getDistroData());
                default:
                    return new DistroDataResponse();
            }
        } catch (Exception e) {
            Loggers.DISTRO.error("[DISTRO-FAILED] distro handle with exception", e);
            DistroDataResponse result = new DistroDataResponse();
            result.setErrorCode(ResponseCode.FAIL.getCode());
            result.setMessage("handle distro request with exception");
            return result;
        }
    }
    
    private DistroDataResponse handleVerify(DistroData distroData, RequestMeta meta) {
        DistroDataResponse result = new DistroDataResponse();
        // 验证 客户端数据
        if (!distroProtocol.onVerify(distroData, meta.getClientIp())) {
            result.setErrorInfo(ResponseCode.FAIL.getCode(), "[DISTRO-FAILED] distro data verify failed");
        }
        return result;
    }
    
    private DistroDataResponse handleSnapshot() {
        DistroDataResponse result = new DistroDataResponse();
        // 在 distro 协议对象中获取 快照信息
        DistroData distroData = distroProtocol.onSnapshot(DistroClientDataProcessor.TYPE);
        result.setDistroData(distroData);
        return result;
    }

    /**
     * 处理 同步服务实例新增/删除 数据（即客户端连接的新增/删除）
     * @param distroData
     * @return
     */
    private DistroDataResponse handleSyncData(DistroData distroData) {
        DistroDataResponse result = new DistroDataResponse();
        if (!distroProtocol.onReceive(distroData)) {
            result.setErrorCode(ResponseCode.FAIL.getCode());
            result.setMessage("[DISTRO-FAILED] distro data handle failed");
        }
        return result;
    }
    
    private DistroDataResponse handleQueryData(DistroData distroData) {
        DistroDataResponse result = new DistroDataResponse();
        DistroKey distroKey = distroData.getDistroKey();
        DistroData queryData = distroProtocol.onQuery(distroKey);
        result.setDistroData(queryData);
        return result;
    }
}
