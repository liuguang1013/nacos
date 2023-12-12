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

package com.alibaba.nacos.core.remote.grpc;

import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.grpc.auto.BiRequestStreamGrpc;
import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.api.remote.request.ConnectResetRequest;
import com.alibaba.nacos.api.remote.request.ConnectionSetupRequest;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.grpc.GrpcUtils;
import com.alibaba.nacos.core.remote.Connection;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.core.remote.ConnectionMeta;
import com.alibaba.nacos.core.remote.RpcAckCallbackSynchronizer;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.Map;

/**
 * grpc bi stream request .
 *
 * @author liuzunfei
 * @version $Id: GrpcBiStreamRequest.java, v 0.1 2020年09月01日 10:41 PM liuzunfei Exp $
 */
@Service
public class GrpcBiStreamRequestAcceptor extends BiRequestStreamGrpc.BiRequestStreamImplBase {
    
    @Autowired
    ConnectionManager connectionManager;
    
    private void traceDetailIfNecessary(Payload grpcRequest) {
        String clientIp = grpcRequest.getMetadata().getClientIp();
        String connectionId = GrpcServerConstants.CONTEXT_KEY_CONN_ID.get();
        try {
            if (connectionManager.traced(clientIp)) {
                Loggers.REMOTE_DIGEST.info("[{}]Bi stream request receive, meta={},body={}", connectionId,
                        grpcRequest.getMetadata().toByteString().toStringUtf8(),
                        grpcRequest.getBody().toByteString().toStringUtf8());
            }
        } catch (Throwable throwable) {
            Loggers.REMOTE_DIGEST.error("[{}]Bi stream request error,payload={},error={}", connectionId,
                    grpcRequest.toByteString().toStringUtf8(), throwable);
        }
        
    }
    
    @Override
    public StreamObserver<Payload> requestBiStream(StreamObserver<Payload> responseObserver) {

        // 创建 流 的观察者
        StreamObserver<Payload> streamObserver = new StreamObserver<Payload>() {
            /**
             * grpc 连接id
             */
            final String connectionId = GrpcServerConstants.CONTEXT_KEY_CONN_ID.get();
            /**
             * 本地端口号
             */
            final Integer localPort = GrpcServerConstants.CONTEXT_KEY_CONN_LOCAL_PORT.get();

            /**
             * 远程端口号
             */
            final int remotePort = GrpcServerConstants.CONTEXT_KEY_CONN_REMOTE_PORT.get();

            /**
             * 远程ip
             */
            String remoteIp = GrpcServerConstants.CONTEXT_KEY_CONN_REMOTE_IP.get();
            /**
             * 客户端 ip
             */
            String clientIp = "";
            
            @Override
            public void onNext(Payload payload) {
                // 在请求头中获取客户端的 ip，客户端每次请求都会在 对象转 payload 时候加上
                clientIp = payload.getMetadata().getClientIp();
                traceDetailIfNecessary(payload);
                
                Object parseObj;
                try {
                    parseObj = GrpcUtils.parse(payload);
                } catch (Throwable throwable) {
                    Loggers.REMOTE_DIGEST
                            .warn("[{}]Grpc request bi stream,payload parse error={}", connectionId, throwable);
                    return;
                }
                // 请求空值检查
                if (parseObj == null) {
                    Loggers.REMOTE_DIGEST
                            .warn("[{}]Grpc request bi stream,payload parse null ,body={},meta={}", connectionId,
                                    payload.getBody().getValue().toStringUtf8(), payload.getMetadata());
                    return;
                }
                // 判断是 连接重置请求
                // todo： 什么时候发的 ConnectionSetupRequest？客户端 connectToServer() 时，发送该流式请求
                if (parseObj instanceof ConnectionSetupRequest) {
                    ConnectionSetupRequest setUpRequest = (ConnectionSetupRequest) parseObj;
                    Map<String, String> labels = setUpRequest.getLabels();
                    String appName = "-";
                    if (labels != null && labels.containsKey(Constants.APPNAME)) {
                        appName = labels.get(Constants.APPNAME);
                    }
                    // 创建 连接元数据： 连接id 、客户端IP + 端口、服务端端IP + 端口、连接类型、客户端版本、app 名、标签
                    ConnectionMeta metaInfo = new ConnectionMeta(connectionId, payload.getMetadata().getClientIp(),
                            remoteIp, remotePort, localPort, ConnectionType.GRPC.getType(),
                            setUpRequest.getClientVersion(), appName, setUpRequest.getLabels());
                    // 租户：实际是 nameSpace
                    metaInfo.setTenant(setUpRequest.getTenant());
                    // 创建 grpc 连接对象
                    Connection connection = new GrpcConnection(metaInfo, responseObserver, GrpcServerConstants.CONTEXT_KEY_CHANNEL.get());
                    // 为服务端设置客户端的能力： clientWork 设置  nacos 客户端能力：远程、配置能力
                    connection.setAbilities(setUpRequest.getAbilities());
                    // 是 sdk 并且 服务器没有启动成功
                    boolean rejectSdkOnStarting = metaInfo.isSdkSource() && !ApplicationUtils.isStarted();

                    // 连接管理者注册连接不成功，且 还没开始完成
                    if (rejectSdkOnStarting || !connectionManager.register(connectionId, connection)) {
                        //Not register to the connection manager if current server is over limit or server is starting.
                        //如果当前服务器超过限制或服务器正在启动，则不注册到连接管理器。
                        try {
                            Loggers.REMOTE_DIGEST.warn("[{}]Connection register fail,reason:{}", connectionId,
                                    rejectSdkOnStarting ? " server is not started" : " server is over limited.");
                            // 发送流式请求：连接重置
                            connection.request(new ConnectResetRequest(), 3000L);
                            // 关闭连接
                            connection.close();
                        } catch (Exception e) {
                            //Do nothing.
                            // 超时会抛出异常，但是不处理
                            if (connectionManager.traced(clientIp)) {
                                Loggers.REMOTE_DIGEST
                                        .warn("[{}]Send connect reset request error,error={}", connectionId, e);
                            }
                        }
                    }
                    
                }
                // 响应
                else if (parseObj instanceof Response) {
                    Response response = (Response) parseObj;
                    if (connectionManager.traced(clientIp)) {
                        Loggers.REMOTE_DIGEST
                                .warn("[{}]Receive response of server request  ,response={}", connectionId, response);
                    }
                    // 服务端发送请求后，客户端返回响应，回调通知
                    RpcAckCallbackSynchronizer.ackNotify(connectionId, response);
                    // 刷新激活时间
                    connectionManager.refreshActiveTime(connectionId);
                } else {
                    Loggers.REMOTE_DIGEST
                            .warn("[{}]Grpc request bi stream,unknown payload receive ,parseObj={}", connectionId,
                                    parseObj);
                }
                
            }
            
            @Override
            public void onError(Throwable t) {
                if (connectionManager.traced(clientIp)) {
                    Loggers.REMOTE_DIGEST.warn("[{}]Bi stream on error,error={}", connectionId, t);
                }
                
                if (responseObserver instanceof ServerCallStreamObserver) {
                    ServerCallStreamObserver serverCallStreamObserver = ((ServerCallStreamObserver) responseObserver);
                    if (serverCallStreamObserver.isCancelled()) {
                        //client close the stream.
                    } else {
                        try {
                            serverCallStreamObserver.onCompleted();
                        } catch (Throwable throwable) {
                            //ignore
                        }
                    }
                }
                
            }
            
            @Override
            public void onCompleted() {
                if (connectionManager.traced(clientIp)) {
                    Loggers.REMOTE_DIGEST.warn("[{}]Bi stream on completed", connectionId);
                }
                if (responseObserver instanceof ServerCallStreamObserver) {
                    ServerCallStreamObserver serverCallStreamObserver = ((ServerCallStreamObserver) responseObserver);
                    if (serverCallStreamObserver.isCancelled()) {
                        //client close the stream.
                    } else {
                        try {
                            serverCallStreamObserver.onCompleted();
                        } catch (Throwable throwable) {
                            //ignore
                        }
                        
                    }
                }
            }
        };
        
        return streamObserver;
    }
    
}
