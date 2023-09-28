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

import com.alibaba.nacos.api.grpc.auto.Payload;
import com.alibaba.nacos.common.packagescan.resource.DefaultResourceLoader;
import com.alibaba.nacos.common.packagescan.resource.Resource;
import com.alibaba.nacos.common.packagescan.resource.ResourceLoader;
import com.alibaba.nacos.common.remote.ConnectionType;

import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.TlsTypeResolve;
import com.alibaba.nacos.core.remote.BaseRpcServer;
import com.alibaba.nacos.core.remote.ConnectionManager;
import com.alibaba.nacos.sys.env.EnvUtil;
import io.grpc.CompressorRegistry;
import io.grpc.DecompressorRegistry;
import io.grpc.MethodDescriptor;
import io.grpc.Server;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.ServerInterceptors;
import io.grpc.ServerServiceDefinition;
import io.grpc.netty.shaded.io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.ClientAuth;
import io.grpc.netty.shaded.io.netty.handler.ssl.SslContext;

import io.grpc.netty.shaded.io.netty.handler.ssl.SslContextBuilder;
import io.grpc.netty.shaded.io.netty.handler.ssl.util.InsecureTrustManagerFactory;
import io.grpc.protobuf.ProtoUtils;
import io.grpc.stub.ServerCalls;
import io.grpc.util.MutableHandlerRegistry;
import org.springframework.beans.factory.annotation.Autowired;

import javax.net.ssl.SSLException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Grpc implementation as a rpc server.
 * GRPC 服务端 有两个实现类：
 *  GrpcClusterServer 集群下的服务端、GrpcSdkServer sdk下服务端
 *  itodo： 两个实现类是在什么时候加载的？
 * @author liuzunfei
 * @version $Id: BaseGrpcServer.java, v 0.1 2020年07月13日 3:42 PM liuzunfei Exp $
 */
public abstract class BaseGrpcServer extends BaseRpcServer {

    private Server server;
    /**
     * 默认的资源加载器
     */
    private final ResourceLoader resourceLoader = new DefaultResourceLoader();

    @Autowired
    private GrpcRequestAcceptor grpcCommonRequestAcceptor;

    @Autowired
    private GrpcBiStreamRequestAcceptor grpcBiStreamRequestAcceptor;

    @Autowired
    private ConnectionManager connectionManager;

    @Override
    public ConnectionType getConnectionType() {
        return ConnectionType.GRPC;
    }

    /**
     * grpc 服务端  启动
     * @throws Exception
     */
    @Override
    public void startServer() throws Exception {
        final MutableHandlerRegistry handlerRegistry = new MutableHandlerRegistry();
        // 添加服务
        addServices(handlerRegistry, new GrpcConnectionInterceptor());
        // 创建 netty 服务端建造者
        NettyServerBuilder builder = NettyServerBuilder.forPort(getServicePort()).executor(getRpcExecutor());
        // 判断是否 使用 TLS
        if (grpcServerConfig.getEnableTls()) {
            // 判断是否兼容，默认是兼容的
            if (grpcServerConfig.getCompatibility()) {
                builder.protocolNegotiator(new OptionalTlsProtocolNegotiator(getSslContextBuilder()));
            } else {
                // 获取 ssl 上下文
                builder.sslContext(getSslContextBuilder());
            }
        }

        server = builder.maxInboundMessageSize(getMaxInboundMessageSize())
                .fallbackHandlerRegistry(handlerRegistry)
                .compressorRegistry(CompressorRegistry.getDefaultInstance())
                .decompressorRegistry(DecompressorRegistry.getDefaultInstance())
                .addTransportFilter(new AddressTransportFilter(connectionManager))
                .keepAliveTime(getKeepAliveTime(), TimeUnit.MILLISECONDS)
                .keepAliveTimeout(getKeepAliveTimeout(), TimeUnit.MILLISECONDS)
                .permitKeepAliveTime(getPermitKeepAliveTime(), TimeUnit.MILLISECONDS)
                .build();
        // 开启 客户端
        server.start();
    }

    protected long getPermitKeepAliveTime() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_PERMIT_KEEP_ALIVE_TIME;
    }

    protected long getKeepAliveTime() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_KEEP_ALIVE_TIME;
    }

    protected long getKeepAliveTimeout() {
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_KEEP_ALIVE_TIMEOUT;
    }

    protected int getMaxInboundMessageSize() {
        Integer property = EnvUtil.getProperty(GrpcServerConstants.GrpcConfig.MAX_INBOUND_MSG_SIZE_PROPERTY,
                Integer.class);
        if (property != null) {
            return property;
        }
        return GrpcServerConstants.GrpcConfig.DEFAULT_GRPC_MAX_INBOUND_MSG_SIZE;
    }

    /**
     * GPRC 添加服务
     */
    private void addServices(MutableHandlerRegistry handlerRegistry, ServerInterceptor... serverInterceptor) {

        // unary common call register.
        // 普通请求模式 的服务注册
        // 方法描述符：指出 请求和响应的类型
        final MethodDescriptor<Payload, Payload> unaryPayloadMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.UNARY)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(GrpcServerConstants.REQUEST_SERVICE_NAME,
                        GrpcServerConstants.REQUEST_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance())).build();

        final ServerCallHandler<Payload, Payload> payloadHandler = ServerCalls
                .asyncUnaryCall((request, responseObserver) -> grpcCommonRequestAcceptor.request(request, responseObserver));

        final ServerServiceDefinition serviceDefOfUnaryPayload = ServerServiceDefinition
                .builder(GrpcServerConstants.REQUEST_SERVICE_NAME)
                .addMethod(unaryPayloadMethod, payloadHandler)
                .build();
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfUnaryPayload, serverInterceptor));

        // bi stream register.
        // 双向流模式 的服务注册
        final ServerCallHandler<Payload, Payload> biStreamHandler = ServerCalls.asyncBidiStreamingCall(
                (responseObserver) -> grpcBiStreamRequestAcceptor.requestBiStream(responseObserver));

        final MethodDescriptor<Payload, Payload> biStreamMethod = MethodDescriptor.<Payload, Payload>newBuilder()
                .setType(MethodDescriptor.MethodType.BIDI_STREAMING)
                .setFullMethodName(MethodDescriptor.generateFullMethodName(GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME,
                                GrpcServerConstants.REQUEST_BI_STREAM_METHOD_NAME))
                .setRequestMarshaller(ProtoUtils.marshaller(Payload.newBuilder().build()))
                .setResponseMarshaller(ProtoUtils.marshaller(Payload.getDefaultInstance()))
                .build();

        final ServerServiceDefinition serviceDefOfBiStream = ServerServiceDefinition
                .builder(GrpcServerConstants.REQUEST_BI_STREAM_SERVICE_NAME)
                .addMethod(biStreamMethod, biStreamHandler)
                .build();
        handlerRegistry.addService(ServerInterceptors.intercept(serviceDefOfBiStream, serverInterceptor));

    }

    @Override
    public void shutdownServer() {
        if (server != null) {
            server.shutdownNow();
        }
    }

    private SslContext getSslContextBuilder() {
        try {
            //未获取证书链文件 、证书私钥为空，抛出异常
            if (StringUtils.isBlank(grpcServerConfig.getCertChainFile()) || StringUtils.isBlank(grpcServerConfig.getCertPrivateKey())) {
                throw new IllegalArgumentException("Server certChainFile or certPrivateKey must be not null");
            }
            // 加载 证书和证书私钥
            InputStream certificateChainFile = getInputStream(grpcServerConfig.getCertChainFile(), "certChainFile");
            InputStream privateKeyFile = getInputStream(grpcServerConfig.getCertPrivateKey(), "certPrivateKey");
            // 获取 grpc ssl 上下文创建者
            SslContextBuilder sslClientContextBuilder = SslContextBuilder.forServer(certificateChainFile, privateKeyFile,
                    grpcServerConfig.getCertPrivateKeyPassword());

            if (StringUtils.isNotBlank(grpcServerConfig.getProtocols())) {
                sslClientContextBuilder.protocols(grpcServerConfig.getProtocols().split(","));
            }

            if (StringUtils.isNotBlank(grpcServerConfig.getCiphers())) {
                sslClientContextBuilder.ciphers(Arrays.asList(grpcServerConfig.getCiphers().split(",")));
            }
            // 判断是否相互 认证
            if (grpcServerConfig.getMutualAuthEnable()) {
                // trust all certificate
                // 判断是否信任所有证书
                if (grpcServerConfig.getTrustAll()) {
                    sslClientContextBuilder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                } else {
                    if (StringUtils.isBlank(grpcServerConfig.getTrustCollectionCertFile())) {
                        throw new IllegalArgumentException("enable mutual auth,trustCollectionCertFile must be not null");
                    }

                    InputStream clientCert = getInputStream(grpcServerConfig.getTrustCollectionCertFile(), "trustCollectionCertFile");
                    sslClientContextBuilder.trustManager(clientCert);
                }
                // 设置需要客户端认证
                sslClientContextBuilder.clientAuth(ClientAuth.REQUIRE);
            }
            SslContextBuilder configure = GrpcSslContexts.configure(sslClientContextBuilder,
                    TlsTypeResolve.getSslProvider(grpcServerConfig.getSslProvider()));
            return configure.build();
        } catch (SSLException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream getInputStream(String path, String config) {
        try {
            Resource resource = resourceLoader.getResource(path);
            return resource.getInputStream();
        } catch (IOException e) {
            throw new RuntimeException(config + " load fail", e);
        }
    }

    /**
     * get rpc executor.
     *
     * @return executor.
     */
    public abstract ThreadPoolExecutor getRpcExecutor();

}
