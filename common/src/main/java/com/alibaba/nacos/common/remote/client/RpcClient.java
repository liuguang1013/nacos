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

package com.alibaba.nacos.common.remote.client;

import com.alibaba.nacos.api.ability.ClientAbilities;
import com.alibaba.nacos.api.common.Constants;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.RequestFuture;
import com.alibaba.nacos.api.remote.request.ClientDetectionRequest;
import com.alibaba.nacos.api.remote.request.ConnectResetRequest;
import com.alibaba.nacos.api.remote.request.HealthCheckRequest;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.ClientDetectionResponse;
import com.alibaba.nacos.api.remote.response.ConnectResetResponse;
import com.alibaba.nacos.api.remote.response.ErrorResponse;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.packagescan.resource.DefaultResourceLoader;
import com.alibaba.nacos.common.packagescan.resource.ResourceLoader;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.PayloadRegistry;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.common.utils.InternetAddressUtil;
import com.alibaba.nacos.common.utils.LoggerUtils;
import com.alibaba.nacos.common.utils.NumberUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static com.alibaba.nacos.api.exception.NacosException.SERVER_ERROR;

/**
 * abstract remote client to connect to server.
 *
 * @author liuzunfei
 * @version $Id: RpcClient.java, v 0.1 2020年07月13日 9:15 PM liuzunfei Exp $
 */
@SuppressWarnings("PMD.AbstractClassShouldStartWithAbstractNamingRule")
public abstract class RpcClient implements Closeable {
    
    private static final Logger LOGGER = LoggerFactory.getLogger("com.alibaba.nacos.common.remote.client");
    
    private ServerListFactory serverListFactory;

    /**
     * 保存连接事件的阻塞队列
     * 1、closeConnection 在断开连接的时候，向阻塞队列中添加元素
     * 2、成功连接服务端的时候，向阻塞队列中添加元素
     */
    protected BlockingQueue<ConnectionEvent> eventLinkedBlockingQueue = new LinkedBlockingQueue<>();

    /**
     * rpc 客户端状态
     */
    protected volatile AtomicReference<RpcClientStatus> rpcClientStatus = new AtomicReference<>(
            RpcClientStatus.WAIT_INIT);
    
    protected ScheduledExecutorService clientEventExecutor;
    /**
     * 阻塞队列：保存需要重连的信号
     *  当接收 ConnectResetRequest 请求时候 添加
     */
    private final BlockingQueue<ReconnectContext> reconnectionSignal = new ArrayBlockingQueue<>(1);
    
    protected volatile Connection currentConnection;
    
    private String tenant;
    
    protected ClientAbilities clientAbilities;
    
    private long lastActiveTimeStamp = System.currentTimeMillis();
    
    /**
     * listener called where connection's status changed.
     * 监听连接状态的改变的处理器的列表，实际有：NamingGrpcRedoService
     */
    protected List<ConnectionEventListener> connectionEventListeners = new ArrayList<>();
    
    /**
     * handlers to process server push request.
     * 处理 服务端 推送的请求的列表，
     * 实际有： NamingPushRequestHandler 处理 NotifySubscriberRequest 请求    在NamingGrpcClientProxy 初始化添加
     *      匿名内部类处理 ClientDetectionRequest 请求
     *      匿名内部类处理 ConnectResetRequest 请求
     *      this.ConnectResetRequestHandler 连接请求
     */
    protected List<ServerRequestHandler> serverRequestHandlers = new ArrayList<>();
    
    private static final Pattern EXCLUDE_PROTOCOL_PATTERN = Pattern.compile("(?<=\\w{1,5}://)(.*)");

    /**
     * rpc 客户端配置
     */
    protected RpcClientConfig rpcClientConfig;

    protected final ResourceLoader resourceLoader = new DefaultResourceLoader();

    static {
        // 载荷初始化，加载 Payload 接口的实现类 名称和.class 的关系
        PayloadRegistry.init();
    }
    
    public RpcClient(RpcClientConfig rpcClientConfig) {
        this(rpcClientConfig, null);
    }
    
    public RpcClient(RpcClientConfig rpcClientConfig, ServerListFactory serverListFactory) {
        this.rpcClientConfig = rpcClientConfig;
        this.serverListFactory = serverListFactory;
        // 初始化
        init();
    }
    
    protected void init() {
        // 在初始化的时候，是 null
        if (this.serverListFactory != null) {
            // CAS 置换 当前 rpc 客户端状态
            rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
            LoggerUtils.printIfInfoEnabled(LOGGER, "RpcClient init in constructor, ServerListFactory = {}",
                    serverListFactory.getClass().getName());
        }
    }
    
    public Map<String, String> labels() {
        return Collections.unmodifiableMap(rpcClientConfig.labels());
    }
    
    /**
     * init client abilities.
     * clientWork 设置  nacos 客户端能力：远程、配置能力
     * @param clientAbilities clientAbilities.
     */
    public RpcClient clientAbilities(ClientAbilities clientAbilities) {
        this.clientAbilities = clientAbilities;
        return this;
    }
    
    /**
     * init server list factory. only can init once.
     * 初始化 服务端列表 工厂，只初始化一次
     * 之前创建 GrpcSdkClient 的时候，并未传入 serverListFactory，故此处进行 CAS 状态置换
     * @param serverListFactory serverListFactory
     */
    public RpcClient serverListFactory(ServerListFactory serverListFactory) {
        if (!isWaitInitiated()) {
            return this;
        }
        this.serverListFactory = serverListFactory;
        rpcClientStatus.compareAndSet(RpcClientStatus.WAIT_INIT, RpcClientStatus.INITIALIZED);
        
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] RpcClient init, ServerListFactory = {}", rpcClientConfig.name(),
                serverListFactory.getClass().getName());
        return this;
    }
    
    /**
     * Notify when client disconnected.
     */
    protected void notifyDisConnected() {
        if (connectionEventListeners.isEmpty()) {
            return;
        }
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Notify disconnected event to listeners", rpcClientConfig.name());
        for (ConnectionEventListener connectionEventListener : connectionEventListeners) {
            try {
                // todo： 断开连接 所做的事，需要看
                connectionEventListener.onDisConnect();
            } catch (Throwable throwable) {
                LoggerUtils.printIfErrorEnabled(LOGGER, "[{}] Notify disconnect listener error, listener = {}",
                        rpcClientConfig.name(), connectionEventListener.getClass().getName());
            }
        }
    }
    
    /**
     * Notify when client new connected.
     */
    protected void notifyConnected() {
        if (connectionEventListeners.isEmpty()) {
            return;
        }
        // rpcClientConfig.name() 是 uuid RpcClientFactory 创建 GrpcSdkClient 时传入的
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Notify connected event to listeners.", rpcClientConfig.name());
        // 命名服务：NamingGrpcRedoService 将 连接状态设置为 true
        for (ConnectionEventListener connectionEventListener : connectionEventListeners) {
            try {
                // todo： 连接成功 所做的事，需要看
                connectionEventListener.onConnected();
            } catch (Throwable throwable) {
                LoggerUtils.printIfErrorEnabled(LOGGER, "[{}] Notify connect listener error, listener = {}",
                        rpcClientConfig.name(), connectionEventListener.getClass().getName());
            }
        }
    }
    
    /**
     * check is this client is initiated.
     *
     * @return is wait initiated or not.
     */
    public boolean isWaitInitiated() {
        return this.rpcClientStatus.get() == RpcClientStatus.WAIT_INIT;
    }
    
    /**
     * check is this client is running.
     * 检查客户端状态是不是 运行状态
     * @return is running or not.
     */
    public boolean isRunning() {
        return this.rpcClientStatus.get() == RpcClientStatus.RUNNING;
    }
    
    /**
     * check is this client is shutdown.
     *
     * @return is shutdown or not.
     */
    public boolean isShutdown() {
        return this.rpcClientStatus.get() == RpcClientStatus.SHUTDOWN;
    }
    
    /**
     * check if current connected server is in server list, if not switch server.
     */
    public void onServerListChange() {
        if (currentConnection != null && currentConnection.serverInfo != null) {
            ServerInfo serverInfo = currentConnection.serverInfo;
            boolean found = false;
            for (String serverAddress : serverListFactory.getServerList()) {
                if (resolveServerInfo(serverAddress).getAddress().equalsIgnoreCase(serverInfo.getAddress())) {
                    found = true;
                    break;
                }
            }
            if (!found) {
                LoggerUtils.printIfInfoEnabled(LOGGER,
                        "Current connected server {} is not in latest server list, switch switchServerAsync",
                        serverInfo.getAddress());
                switchServerAsync();
            }
            
        }
    }
    
    /**
     * Start this client.
     * 开启客户端
     */
    public final void start() throws NacosException {
        
        boolean success = rpcClientStatus.compareAndSet(RpcClientStatus.INITIALIZED, RpcClientStatus.STARTING);
        if (!success) {
            return;
        }
        // 初始化 客户端事件线程池
        clientEventExecutor = new ScheduledThreadPoolExecutor(2, r -> {
            Thread t = new Thread(r);
            t.setName("com.alibaba.nacos.client.remote.worker");
            t.setDaemon(true);
            return t;
        });
        
        // connection event consumer.
        // 添加连接事件处理任务
        clientEventExecutor.submit(() -> {
            // 无限循环：在线程池 未关闭状态下
            while (!clientEventExecutor.isTerminated() && !clientEventExecutor.isShutdown()) {
                ConnectionEvent take;
                try {
                    // 获取 连接事件，
                    take = eventLinkedBlockingQueue.take();
                    if (take.isConnected()) {
                        // 通知 已连接事件
                        notifyConnected();
                    } else if (take.isDisConnected()) {
                        // 通知 断开连接事件
                        notifyDisConnected();
                    }
                } catch (Throwable e) {
                    // Do nothing
                }
            }
        });

        // 添加无限循环任务：心跳任务
        clientEventExecutor.submit(() -> {
            while (true) {
                try {
                    // 判断 grpc 状态是否为关闭
                    if (isShutdown()) {
                        break;
                    }
                    // 在阻塞队列中，获取重连信号
                    ReconnectContext reconnectContext = reconnectionSignal
                            .poll(rpcClientConfig.connectionKeepAlive(), TimeUnit.MILLISECONDS);

                    if (reconnectContext == null) {
                        // 未获取到需要重连的上下文：上下文中实际就是 服务端的 IP+端口号
                        // check alive time.
                        // 检查 存活： 距离上次探活时间 大于 配置的连接保活时间，进行健康检查
                        if (System.currentTimeMillis() - lastActiveTimeStamp >= rpcClientConfig.connectionKeepAlive()) {
                            // 健康检查，请求3次
                            boolean isHealthy = healthCheck();
                            if (!isHealthy) {
                                // 检查失败
                                if (currentConnection == null) {
                                    continue;
                                }
                                LoggerUtils.printIfInfoEnabled(LOGGER,
                                        "[{}] Server healthy check fail, currentConnection = {}",
                                        rpcClientConfig.name(), currentConnection.getConnectionId());
                                
                                RpcClientStatus rpcClientStatus = RpcClient.this.rpcClientStatus.get();
                                // 当 客户端的状态是 SHUTDOWN 直接结束任务
                                if (RpcClientStatus.SHUTDOWN.equals(rpcClientStatus)) {
                                    break;
                                }
                                // 设置 CAS 状态为： 不健康
                                boolean statusFLowSuccess = RpcClient.this.rpcClientStatus
                                        .compareAndSet(rpcClientStatus, RpcClientStatus.UNHEALTHY);
                                if (statusFLowSuccess) {
                                    // 创建 重新连接上下文，
                                    // 健康检查失败 不设置 serverInfo，意味着没有指定重连的服务端信息
                                    reconnectContext = new ReconnectContext(null, false);
                                } else {
                                    continue;
                                }
                                
                            } else {
                                lastActiveTimeStamp = System.currentTimeMillis();
                                continue;
                            }
                        } else {
                            continue;
                        }
                        
                    }
                    // itodo：什么情况下，指定某个 服务端的 IP + 端口
                    if (reconnectContext.serverInfo != null) {
                        // clear recommend server if server is not in server list.
                        // 如果服务器不在服务器列表中，则清除推荐服务器。
                        boolean serverExist = false;
                        for (String server : getServerListFactory().getServerList()) {
                            // 将 IP+端口号 的字符串，解析成 ServerInfo 对象
                            ServerInfo serverInfo = resolveServerInfo(server);
                            if (serverInfo.getServerIp().equals(reconnectContext.serverInfo.getServerIp())) {
                                serverExist = true;
                                // 设置端口号
                                reconnectContext.serverInfo.serverPort = serverInfo.serverPort;
                                break;
                            }
                        }
                        // 重新连接的上下文中，指定的服务端地址不存在 ServerListManager 的列表中，ServerListManager 中是在启动时候指定的
                        if (!serverExist) {
                            LoggerUtils.printIfInfoEnabled(LOGGER,
                                    "[{}] Recommend server is not in server list, ignore recommend server {}",
                                    rpcClientConfig.name(), reconnectContext.serverInfo.getAddress());
                            // 设置对象为空
                            reconnectContext.serverInfo = null;
                            
                        }
                    }

                    // 重新连接：换其他服务器
                    reconnect(reconnectContext.serverInfo, reconnectContext.onRequestFail);
                } catch (Throwable throwable) {
                    // Do nothing
                }
            }
        });
        
        // connect to server, try to connect to server sync retryTimes times, async starting if failed.
        Connection connectToServer = null;
        rpcClientStatus.set(RpcClientStatus.STARTING);

        // 重试
        int startUpRetryTimes = rpcClientConfig.retryTimes();
        while (startUpRetryTimes > 0 && connectToServer == null) {
            try {
                startUpRetryTimes--;
                // 获取连接的服务端的 IP+端口
                ServerInfo serverInfo = nextRpcServer();
                
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Try to connect to server on start up, server: {}",
                        rpcClientConfig.name(), serverInfo);
                // 连接服务端
                connectToServer = connectToServer(serverInfo);
            } catch (Throwable e) {
                LoggerUtils.printIfWarnEnabled(LOGGER,
                        "[{}] Fail to connect to server on start up, error message = {}, start up retry times left: {}",
                        rpcClientConfig.name(), e.getMessage(), startUpRetryTimes, e);
            }
            
        }
        
        if (connectToServer != null) {
            LoggerUtils
                    .printIfInfoEnabled(LOGGER, "[{}] Success to connect to server [{}] on start up, connectionId = {}",
                            rpcClientConfig.name(), connectToServer.serverInfo.getAddress(),
                            connectToServer.getConnectionId());
            this.currentConnection = connectToServer;
            // 设置 客户端的状态是 RUNNING
            rpcClientStatus.set(RpcClientStatus.RUNNING);
            // 向阻塞队列中添加元素
            eventLinkedBlockingQueue.offer(new ConnectionEvent(ConnectionEvent.CONNECTED));
        } else {
            // 连接服务端，未连接成功
            switchServerAsync();
        }

        // 注册服务端请求处理器：服务端发送 连接重置请求时候 使用
        // 在 客户端连接服务端时候，服务端还未启动完成/超过服务端连接数，服务端发送连接重置请求
        registerServerRequestHandler(new ConnectResetRequestHandler());
        
        // register client detection request.
        // 注册 客户端探测请求
        registerServerRequestHandler(request -> {
            if (request instanceof ClientDetectionRequest) {
                return new ClientDetectionResponse();
            }
            
            return null;
        });
        
    }

    /**
     * 流式处理的 onNext 方法中
     * 处理服务端连接重置请求
     */
    class ConnectResetRequestHandler implements ServerRequestHandler {
        
        @Override
        public Response requestReply(Request request) {
            // 判断请求类型
            if (request instanceof ConnectResetRequest) {
                
                try {
                    synchronized (RpcClient.this) {
                        if (isRunning()) {
                            ConnectResetRequest connectResetRequest = (ConnectResetRequest) request;
                            // 获取服务端 ip ，重新连接服务端
                            if (StringUtils.isNotBlank(connectResetRequest.getServerIp())) {
                                ServerInfo serverInfo = resolveServerInfo(
                                        connectResetRequest.getServerIp() + Constants.COLON + connectResetRequest
                                                .getServerPort());
                                switchServerAsync(serverInfo, false);
                            } else {
                                switchServerAsync();
                            }
                        }
                    }
                } catch (Exception e) {
                    LoggerUtils.printIfErrorEnabled(LOGGER, "[{}] Switch server error, {}", rpcClientConfig.name(), e);
                }
                return new ConnectResetResponse();
            }
            return null;
        }
    }
    
    @Override
    public void shutdown() throws NacosException {
        LOGGER.info("Shutdown rpc client, set status to shutdown");
        // 设置客户端状态为 关闭
        rpcClientStatus.set(RpcClientStatus.SHUTDOWN);
        LOGGER.info("Shutdown client event executor " + clientEventExecutor);
        // 关闭 处理心跳健康检查的线程
        if (clientEventExecutor != null) {
            clientEventExecutor.shutdownNow();
        }
        // 关闭连接
        closeConnection(currentConnection);
    }
    
    private boolean healthCheck() {
        HealthCheckRequest healthCheckRequest = new HealthCheckRequest();
        if (this.currentConnection == null) {
            return false;
        }
        // 默认 3 次，DefaultGrpcClientConfig 的 builder 中有设置
        int reTryTimes = rpcClientConfig.healthCheckRetryTimes();
        while (reTryTimes >= 0) {
            reTryTimes--;
            try {
                // 发送 健康检查请求 ，默认的超时时间：3s
                Response response = this.currentConnection
                        .request(healthCheckRequest, rpcClientConfig.healthCheckTimeOut());
                // not only check server is ok, also check connection is register.
                return response != null && response.isSuccess();
            } catch (NacosException e) {
                // ignore
            }
        }
        return false;
    }
    
    public void switchServerAsyncOnRequestFail() {
        switchServerAsync(null, true);
    }
    
    public void switchServerAsync() {
        switchServerAsync(null, false);
    }
    
    protected void switchServerAsync(final ServerInfo recommendServerInfo, boolean onRequestFail) {
        // 向保存重连信号的 阻塞队列中添加元素
        // 当接收 ConnectResetRequest 请求时候 添加
        reconnectionSignal.offer(new ReconnectContext(recommendServerInfo, onRequestFail));
    }

    /**
     * switch server .
     * 转换服务器连接
     * @param recommendServerInfo 当入参为空的时候，随机选择一个服务端连接
     * @param onRequestFail
     */
    protected void reconnect(final ServerInfo recommendServerInfo, boolean onRequestFail) {
        
        try {
            
            AtomicReference<ServerInfo> recommendServer = new AtomicReference<>(recommendServerInfo);
            // 请求失败，是否进行健康检查
            if (onRequestFail && healthCheck()) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Server check success, currentServer is {} ",
                        rpcClientConfig.name(), currentConnection.serverInfo.getAddress());
                // 健康检查成功后，重新设置客户端状态为 RUNNING
                rpcClientStatus.set(RpcClientStatus.RUNNING);
                return;
            }
            // 日志输出： 客户端id 是 uuid
            LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Try to reconnect to a new server, server is {}",
                    rpcClientConfig.name(), recommendServerInfo == null ? " not appointed, will choose a random server."
                            : (recommendServerInfo.getAddress() + ", will try it once."));
            
            // loop until start client success.
            // 循环直到 客户端 重启成功
            boolean switchSuccess = false;
            
            int reConnectTimes = 0;
            int retryTurns = 0;
            Exception lastException;
            while (!switchSuccess && !isShutdown()) {
                
                // 1.get a new server
                ServerInfo serverInfo = null;
                try {
                    // 获取一个新的 服务端
                    serverInfo = recommendServer.get() == null ? nextRpcServer() : recommendServer.get();
                    // 2.create a new channel to new server
                    Connection connectionNew = connectToServer(serverInfo);
                    // 新的连接对象不为空
                    // 当服务端检查失败的时候为 null
                    if (connectionNew != null) {
                        LoggerUtils
                                .printIfInfoEnabled(LOGGER, "[{}] Success to connect a server [{}], connectionId = {}",
                                        rpcClientConfig.name(), serverInfo.getAddress(),
                                        connectionNew.getConnectionId());
                        // successfully create a new connect.
                        // 成功创建一个新的 连接对象，当前的连接不为空，标记为弃用，关闭连接
                        if (currentConnection != null) {
                            LoggerUtils.printIfInfoEnabled(LOGGER,
                                    "[{}] Abandon prev connection, server is {}, connectionId is {}",
                                    rpcClientConfig.name(), currentConnection.serverInfo.getAddress(),
                                    currentConnection.getConnectionId());
                            // set current connection to enable connection event.
                            currentConnection.setAbandon(true);
                            // 关闭连接
                            closeConnection(currentConnection);
                        }
                        currentConnection = connectionNew;
                        rpcClientStatus.set(RpcClientStatus.RUNNING);
                        switchSuccess = true;
                        eventLinkedBlockingQueue.add(new ConnectionEvent(ConnectionEvent.CONNECTED));
                        return;
                    }
                    
                    // close connection if client is already shutdown.
                    // 整个客户端都关闭，也调用 关闭连接方法
                    if (isShutdown()) {
                        closeConnection(currentConnection);
                    }
                    
                    lastException = null;
                    
                } catch (Exception e) {
                    lastException = e;
                } finally {
                    recommendServer.set(null);
                }
                
                if (CollectionUtils.isEmpty(RpcClient.this.serverListFactory.getServerList())) {
                    throw new Exception("server list is empty");
                }

                // 对所有的服务端都重连过，记录轮数
                if (reConnectTimes > 0
                        && reConnectTimes % RpcClient.this.serverListFactory.getServerList().size() == 0) {
                    LoggerUtils.printIfInfoEnabled(LOGGER,
                            "[{}] Fail to connect server, after trying {} times, last try server is {}, error = {}",
                            rpcClientConfig.name(), reConnectTimes, serverInfo,
                            lastException == null ? "unknown" : lastException);
                    if (Integer.MAX_VALUE == retryTurns) {
                        retryTurns = 50;
                    } else {
                        retryTurns++;
                    }
                }

                // 增加重连次数
                reConnectTimes++;
                
                try {
                    // sleep x milliseconds to switch next server.
                    //
                    if (!isRunning()) {
                        // first round, try servers at a delay 100ms;second round, 200ms; max delays 5s. to be reconsidered.
                        //第 一轮，尝试服务器延迟100ms;第二轮，延迟200ms;最大延迟5秒。需要重新考虑
                        Thread.sleep(Math.min(retryTurns + 1, 50) * 100L);
                    }
                } catch (InterruptedException e) {
                    // Do nothing.
                    // set the interrupted flag
                    Thread.currentThread().interrupt();
                }
            }
            
            if (isShutdown()) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Client is shutdown, stop reconnect to server",
                        rpcClientConfig.name());
            }
            
        } catch (Exception e) {
            LoggerUtils
                    .printIfWarnEnabled(LOGGER, "[{}] Fail to reconnect to server, error is {}", rpcClientConfig.name(),
                            e);
        }
    }
    
    private void closeConnection(Connection connection) {
        if (connection != null) {
            LOGGER.info("Close current connection " + connection.getConnectionId());
            connection.close();
            // 添加连接断开事件
            eventLinkedBlockingQueue.add(new ConnectionEvent(ConnectionEvent.DISCONNECTED));
        }
    }
    
    /**
     * get connection type of this client.
     *
     * @return ConnectionType.
     */
    public abstract ConnectionType getConnectionType();
    
    /**
     * increase offset of the nacos server port for the rpc server port.
     *
     * @return rpc port offset
     */
    public abstract int rpcPortOffset();
    
    /**
     * get current server.
     *
     * @return server info.
     */
    public ServerInfo getCurrentServer() {
        if (this.currentConnection != null) {
            return currentConnection.serverInfo;
        }
        return null;
    }
    
    /**
     * send request.
     *
     * @param request request.
     * @return response from server.
     */
    public Response request(Request request) throws NacosException {
        return request(request, rpcClientConfig.timeOutMills());
    }
    
    /**
     * send request.
     *  发送
     * @param request request.
     * @return response from server.
     */
    public Response request(Request request, long timeoutMills) throws NacosException {
        int retryTimes = 0;
        Response response;
        Throwable exceptionThrow = null;
        long start = System.currentTimeMillis();
        // 当 小于最大重试次数、并且小于 超时时间
        while (retryTimes < rpcClientConfig.retryTimes() && System.currentTimeMillis() < timeoutMills + start) {
            boolean waitReconnect = false;
            try {
                // 当前客户端连接为空，并且不是 运行状态
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_DISCONNECT,
                            "Client not connected, current status:" + rpcClientStatus.get());
                }
                // 通过 GrpcConnection 访问 com.alibaba.nacos.core.remote.grpc 包下 的 GrpcRequestAcceptor
                response = this.currentConnection.request(request, timeoutMills);
                // 没有响应抛异常
                if (response == null) {
                    throw new NacosException(SERVER_ERROR, "Unknown Exception.");
                }
                // 错误响应：当服务端还没启动完成可能会返回这个响应
                if (response instanceof ErrorResponse) {
                    // 错误类型：连接没有注册，说明 rpc 连接不健康，服务端此时已经把当前客户端连接移除了
                    if (response.getErrorCode() == NacosException.UN_REGISTER) {
                        synchronized (this) {
                            waitReconnect = true;
                            // 将 当前客户端状态置为 不健康状态
                            if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
                                LoggerUtils.printIfErrorEnabled(LOGGER,
                                        "Connection is unregistered, switch server, connectionId = {}, request = {}",
                                        currentConnection.getConnectionId(), request.getClass().getSimpleName());
                                // 异步的转换另一服务端，进行连接
                                switchServerAsync();
                            }
                        }
                        
                    }
                    throw new NacosException(response.getErrorCode(), response.getMessage());
                }
                // return response.
                // 记录调用成功时间
                lastActiveTimeStamp = System.currentTimeMillis();
                return response;
                
            } catch (Throwable e) {
                if (waitReconnect) {
                    try {
                        // wait client to reconnect.
                        Thread.sleep(Math.min(100, timeoutMills / 3));
                    } catch (Exception exception) {
                        // Do nothing.
                    }
                }
                
                LoggerUtils.printIfErrorEnabled(LOGGER,
                        "Send request fail, request = {}, retryTimes = {}, errorMessage = {}", request, retryTimes,
                        e.getMessage());
                
                exceptionThrow = e;
                
            }
            retryTimes++;
            
        }

        // 发送 一定次数请后，仍然未成功，将客户端设置为 UNHEALTHY 状态
        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            // 向 reconnectionSignal 阻塞队列中添加重连服务端信息，客户端定时任务会
            switchServerAsyncOnRequestFail();
        }
        
        if (exceptionThrow != null) {
            throw (exceptionThrow instanceof NacosException) ? (NacosException) exceptionThrow
                    : new NacosException(SERVER_ERROR, exceptionThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "Request fail, unknown Error");
        }
    }
    
    /**
     * send async request.
     * 发送异步请求： 1、服务端成员间 客户端数据认证请求，发送异步请求
     * @param request request.
     */
    public void asyncRequest(Request request, RequestCallBack callback) throws NacosException {
        int retryTimes = 0;
    
        Throwable exceptionToThrow = null;
        long start = System.currentTimeMillis();
        // 检查 小于 重试次数 并且 小于回调超时时间
        while (retryTimes < rpcClientConfig.retryTimes() && System.currentTimeMillis() < start + callback
                .getTimeout()) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    // 等待重新连接标识：当前客户端 连接对象为空，并且未运行状态
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "Client not connected.");
                }
                this.currentConnection.asyncRequest(request, callback);
                return;
            } catch (Throwable e) {
                if (waitReconnect) {
                    try {
                        // wait client to reconnect.
                        // 等待客户端重连
                        Thread.sleep(Math.min(100, callback.getTimeout() / 3));
                    } catch (Exception exception) {
                        // Do nothing.
                    }
                }
                LoggerUtils.printIfErrorEnabled(LOGGER,
                        "[{}] Send request fail, request = {}, retryTimes = {}, errorMessage = {}",
                        rpcClientConfig.name(), request, retryTimes, e.getMessage());
                exceptionToThrow = e;
                
            }
            retryTimes++;
            
        }
        
        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }
        if (exceptionToThrow != null) {
            throw (exceptionToThrow instanceof NacosException) ? (NacosException) exceptionToThrow
                    : new NacosException(SERVER_ERROR, exceptionToThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "AsyncRequest fail, unknown error");
        }
    }
    
    /**
     * send async request.
     *
     * @param request request.
     * @return request future.
     */
    public RequestFuture requestFuture(Request request) throws NacosException {
        int retryTimes = 0;
        long start = System.currentTimeMillis();
        Exception exceptionToThrow = null;
        while (retryTimes < rpcClientConfig.retryTimes() && System.currentTimeMillis() < start + rpcClientConfig
                .timeOutMills()) {
            boolean waitReconnect = false;
            try {
                if (this.currentConnection == null || !isRunning()) {
                    waitReconnect = true;
                    throw new NacosException(NacosException.CLIENT_INVALID_PARAM, "Client not connected.");
                }
                return this.currentConnection.requestFuture(request);
            } catch (Exception e) {
                if (waitReconnect) {
                    try {
                        // wait client to reconnect.
                        Thread.sleep(100L);
                    } catch (Exception exception) {
                        // Do nothing.
                    }
                }
                LoggerUtils.printIfErrorEnabled(LOGGER,
                        "[{}] Send request fail, request = {}, retryTimes = {}, errorMessage = {}",
                        rpcClientConfig.name(), request, retryTimes, e.getMessage());
                exceptionToThrow = e;
                
            }
            retryTimes++;
        }
        
        if (rpcClientStatus.compareAndSet(RpcClientStatus.RUNNING, RpcClientStatus.UNHEALTHY)) {
            switchServerAsyncOnRequestFail();
        }
        
        if (exceptionToThrow != null) {
            throw (exceptionToThrow instanceof NacosException) ? (NacosException) exceptionToThrow
                    : new NacosException(SERVER_ERROR, exceptionToThrow);
        } else {
            throw new NacosException(SERVER_ERROR, "Request future fail, unknown error");
        }
        
    }
    
    /**
     * connect to server.
     *
     * @param serverInfo server address to connect.
     * @return return connection when successfully connect to server, or null if failed.
     * @throws Exception exception when fail to connect to server.
     */
    public abstract Connection connectToServer(ServerInfo serverInfo) throws Exception;
    
    /**
     * handle server request.
     * 处理服务端请求：
     * 1、NotifySubscriberRequest 通知订阅者请求
     * 2、连接重置请求
     * @param request request.
     * @return response.
     */
    protected Response handleServerRequest(final Request request) {
        
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Receive server push request, request = {}, requestId = {}",
                rpcClientConfig.name(), request.getClass().getSimpleName(), request.getRequestId());
        lastActiveTimeStamp = System.currentTimeMillis();
        for (ServerRequestHandler serverRequestHandler : serverRequestHandlers) {
            try {
                Response response = serverRequestHandler.requestReply(request);
                
                if (response != null) {
                    LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Ack server push request, request = {}, requestId = {}",
                            rpcClientConfig.name(), request.getClass().getSimpleName(), request.getRequestId());
                    return response;
                }
            } catch (Exception e) {
                LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] HandleServerRequest:{}, errorMessage = {}",
                        rpcClientConfig.name(), serverRequestHandler.getClass().getName(), e.getMessage());
            }
            
        }
        return null;
    }

    /**
     * Register connection handler. Will be notified when inner connection's state changed.
     * 注册连接处理程序。当内部连接的状态改变时，将被通知
     *
     * @param connectionEventListener connectionEventListener
     */
    public synchronized void registerConnectionListener(ConnectionEventListener connectionEventListener) {
        
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Registry connection listener to current client:{}",
                rpcClientConfig.name(), connectionEventListener.getClass().getName());
        this.connectionEventListeners.add(connectionEventListener);
    }
    
    /**
     * Register serverRequestHandler, the handler will handle the request from server side.
     *
     * @param serverRequestHandler serverRequestHandler
     */
    public synchronized void registerServerRequestHandler(ServerRequestHandler serverRequestHandler) {
        LoggerUtils.printIfInfoEnabled(LOGGER, "[{}] Register server push request handler:{}", rpcClientConfig.name(),
                serverRequestHandler.getClass().getName());
        
        this.serverRequestHandlers.add(serverRequestHandler);
    }
    
    /**
     * Getter method for property <tt>name</tt>.
     *
     * @return property value of name
     */
    public String getName() {
        return rpcClientConfig.name();
    }
    
    /**
     * Getter method for property <tt>serverListFactory</tt>.
     *
     * @return property value of serverListFactory
     */
    public ServerListFactory getServerListFactory() {
        return serverListFactory;
    }
    
    protected ServerInfo nextRpcServer() {
        // 在 ServerListManager 中获取下一个 ip+ 端口号
        String serverAddress = getServerListFactory().genNextServer();
        return resolveServerInfo(serverAddress);
    }
    
    protected ServerInfo currentRpcServer() {
        String serverAddress = getServerListFactory().getCurrentServer();
        return resolveServerInfo(serverAddress);
    }
    
    /**
     * resolve server info.
     * 将 IP+端口号 的字符串，解析成 ServerInfo 对象
     * @param serverAddress address.
     * @return
     */
    @SuppressWarnings("PMD.UndefineMagicConstantRule")
    private ServerInfo resolveServerInfo(String serverAddress) {
        // itodo： 正则表达式判断的是什么？
        Matcher matcher = EXCLUDE_PROTOCOL_PATTERN.matcher(serverAddress);
        if (matcher.find()) {
            serverAddress = matcher.group(1);
        }
        // 获取 ip 端口号数组
        String[] ipPortTuple = InternetAddressUtil.splitIPPortStr(serverAddress);
        int defaultPort = Integer.parseInt(System.getProperty("nacos.server.port", "8848"));
        String serverPort = CollectionUtils.getOrDefault(ipPortTuple, 1, Integer.toString(defaultPort));
        
        return new ServerInfo(ipPortTuple[0], NumberUtils.toInt(serverPort, defaultPort));
    }
    
    public static class ServerInfo {
        
        protected String serverIp;
        
        protected int serverPort;
        
        public ServerInfo() {
        
        }
        
        public ServerInfo(String serverIp, int serverPort) {
            this.serverPort = serverPort;
            this.serverIp = serverIp;
        }
        
        /**
         * get address, ip:port.
         *
         * @return address.
         */
        public String getAddress() {
            return serverIp + Constants.COLON + serverPort;
        }
        
        /**
         * Setter method for property <tt>serverIp</tt>.
         *
         * @param serverIp value to be assigned to property serverIp
         */
        public void setServerIp(String serverIp) {
            this.serverIp = serverIp;
        }
        
        /**
         * Setter method for property <tt>serverPort</tt>.
         *
         * @param serverPort value to be assigned to property serverPort
         */
        public void setServerPort(int serverPort) {
            this.serverPort = serverPort;
        }
        
        /**
         * Getter method for property <tt>serverIp</tt>.
         *
         * @return property value of serverIp
         */
        public String getServerIp() {
            return serverIp;
        }
        
        /**
         * Getter method for property <tt>serverPort</tt>.
         *
         * @return property value of serverPort
         */
        public int getServerPort() {
            return serverPort;
        }
        
        @Override
        public String toString() {
            return "{serverIp = '" + serverIp + '\'' + ", server main port = " + serverPort + '}';
        }
    }
    
    public class ConnectionEvent {
        
        public static final int CONNECTED = 1;
        
        public static final int DISCONNECTED = 0;
        
        int eventType;
        
        public ConnectionEvent(int eventType) {
            this.eventType = eventType;
        }
        
        public boolean isConnected() {
            return eventType == CONNECTED;
        }
        
        public boolean isDisConnected() {
            return eventType == DISCONNECTED;
        }
    }
    
    /**
     * Getter method for property <tt>labels</tt>.
     *
     * @return property value of labels
     */
    public Map<String, String> getLabels() {
        return rpcClientConfig.labels();
    }
    
    class ReconnectContext {
        
        public ReconnectContext(ServerInfo serverInfo, boolean onRequestFail) {
            this.onRequestFail = onRequestFail;
            this.serverInfo = serverInfo;
        }
        
        boolean onRequestFail;
        
        ServerInfo serverInfo;
    }
    
    public String getTenant() {
        return tenant;
    }

    /**
     * clientWork 中设置，实际是 namespace 的 值
     * @param tenant
     */
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }
}
