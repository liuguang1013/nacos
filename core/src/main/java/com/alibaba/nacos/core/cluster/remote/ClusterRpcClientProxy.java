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

package com.alibaba.nacos.core.cluster.remote;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.RemoteConstants;
import com.alibaba.nacos.api.remote.RequestCallBack;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.remote.ConnectionType;
import com.alibaba.nacos.common.remote.client.RpcClient;
import com.alibaba.nacos.common.remote.client.RpcClientFactory;
import com.alibaba.nacos.common.remote.client.ServerListFactory;
import com.alibaba.nacos.common.utils.CollectionUtils;
import com.alibaba.nacos.core.cluster.Member;
import com.alibaba.nacos.core.cluster.MemberChangeListener;
import com.alibaba.nacos.core.cluster.MembersChangeEvent;
import com.alibaba.nacos.core.cluster.ServerMemberManager;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.EnvUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static com.alibaba.nacos.api.exception.NacosException.CLIENT_INVALID_PARAM;

/**
 * cluster rpc client proxy.
 * 集群客户端代理：用于服务端成员之间的 rpc 通信
 *
 * @author liuzunfei
 * @version $Id: ClusterRpcClientProxy.java, v 0.1 2020年08月11日 2:11 PM liuzunfei Exp $
 */
@Service
public class ClusterRpcClientProxy extends MemberChangeListener {
    
    private static final long DEFAULT_REQUEST_TIME_OUT = 3000L;
    
    @Autowired
    ServerMemberManager serverMemberManager;
    
    /**
     * init after constructor.
     * 初始化方法
     */
    @PostConstruct
    public void init() {
        try {
            // 订阅 集群成员改变事件
            NotifyCenter.registerSubscriber(this);
            // 获取集群所有成员
            List<Member> members = serverMemberManager.allMembersWithoutSelf();
            // 刷新
            refresh(members);

            Loggers.CLUSTER
                    .info("[ClusterRpcClientProxy] success to refresh cluster rpc client on start up,members ={} ",
                            members);
        } catch (NacosException e) {
            Loggers.CLUSTER.warn("[ClusterRpcClientProxy] fail to refresh cluster rpc client,{} ", e.getMessage());
        }
        
    }
    
    /**
     * init cluster rpc clients.
     * 初始化集群 rpc 客户端
     *
     * @param members cluster server list member list.
     */
    private void refresh(List<Member> members) throws NacosException {
        
        //ensure to create client of new members
        // 确保创建新成员客户端，
        // 和其他每个服务端都连接
        for (Member member : members) {
            createRpcClientAndStart(member, ConnectionType.GRPC);
        }
        
        //shutdown and remove old members.
        // 获取所有客户端连接
        Set<Map.Entry<String, RpcClient>> allClientEntrys = RpcClientFactory.getAllClientEntries();
        Iterator<Map.Entry<String, RpcClient>> iterator = allClientEntrys.iterator();
        // 获取新 客户端对象的 key
        List<String> newMemberKeys = members.stream().map(this::memberClientKey).collect(Collectors.toList());
        // 遍历 旧客户端对象，不在新客户端列表中的关闭，移除缓存
        while (iterator.hasNext()) {
            Map.Entry<String, RpcClient> next1 = iterator.next();
            if (next1.getKey().startsWith("Cluster-") && !newMemberKeys.contains(next1.getKey())) {
                Loggers.CLUSTER.info("member leave,destroy client of member - > : {}", next1.getKey());
                // 关闭客户端
                RpcClientFactory.getClient(next1.getKey()).shutdown();
                iterator.remove();
            }
        }
        
    }
    
    private String memberClientKey(Member member) {
        return "Cluster-" + member.getAddress();
    }
    
    private void createRpcClientAndStart(Member member, ConnectionType type) throws NacosException {
        Map<String, String> labels = new HashMap<>(2);
        // 加入标签： source  cluster
        labels.put(RemoteConstants.LABEL_SOURCE, RemoteConstants.LABEL_SOURCE_CLUSTER);
        // Cluster-ip+端口
        String memberClientKey = memberClientKey(member);
        // 创建服务端集群间，grpc 客户端，端口 9849
        RpcClient client = buildRpcClient(type, labels, memberClientKey);

        if (!client.getConnectionType().equals(type)) {
            Loggers.CLUSTER.info("connection type changed,destroy client of member - > : {}", member);
            RpcClientFactory.destroyClient(memberClientKey);
            // itodo： 为社么此处再次重新创建？
            client = buildRpcClient(type, labels, memberClientKey);
        }

        // 判断客户端 是否为初次创建，等待初始化，此处的判断，主要是控制只在初次的时候调用
        if (client.isWaitInitiated()) {
            Loggers.CLUSTER.info("start a new rpc client to member - > : {}", member);
            
            //one fixed server
            client.serverListFactory(new ServerListFactory() {
                @Override
                public String genNextServer() {
                    return member.getAddress();
                }
                
                @Override
                public String getCurrentServer() {
                    return member.getAddress();
                }
                
                @Override
                public List<String> getServerList() {
                    return CollectionUtils.list(member.getAddress());
                }
            });
            
            client.start();
        }
    }
    
    /**
     * Using {@link EnvUtil#getAvailableProcessors(int)} to build cluster clients' grpc thread pool.
     *
     * 通过工厂类创建 grpc 的客户端
     */
    private RpcClient buildRpcClient(ConnectionType type, Map<String, String> labels, String memberClientKey) {
        RpcClient clusterClient = RpcClientFactory
                .createClusterClient(memberClientKey, type, EnvUtil.getAvailableProcessors(2),
                        EnvUtil.getAvailableProcessors(8), labels);
        return clusterClient;
    }
    
    /**
     * send request to member.
     *
     * @param member  member of server.
     * @param request request.
     * @return Response response.
     * @throws NacosException exception may throws.
     */
    public Response sendRequest(Member member, Request request) throws NacosException {
        return sendRequest(member, request, DEFAULT_REQUEST_TIME_OUT);
    }
    
    /**
     * send request to member.
     * 同步请求
     *
     * @param member  member of server.
     * @param request request.
     * @return Response response.
     * @throws NacosException exception may throws.
     */
    public Response sendRequest(Member member, Request request, long timeoutMills) throws NacosException {
        RpcClient client = RpcClientFactory.getClient(memberClientKey(member));
        if (client != null) {
            return client.request(request, timeoutMills);
        } else {
            throw new NacosException(CLIENT_INVALID_PARAM, "No rpc client related to member: " + member);
        }
    }
    
    /**
     * aync send request to member with callback.
     * 异步请求，带有回调
     *
     * @param member   member of server.
     * @param request  request.
     * @param callBack RequestCallBack.
     * @throws NacosException exception may throws.
     */
    public void asyncRequest(Member member, Request request, RequestCallBack callBack) throws NacosException {
        RpcClient client = RpcClientFactory.getClient(memberClientKey(member));
        if (client != null) {
            client.asyncRequest(request, callBack);
        } else {
            throw new NacosException(CLIENT_INVALID_PARAM, "No rpc client related to member: " + member);
        }
    }
    
    /**
     * send request to member.
     *
     * @param request request.
     * @throws NacosException exception may throw.
     */
    public void sendRequestToAllMembers(Request request) throws NacosException {
        List<Member> members = serverMemberManager.allMembersWithoutSelf();
        for (Member member1 : members) {
            sendRequest(member1, request);
        }
    }
    
    @Override
    public void onEvent(MembersChangeEvent event) {
        try {
            List<Member> members = serverMemberManager.allMembersWithoutSelf();
            // 刷新服务端成员， 创建或者销毁 集群间客户端对象
            refresh(members);
        } catch (NacosException e) {
            Loggers.CLUSTER.warn("[serverlist] fail to refresh cluster rpc client, event:{}, msg: {} ", event, e.getMessage());
        }
    }
    
    /**
     * Check whether client for member is running.
     *
     * @param member member
     * @return {@code true} if target client is connected, otherwise {@code false}
     */
    public boolean isRunning(Member member) {
        RpcClient client = RpcClientFactory.getClient(memberClientKey(member));
        if (null == client) {
            return false;
        }
        return client.isRunning();
    }
}
