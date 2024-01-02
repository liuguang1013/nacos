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

package com.alibaba.nacos.core.cluster;

import com.alibaba.nacos.api.ability.ServerAbilities;
import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.response.ResponseCode;
import com.alibaba.nacos.core.cluster.remote.request.MemberReportRequest;
import com.alibaba.nacos.auth.util.AuthHeaderUtil;
import com.alibaba.nacos.common.JustForTest;
import com.alibaba.nacos.common.http.Callback;
import com.alibaba.nacos.common.http.HttpClientBeanHolder;
import com.alibaba.nacos.common.http.HttpUtils;
import com.alibaba.nacos.common.http.client.NacosAsyncRestTemplate;
import com.alibaba.nacos.common.http.param.Header;
import com.alibaba.nacos.common.http.param.Query;
import com.alibaba.nacos.common.model.RestResult;
import com.alibaba.nacos.common.notify.Event;
import com.alibaba.nacos.common.notify.NotifyCenter;
import com.alibaba.nacos.common.notify.listener.Subscriber;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.VersionUtils;
import com.alibaba.nacos.core.ability.ServerAbilityInitializer;
import com.alibaba.nacos.core.ability.ServerAbilityInitializerHolder;
import com.alibaba.nacos.core.cluster.lookup.LookupFactory;
import com.alibaba.nacos.core.cluster.remote.ClusterRpcClientProxy;
import com.alibaba.nacos.core.cluster.remote.response.MemberReportResponse;
import com.alibaba.nacos.core.utils.Commons;
import com.alibaba.nacos.core.utils.GenericType;
import com.alibaba.nacos.core.utils.GlobalExecutor;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.sys.env.Constants;
import com.alibaba.nacos.sys.env.EnvUtil;
import com.alibaba.nacos.sys.utils.ApplicationUtils;
import com.alibaba.nacos.sys.utils.InetUtils;
import org.springframework.boot.web.context.WebServerInitializedEvent;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.servlet.ServletContext;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

import static com.alibaba.nacos.api.exception.NacosException.CLIENT_INVALID_PARAM;

/**
 * Cluster node management in Nacos.
 * nacos 的集群节点管理
 *
 * <p>{@link ServerMemberManager#init()} Cluster node manager initialization
 * 集群节点管理器初始化
 *
 * {@link ServerMemberManager#shutdown()} The cluster node manager is down
 * 集群节点管理器关闭
 *
 * {@link ServerMemberManager#getSelf()} Gets local node information
 * 获取本地节点信息
 *
 * {@link ServerMemberManager#getServerList()} Gets the cluster node dictionary
 * 获取集群节点字典
 *
 * {@link ServerMemberManager#getMemberAddressInfos()} Gets the address information of the healthy member node
 * 获取健康成员节点的地址信息
 *
 * {@link ServerMemberManager#allMembers()} Gets a list of member information objects
 * 获取成员信息对象的列表
 *
 * {@link ServerMemberManager#allMembersWithoutSelf()} Gets a list of cluster member nodes with the exception of this node
 * 获取群集成员节点的列表，但此节点除外
 *
 * {@link ServerMemberManager#hasMember(String)} Is there a node
 * 节点是否存在
 *
 * {@link ServerMemberManager#memberChange(Collection)} The final node list changes the method, making the full size more
 * 最后节点列表改变，使 size 更大
 *
 * {@link ServerMemberManager#memberJoin(Collection)} Node join, can automatically trigger
 * 加入节点，自动触发
 *
 * {@link ServerMemberManager#memberLeave(Collection)} When the node leaves, only the interface call can be manually triggered
 * 当节点断开，只能手动触发接口调用
 *
 * {@link ServerMemberManager#update(Member)} Update the target node information
 * 更新某节点信息
 *
 * {@link ServerMemberManager#isUnHealth(String)} Whether the target node is healthy
 * 判断即诶单是够健康
 *
 * {@link ServerMemberManager#initAndStartLookup()} Initializes the addressing mode
 * 初始化寻址模式
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
@Component(value = "serverMemberManager")
public class ServerMemberManager implements ApplicationListener<WebServerInitializedEvent> {
    
    private final NacosAsyncRestTemplate asyncRestTemplate = HttpClientBeanHolder
            .getNacosAsyncRestTemplate(Loggers.CORE);
    
    private static final int DEFAULT_SERVER_PORT = 8848;
    
    private static final String SERVER_PORT_PROPERTY = "server.port";
    
    private static final String SPRING_MANAGEMENT_CONTEXT_NAMESPACE = "management";
    
    private static final String MEMBER_CHANGE_EVENT_QUEUE_SIZE_PROPERTY = "nacos.member-change-event.queue.size";
    
    private static final int DEFAULT_MEMBER_CHANGE_EVENT_QUEUE_SIZE = 128;
    
    private static boolean isUseAddressServer = false;
    
    private static final long DEFAULT_TASK_DELAY_TIME = 5_000L;
    
    /**
     * Cluster node list.
     * key ： ip：port
     * value ： Member 对象
     */
    private volatile ConcurrentSkipListMap<String, Member> serverList;
    
    /**
     * Is this node in the cluster list.
     */
    private static volatile boolean isInIpList = true;
    
    /**
     * port.
     */
    private int port;
    
    /**
     * Address information for the local node.
     */
    private String localAddress;
    
    /**
     * Addressing pattern instances.
     */
    private MemberLookup lookup;
    
    /**
     * self member obj.
     */
    private volatile Member self;
    
    /**
     * here is always the node information of the "UP" state.
     */
    private volatile Set<String> memberAddressInfos = new ConcurrentHashSet<>();
    
    /**
     * Broadcast this node element information task.
     */
    private final MemberInfoReportTask infoReportTask = new MemberInfoReportTask();
    
    public ServerMemberManager(ServletContext servletContext) throws Exception {
        this.serverList = new ConcurrentSkipListMap<>();
        // 设置 ContextPath
        EnvUtil.setContextPath(servletContext.getContextPath());
        // 初始化方法
        init();
    }
    
    protected void init() throws NacosException {
        Loggers.CORE.info("Nacos-related cluster resource initialization");
        // 默认端口 8848
        this.port = EnvUtil.getProperty(SERVER_PORT_PROPERTY, Integer.class, DEFAULT_SERVER_PORT);
        // 获取本机地址
        this.localAddress = InetUtils.getSelfIP() + ":" + port;
        // 根据 ip 端口 创建 Member 对象
        this.self = MemberUtil.singleParse(this.localAddress);
        // 设置版本
        this.self.setExtendVal(MemberMetaDataConstants.VERSION, VersionUtils.version);
        
        // init abilities.
        // 初始化能力
        this.self.setAbilities(initMemberAbilities());
        // 服务端列表中 添加本节点
        serverList.put(self.getAddress(), self);
        
        // register NodeChangeEvent publisher to NotifyManager
        // 注册订阅 集群 事件
        registerClusterEvent();
        
        // Initializes the lookup mode
        // 初始化并查找其他服务端节点
        initAndStartLookup();
        
        if (serverList.isEmpty()) {
            throw new NacosException(NacosException.SERVER_ERROR, "cannot get serverlist, so exit.");
        }
        
        Loggers.CORE.info("The cluster resource is initialized");
    }
    
    private ServerAbilities initMemberAbilities() {
        ServerAbilities serverAbilities = new ServerAbilities();
        for (ServerAbilityInitializer each : ServerAbilityInitializerHolder.getInstance().getInitializers()) {
            // RemoteAbilityInitializer
            each.initialize(serverAbilities);
        }
        return serverAbilities;
    }
    
    private void registerClusterEvent() {
        // Register node change events

        // 注册 MembersChangeEvent 发布者
        NotifyCenter.registerToPublisher(MembersChangeEvent.class,
                EnvUtil.getProperty(MEMBER_CHANGE_EVENT_QUEUE_SIZE_PROPERTY, Integer.class,
                        DEFAULT_MEMBER_CHANGE_EVENT_QUEUE_SIZE));
        
        // The address information of this node needs to be dynamically modified
        // when registering the IP change of this node

        // 注册 订阅者
        // InetUtils 工具类 30s 一次检查 ip 是否变化
        // 在注册该节点的IP变更时，需要动态修改该节点的地址信息
        NotifyCenter.registerSubscriber(new Subscriber<InetUtils.IPChangeEvent>() {
            @Override
            public void onEvent(InetUtils.IPChangeEvent event) {
                String newAddress = event.getNewIP() + ":" + port;
                ServerMemberManager.this.localAddress = newAddress;
                EnvUtil.setLocalAddress(localAddress);
                
                Member self = ServerMemberManager.this.self;
                self.setIp(event.getNewIP());
                
                String oldAddress = event.getOldIP() + ":" + port;
                ServerMemberManager.this.serverList.remove(oldAddress);
                ServerMemberManager.this.serverList.put(newAddress, self);
                
                ServerMemberManager.this.memberAddressInfos.remove(oldAddress);
                ServerMemberManager.this.memberAddressInfos.add(newAddress);
            }
            
            @Override
            public Class<? extends Event> subscribeType() {
                return InetUtils.IPChangeEvent.class;
            }
        });
    }
    
    private void initAndStartLookup() throws NacosException {
        // 查找寻址类型
        this.lookup = LookupFactory.createLookUp(this);
        // 使用地址服务器： 只有 AddressServerMemberLookup 使用 远程地址
        isUseAddressServer = this.lookup.useAddressServer();
        // 真正的开始寻址
        this.lookup.start();
    }
    
    /**
     * switch look up.
     *
     * @param name look up name.
     * @throws NacosException exception.
     */
    public void switchLookup(String name) throws NacosException {
        this.lookup = LookupFactory.switchLookup(name, this);
        isUseAddressServer = this.lookup.useAddressServer();
        this.lookup.start();
    }
    
    public static boolean isUseAddressServer() {
        return isUseAddressServer;
    }
    
    /**
     * member information update.
     *
     * @param newMember {@link Member}
     * @return update is success
     */
    public boolean update(Member newMember) {
        Loggers.CLUSTER.debug("member information update : {}", newMember);
        
        String address = newMember.getAddress();
        if (!serverList.containsKey(address)) {
            Loggers.CLUSTER.warn("address {} want to update Member, but not in member list!", newMember.getAddress());
            return false;
        }
        
        serverList.computeIfPresent(address, (s, member) -> {
            if (NodeState.DOWN.equals(newMember.getState())) {
                memberAddressInfos.remove(newMember.getAddress());
            }
            boolean isPublishChangeEvent = MemberUtil.isBasicInfoChanged(newMember, member);
            newMember.setExtendVal(MemberMetaDataConstants.LAST_REFRESH_TIME, System.currentTimeMillis());
            MemberUtil.copy(newMember, member);
            if (isPublishChangeEvent) {
                // member basic data changes and all listeners need to be notified
                notifyMemberChange(member);
            }
            return member;
        });
        
        return true;
    }
    
    void notifyMemberChange(Member member) {
        NotifyCenter.publishEvent(MembersChangeEvent.builder().trigger(member).members(allMembers()).build());
    }
    
    /**
     * Whether the node exists within the cluster.
     *
     * @param address ip:port
     * @return is exists
     */
    public boolean hasMember(String address) {
        boolean result = serverList.containsKey(address);
        if (result) {
            return true;
        }
        
        // If only IP information is passed in, a fuzzy match is required
        for (Map.Entry<String, Member> entry : serverList.entrySet()) {
            if (StringUtils.contains(entry.getKey(), address)) {
                result = true;
                break;
            }
        }
        return result;
    }
    
    public List<String> getServerListUnhealth() {
        List<String> unhealthyMembers = new ArrayList<>();
        for (Member member : this.allMembers()) {
            NodeState state = member.getState();
            if (state.equals(NodeState.DOWN)) {
                unhealthyMembers.add(member.getAddress());
            }
            
        }
        return unhealthyMembers;
    }
    
    public MemberLookup getLookup() {
        return lookup;
    }
    
    public Member getSelf() {
        return this.self;
    }
    
    public Member find(String address) {
        return serverList.get(address);
    }
    
    /**
     * return this cluster all members.
     *
     * @return {@link Collection} all member
     */
    public Collection<Member> allMembers() {
        // We need to do a copy to avoid affecting the real data
        HashSet<Member> set = new HashSet<>(serverList.values());
        set.add(self);
        return set;
    }
    
    /**
     * return this cluster all members without self.
     *
     * @return {@link Collection} all member without self
     */
    public List<Member> allMembersWithoutSelf() {
        List<Member> members = new ArrayList<>(serverList.values());
        members.remove(self);
        return members;
    }
    
    synchronized boolean memberChange(Collection<Member> members) {
        
        if (members == null || members.isEmpty()) {
            return false;
        }
        // 判断节点中是否包含 本机节点
        boolean isContainSelfIp = members.stream()
                .anyMatch(ipPortTmp -> Objects.equals(localAddress, ipPortTmp.getAddress()));

        if (isContainSelfIp) {
            isInIpList = true;
        }
        // 不包含 将本机节点添加进去
        else {
            isInIpList = false;
            members.add(this.self);
            Loggers.CLUSTER.warn("[serverlist] self ip {} not in serverlist {}", self, members);
        }
        
        // If the number of old and new clusters is different, the cluster information
        // must have changed; if the number of clusters is the same, then compare whether
        // there is a difference; if there is a difference, then the cluster node changes
        // are involved and all recipients need to be notified of the node change event
        // 如果新旧集群数量不同，说明集群信息发生了变化;
        // 如果个数相同，则比较是否存在差异;
        // 如果存在差异，则涉及集群节点更改，并且需要通知所有接收方节点更改事件

        boolean hasChange = members.size() != serverList.size();
        // 临时 map 保存 新旧所有节点
        ConcurrentSkipListMap<String, Member> tmpMap = new ConcurrentSkipListMap<>();
        // 临时 set 保存在线节点
        Set<String> tmpAddressInfo = new ConcurrentHashSet<>();
        for (Member member : members) {
            final String address = member.getAddress();
            
            Member existMember = serverList.get(address);
            if (existMember == null) {
                hasChange = true;
                tmpMap.put(address, member);
            } else {
                //to keep extendInfo and abilities that report dynamically.
                tmpMap.put(address, existMember);
            }
            
            if (NodeState.UP.equals(member.getState())) {
                tmpAddressInfo.add(address);
            }
        }
        
        serverList = tmpMap;
        memberAddressInfos = tmpAddressInfo;
        // 获取所有 成员
        Collection<Member> finalMembers = allMembers();
        
        // Persist the current cluster node information to cluster.conf
        // <important> need to put the event publication into a synchronized block to ensure
        // that the event publication is sequential

        // 将当前集群节点信息持久化到cluster.conf
        // ！！！需要将事件发布放入同步块中，以确保事件发布是顺序的
        if (hasChange) {
            Loggers.CLUSTER.warn("[serverlist] updated to : {}", finalMembers);
            MemberUtil.syncToFile(finalMembers);
            Event event = MembersChangeEvent.builder().members(finalMembers).build();
            NotifyCenter.publishEvent(event);
        } else {
            if (Loggers.CLUSTER.isDebugEnabled()) {
                Loggers.CLUSTER.debug("[serverlist] not updated, is still : {}", finalMembers);
            }
        }
        
        return hasChange;
    }
    
    /**
     * members join this cluster.
     *
     * @param members {@link Collection} new members
     * @return is success
     */
    public synchronized boolean memberJoin(Collection<Member> members) {
        Set<Member> set = new HashSet<>(members);
        set.addAll(allMembers());
        return memberChange(set);
    }
    
    /**
     * members leave this cluster.
     *
     * @param members {@link Collection} wait leave members
     * @return is success
     */
    public synchronized boolean memberLeave(Collection<Member> members) {
        Set<Member> set = new HashSet<>(allMembers());
        set.removeAll(members);
        return memberChange(set);
    }
    
    /**
     * this member {@link Member#getState()} is health.
     *
     * @param address ip:port
     * @return is health
     */
    public boolean isUnHealth(String address) {
        Member member = serverList.get(address);
        if (member == null) {
            return false;
        }
        return !NodeState.UP.equals(member.getState());
    }
    
    public boolean isFirstIp() {
        return Objects.equals(serverList.firstKey(), this.localAddress);
    }
    
    @Override
    public void onApplicationEvent(WebServerInitializedEvent event) {
        String serverNamespace = event.getApplicationContext().getServerNamespace();
        if (SPRING_MANAGEMENT_CONTEXT_NAMESPACE.equals(serverNamespace)) {
            // ignore
            // fix#issue https://github.com/alibaba/nacos/issues/7230
            return;
        }
        getSelf().setState(NodeState.UP);
        if (!EnvUtil.getStandaloneMode()) {
            GlobalExecutor.scheduleByCommon(this.infoReportTask, DEFAULT_TASK_DELAY_TIME);
        }
        EnvUtil.setPort(event.getWebServer().getPort());
        EnvUtil.setLocalAddress(this.localAddress);
        Loggers.CLUSTER.info("This node is ready to provide external services");
    }
    
    /**
     * ServerMemberManager shutdown.
     *
     * @throws NacosException NacosException
     */
    @PreDestroy
    public void shutdown() throws NacosException {
        serverList.clear();
        memberAddressInfos.clear();
        infoReportTask.shutdown();
        LookupFactory.destroy();
    }
    
    public Set<String> getMemberAddressInfos() {
        return memberAddressInfos;
    }
    
    @JustForTest
    public void updateMember(Member member) {
        serverList.put(member.getAddress(), member);
    }
    
    @JustForTest
    public void setMemberAddressInfos(Set<String> memberAddressInfos) {
        this.memberAddressInfos = memberAddressInfos;
    }
    
    @JustForTest
    public MemberInfoReportTask getInfoReportTask() {
        return infoReportTask;
    }
    
    public Map<String, Member> getServerList() {
        return Collections.unmodifiableMap(serverList);
    }
    
    public static boolean isInIpList() {
        return isInIpList;
    }
    
    // Synchronize the metadata information of a node
    // A health check of the target node is also attached
    
    class MemberInfoReportTask extends Task {
        
        private final GenericType<RestResult<String>> reference = new GenericType<RestResult<String>>() {
        };
        
        private int cursor = 0;
    
        private ClusterRpcClientProxy clusterRpcClientProxy;
        
        @Override
        protected void executeBody() {
            List<Member> members = ServerMemberManager.this.allMembersWithoutSelf();
            
            if (members.isEmpty()) {
                return;
            }
            
            this.cursor = (this.cursor + 1) % members.size();
            Member target = members.get(cursor);
            
            Loggers.CLUSTER.debug("report the metadata to the node : {}", target.getAddress());
    
            if (target.getAbilities().getRemoteAbility().isGrpcReportEnabled()) {
                reportByGrpc(target);
            } else {
                reportByHttp(target);
            }
        }
        
        private void reportByHttp(Member target) {
            final String url = HttpUtils
                    .buildUrl(false, target.getAddress(), EnvUtil.getContextPath(), Commons.NACOS_CORE_CONTEXT,
                            "/cluster/report");
    
            try {
                Header header = Header.newInstance().addParam(Constants.NACOS_SERVER_HEADER, VersionUtils.version);
                AuthHeaderUtil.addIdentityToHeader(header);
                asyncRestTemplate
                        .post(url, header, Query.EMPTY, getSelf(), reference.getType(), new Callback<String>() {
                            @Override
                            public void onReceive(RestResult<String> result) {
                                if (result.ok()) {
                                    handleReportResult(result.getData(), target);
                                } else {
                                    Loggers.CLUSTER.warn("failed to report new info to target node : {}, result : {}",
                                            target.getAddress(), result);
                                    MemberUtil.onFail(ServerMemberManager.this, target);
                                }
                            }
                    
                            @Override
                            public void onError(Throwable throwable) {
                                Loggers.CLUSTER.error("failed to report new info to target node : {}, error : {}",
                                        target.getAddress(), ExceptionUtil.getAllExceptionMsg(throwable));
                                MemberUtil.onFail(ServerMemberManager.this, target, throwable);
                            }
                    
                            @Override
                            public void onCancel() {
                        
                            }
                        });
            } catch (Throwable ex) {
                Loggers.CLUSTER.error("failed to report new info to target node by http : {}, error : {}", target.getAddress(),
                        ExceptionUtil.getAllExceptionMsg(ex));
            }
        }
        
        private void reportByGrpc(Member target) {
            //Todo  circular reference
            if (Objects.isNull(clusterRpcClientProxy)) {
                clusterRpcClientProxy =  ApplicationUtils.getBean(ClusterRpcClientProxy.class);
            }
            if (!clusterRpcClientProxy.isRunning(target)) {
                MemberUtil.onFail(ServerMemberManager.this, target,
                        new NacosException(CLIENT_INVALID_PARAM, "No rpc client related to member: " + target));
                return;
            }
            
            MemberReportRequest memberReportRequest = new MemberReportRequest(getSelf());
            
            try {
                MemberReportResponse response = (MemberReportResponse) clusterRpcClientProxy.sendRequest(target, memberReportRequest);
                if (response.getResultCode() == ResponseCode.SUCCESS.getCode()) {
                    MemberUtil.onSuccess(ServerMemberManager.this, target, response.getNode());
                } else {
                    MemberUtil.onFail(ServerMemberManager.this, target);
                }
            } catch (NacosException e) {
                if (e.getErrCode() == NacosException.NO_HANDLER) {
                    target.getAbilities().getRemoteAbility().setGrpcReportEnabled(false);
                }
                Loggers.CLUSTER.error("failed to report new info to target node by grpc : {}, error : {}", target.getAddress(),
                        ExceptionUtil.getAllExceptionMsg(e));
            }
        }
        
        @Override
        protected void after() {
            GlobalExecutor.scheduleByCommon(this, 2_000L);
        }
        
        private void handleReportResult(String reportResult, Member target) {
            if (isBooleanResult(reportResult)) {
                MemberUtil.onSuccess(ServerMemberManager.this, target);
                return;
            }
            try {
                Member member = JacksonUtils.toObj(reportResult, Member.class);
                MemberUtil.onSuccess(ServerMemberManager.this, target, member);
            } catch (Exception e) {
                Loggers.CLUSTER.warn("Receive invalid report result from target {}, context {}", target.getAddress(),
                        reportResult);
                MemberUtil.onSuccess(ServerMemberManager.this, target);
            }
        }
        
        private boolean isBooleanResult(String reportResult) {
            return Boolean.TRUE.toString().equals(reportResult) || Boolean.FALSE.toString().equals(reportResult);
        }
    }
    
}
