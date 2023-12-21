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

package com.alibaba.nacos.naming.core.v2.client.manager;

import com.alibaba.nacos.naming.consistency.ephemeral.distro.v2.DistroClientVerifyInfo;
import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.client.Client;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.impl.IpPortBasedClient;
import com.alibaba.nacos.naming.core.v2.client.manager.impl.ConnectionBasedClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.impl.EphemeralIpPortClientManager;
import com.alibaba.nacos.naming.core.v2.client.manager.impl.PersistentIpPortClientManager;
import org.springframework.context.annotation.DependsOn;
import org.springframework.stereotype.Component;

import java.util.Collection;
import java.util.HashSet;

/**
 * Client manager delegate.
 *
 * @author xiweng.yy
 */
@DependsOn({"clientServiceIndexesManager", "namingMetadataManager"})
@Component("clientManager")
public class ClientManagerDelegate implements ClientManager {
    
    private final ConnectionBasedClientManager connectionBasedClientManager;
    
    private final EphemeralIpPortClientManager ephemeralIpPortClientManager;
    
    private final PersistentIpPortClientManager persistentIpPortClientManager;

    /**
     * 客户端管理 代理
     * @param connectionBasedClientManager 基于连接的类型的 客户端管理
     * @param ephemeralIpPortClientManager 短暂的ip + 端口 类型的 客户端管理
     * @param persistentIpPortClientManager 持久的 ip + 端口 类型的 客户端管理
     */
    public ClientManagerDelegate(ConnectionBasedClientManager connectionBasedClientManager,
            EphemeralIpPortClientManager ephemeralIpPortClientManager,
            PersistentIpPortClientManager persistentIpPortClientManager) {
        this.connectionBasedClientManager = connectionBasedClientManager;
        this.ephemeralIpPortClientManager = ephemeralIpPortClientManager;
        this.persistentIpPortClientManager = persistentIpPortClientManager;
    }
    
    @Override
    public boolean clientConnected(String clientId, ClientAttributes attributes) {
        return getClientManagerById(clientId).clientConnected(clientId, attributes);
    }
    
    @Override
    public boolean clientConnected(Client client) {
        return getClientManagerById(client.getClientId()).clientConnected(client);
    }
    
    @Override
    public boolean syncClientConnected(String clientId, ClientAttributes attributes) {
        return getClientManagerById(clientId).syncClientConnected(clientId, attributes);
    }
    
    @Override
    public boolean clientDisconnected(String clientId) {
        return getClientManagerById(clientId).clientDisconnected(clientId);
    }

    /**
     *
     * @param clientId client id  grpc 的 context 中获取的连接id
     * @return
     */
    @Override
    public Client getClient(String clientId) {
        return getClientManagerById(clientId).getClient(clientId);
    }
    
    @Override
    public boolean contains(String clientId) {
        return connectionBasedClientManager.contains(clientId) || ephemeralIpPortClientManager.contains(clientId)
                || persistentIpPortClientManager.contains(clientId);
    }
    
    @Override
    public Collection<String> allClientId() {
        Collection<String> result = new HashSet<>();
        result.addAll(connectionBasedClientManager.allClientId());
        result.addAll(ephemeralIpPortClientManager.allClientId());
        result.addAll(persistentIpPortClientManager.allClientId());
        return result;
    }
    
    @Override
    public boolean isResponsibleClient(Client client) {
        return getClientManagerById(client.getClientId()).isResponsibleClient(client);
    }
    
    @Override
    public boolean verifyClient(DistroClientVerifyInfo verifyData) {
        return getClientManagerById(verifyData.getClientId()).verifyClient(verifyData);
    }
    
    private ClientManager getClientManagerById(String clientId) {
        // 客户端id 不包含 # 字符就是基于连接的客户端
        if (isConnectionBasedClient(clientId)) {
            return connectionBasedClientManager;
        }
        // 连接id 字符串以 false 结尾是 持久连接，否则是短暂连接
        return clientId.endsWith(ClientConstants.PERSISTENT_SUFFIX) ? persistentIpPortClientManager : ephemeralIpPortClientManager;
    }
    
    private boolean isConnectionBasedClient(String clientId) {
        return !clientId.contains(IpPortBasedClient.ID_DELIMITER);
    }
}
