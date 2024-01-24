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

package com.alibaba.nacos.naming.core.v2.client.factory.impl;

import com.alibaba.nacos.naming.constants.ClientConstants;
import com.alibaba.nacos.naming.core.v2.client.ClientAttributes;
import com.alibaba.nacos.naming.core.v2.client.factory.ClientFactory;
import com.alibaba.nacos.naming.core.v2.client.impl.ConnectionBasedClient;

import static com.alibaba.nacos.naming.constants.ClientConstants.REVISION;

/**
 * Client factory for {@link ConnectionBasedClient}.
 *
 * @author xiweng.yy
 */
public class ConnectionBasedClientFactory implements ClientFactory<ConnectionBasedClient> {
    
    @Override
    public String getType() {
        return ClientConstants.DEFAULT_FACTORY;
    }

    /**
     * 创建 客户端连接后的客户端对象，本服务端负责该客户端
     *
     * @param clientId client id
     * @param attributes client attributes
     * @return
     */
    @Override
    public ConnectionBasedClient newClient(String clientId, ClientAttributes attributes) {
        // 指定客户端的  版本，默认是 0，在服务端间会通过这个进行客户端 认证：详情见  DistroProtocol#startDistroTask() 中 startVerifyTask();
        long revision = attributes.getClientAttribute(REVISION, 0);
        // 创建基于连接的客户端
        // 这个客户端主要是 用来表示和 tcp 连接的绑定，当tcp 断开连接的时候对象应该被清除
        ConnectionBasedClient connectionBasedClient = new ConnectionBasedClient(clientId, true, revision);
        connectionBasedClient.setAttributes(attributes);
        return connectionBasedClient;
    }

    /**
     * 创建从其他服务端同步的客户端对象，不是本服务端负责
     *
     * @param clientId   client id
     * @param attributes client attributes
     * @return
     */
    @Override
    public ConnectionBasedClient newSyncedClient(String clientId, ClientAttributes attributes) {
        long revision = attributes.getClientAttribute(REVISION, 0);
        ConnectionBasedClient connectionBasedClient = new ConnectionBasedClient(clientId, false, revision);
        connectionBasedClient.setAttributes(attributes);
        return connectionBasedClient;
    }
}
