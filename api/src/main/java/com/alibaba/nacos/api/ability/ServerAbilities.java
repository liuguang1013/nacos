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

package com.alibaba.nacos.api.ability;

import com.alibaba.nacos.api.config.ability.ServerConfigAbility;
import com.alibaba.nacos.api.naming.ability.ServerNamingAbility;
import com.alibaba.nacos.api.remote.ability.ServerRemoteAbility;

import java.io.Serializable;
import java.util.Objects;

/**
 * abilities of nacos server.
 * 服务端的能力：
 * @author liuzunfei
 * @version $Id: ServerAbilities.java, v 0.1 2021年01月24日 00:09 AM liuzunfei Exp $
 */
public class ServerAbilities implements Serializable {
    
    private static final long serialVersionUID = -2120543002911304171L;
    /**
     * 服务端 远程能力，代码中设定 默认使用 grpc
     * supportRemoteConnection
     * grpcReportEnabled
     *
     */
    private ServerRemoteAbility remoteAbility = new ServerRemoteAbility();

    /**
     * 服务端 配置能力
     * supportRemoteMetrics
     */
    private ServerConfigAbility configAbility = new ServerConfigAbility();

    /**
     * 服务端 命名能力
     * supportJraft     NamingAbilityInitializer 中设置为 true
     *
     */
    private ServerNamingAbility namingAbility = new ServerNamingAbility();
    
    public ServerRemoteAbility getRemoteAbility() {
        return remoteAbility;
    }
    
    public void setRemoteAbility(ServerRemoteAbility remoteAbility) {
        this.remoteAbility = remoteAbility;
    }
    
    public ServerConfigAbility getConfigAbility() {
        return configAbility;
    }
    
    public void setConfigAbility(ServerConfigAbility configAbility) {
        this.configAbility = configAbility;
    }
    
    public ServerNamingAbility getNamingAbility() {
        return namingAbility;
    }
    
    public void setNamingAbility(ServerNamingAbility namingAbility) {
        this.namingAbility = namingAbility;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ServerAbilities that = (ServerAbilities) o;
        return Objects.equals(remoteAbility, that.remoteAbility) && Objects.equals(configAbility, that.configAbility)
                && Objects.equals(namingAbility, that.namingAbility);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(remoteAbility, configAbility, namingAbility);
    }
}
