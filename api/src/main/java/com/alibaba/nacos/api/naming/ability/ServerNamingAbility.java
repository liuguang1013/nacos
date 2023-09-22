/*
 * Copyright 1999-2021 Alibaba Group Holding Ltd.
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

package com.alibaba.nacos.api.naming.ability;

import java.io.Serializable;
import java.util.Objects;

/**
 * naming abilities of nacos server.
 *
 * @author liuzunfei
 * @version $Id: ServerNamingAbility.java, v 0.1 2021年01月24日 00:09 AM liuzunfei Exp $
 */
public class ServerNamingAbility implements Serializable {
    
    private static final long serialVersionUID = 8308895444341445512L;
    
    /**
     * Nacos server can use SOFA-Jraft to handle persist service and metadata.
     * Nacos服务器可以使用 SOFA-Jraft 来处理持久化服务和元数据。
     */
    private boolean supportJraft;
    
    public boolean isSupportJraft() {
        return supportJraft;
    }
    
    public void setSupportJraft(boolean supportJraft) {
        this.supportJraft = supportJraft;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof ServerNamingAbility)) {
            return false;
        }
        ServerNamingAbility that = (ServerNamingAbility) o;
        return supportJraft == that.supportJraft;
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(supportJraft);
    }
}
