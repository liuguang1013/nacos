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

package com.alibaba.nacos.naming.ability;

import com.alibaba.nacos.api.ability.ServerAbilities;
import com.alibaba.nacos.core.ability.ServerAbilityInitializer;

/**
 * Server ability initializer for naming.
 *  服务端能力初始化器
 *  iTodo：服务端能力初始化器 什么时候加载？
 * @author xiweng.yy
 */
public class NamingAbilityInitializer implements ServerAbilityInitializer {
    
    @Override
    public void initialize(ServerAbilities abilities) {
        abilities.getNamingAbility().setSupportJraft(true);
    }
}
