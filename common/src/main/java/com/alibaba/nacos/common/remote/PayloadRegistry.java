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

package com.alibaba.nacos.common.remote;

import com.alibaba.nacos.api.remote.Payload;

import java.lang.reflect.Modifier;
import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;

/**
 * payload registry,Define basic scan behavior request and response.
 * 有效载荷注册表，定义基本扫描行为请求和响应。
 * @author liuzunfei
 * @author hujun
 * @version $Id: PayloadRegistry.java, v 0.1 2020年09月01日 10:56 AM liuzunfei Exp $
 */

public class PayloadRegistry {

    /**
     * key：Payload 接口实现类的 简单类名
     */
    private static final Map<String, Class<?>> REGISTRY_REQUEST = new HashMap<>();
    
    static boolean initialized = false;
    
    public static void init() {
        // 扫描
        scan();
    }
    
    private static synchronized void scan() {
        if (initialized) {
            return;
        }
        // 加载 实现 Payload 接口的实现类
        ServiceLoader<Payload> payloads = ServiceLoader.load(Payload.class);
        for (Payload payload : payloads) {
            register(payload.getClass().getSimpleName(), payload.getClass());
        }
        initialized = true;
    }
    
    static void register(String type, Class<?> clazz) {
        // 判断是不是抽象类
        if (Modifier.isAbstract(clazz.getModifiers())) {
            return;
        }
        // 判断是否已经加载过，防止类重复加载
        if (REGISTRY_REQUEST.containsKey(type)) {
            throw new RuntimeException(String.format("Fail to register, type:%s ,clazz:%s ", type, clazz.getName()));
        }
        REGISTRY_REQUEST.put(type, clazz);
    }
    
    public static Class<?> getClassByType(String type) {
        return REGISTRY_REQUEST.get(type);
    }
}
