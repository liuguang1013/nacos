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

package com.alibaba.nacos.core.remote;

import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.core.control.TpsControl;
import com.alibaba.nacos.core.control.TpsControlConfig;
import com.alibaba.nacos.plugin.control.ControlManagerCenter;
import org.springframework.context.ApplicationListener;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.stereotype.Service;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * RequestHandlerRegistry.
 *  请求处理器注册  监听 SpringBoot 启动中的 容器刷新事件
 * @author liuzunfei
 * @version $Id: RequestHandlerRegistry.java, v 0.1 2020年07月13日 8:24 PM liuzunfei Exp $
 */

@Service
public class RequestHandlerRegistry implements ApplicationListener<ContextRefreshedEvent> {

    /**
     * 缓存 各类请求的处理器
     */
    Map<String, RequestHandler> registryHandlers = new HashMap<>();
    
    /**
     * Get Request Handler By request Type.
     *
     * @param requestType see definitions  of sub constants classes of RequestTypeConstants
     * @return request handler.
     */
    public RequestHandler getByRequestType(String requestType) {
        return registryHandlers.get(requestType);
    }
    
    @Override
    public void onApplicationEvent(ContextRefreshedEvent event) {
        // 获取请求处理器的实现类， 根据类名很容易识别出作用
        Map<String, RequestHandler> beansOfType = event.getApplicationContext().getBeansOfType(RequestHandler.class);
        Collection<RequestHandler> values = beansOfType.values();
        for (RequestHandler requestHandler : values) {
            
            Class<?> clazz = requestHandler.getClass();
            boolean skip = false;
            // 查找类的 父类，直到父类是 RequestHandler.class 为止
            while (!clazz.getSuperclass().equals(RequestHandler.class)) {
                // 不是 RequestHandler 的子类
                if (clazz.getSuperclass().equals(Object.class)) {
                    skip = true;
                    break;
                }
                clazz = clazz.getSuperclass();
            }
            if (skip) {
                continue;
            }
            
            try {
                // 反射获取方法
                Method method = clazz.getMethod("handle", Request.class, RequestMeta.class);
                // 存在 @TpsControl注解，并且是开启 tps 监控
                if (method.isAnnotationPresent(TpsControl.class) && TpsControlConfig.isTpsControlEnabled()) {
                    TpsControl tpsControl = method.getAnnotation(TpsControl.class);
                    String pointName = tpsControl.pointName();
                    ControlManagerCenter.getInstance().getTpsControlManager().registerTpsPoint(pointName);
                }
            } catch (Exception e) {
                //ignore.
            }
            Class tClass = (Class) ((ParameterizedType) clazz.getGenericSuperclass()).getActualTypeArguments()[0];
            // 向缓存中添加，某类的请求的 处理器
            registryHandlers.putIfAbsent(tClass.getSimpleName(), requestHandler);
        }
    }
}
