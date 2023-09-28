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

package com.alibaba.nacos.core.auth;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.remote.request.Request;
import com.alibaba.nacos.api.remote.request.RequestMeta;
import com.alibaba.nacos.api.remote.response.Response;
import com.alibaba.nacos.auth.GrpcProtocolAuthService;
import com.alibaba.nacos.auth.annotation.Secured;
import com.alibaba.nacos.auth.config.AuthConfigs;
import com.alibaba.nacos.common.utils.ExceptionUtil;
import com.alibaba.nacos.core.remote.AbstractRequestFilter;
import com.alibaba.nacos.core.utils.Loggers;
import com.alibaba.nacos.plugin.auth.api.IdentityContext;
import com.alibaba.nacos.plugin.auth.api.Permission;
import com.alibaba.nacos.plugin.auth.api.Resource;
import com.alibaba.nacos.plugin.auth.constant.Constants;
import com.alibaba.nacos.plugin.auth.exception.AccessException;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;

/**
 * request auth filter for remote.
 *
 * @author liuzunfei
 * @version $Id: RemoteRequestAuthFilter.java, v 0.1 2020年09月14日 12:38 PM liuzunfei Exp $
 */
@Component
public class RemoteRequestAuthFilter extends AbstractRequestFilter {
    
    private final AuthConfigs authConfigs;
    
    private final GrpcProtocolAuthService protocolAuthService;
    
    public RemoteRequestAuthFilter(AuthConfigs authConfigs) {
        this.authConfigs = authConfigs;
        this.protocolAuthService = new GrpcProtocolAuthService(authConfigs);
        this.protocolAuthService.initialize();
    }
    
    @Override
    public Response filter(Request request, RequestMeta meta, Class handlerClazz) throws NacosException {
        
        try {
            // 反射获取 handle 方法
            Method method = getHandleMethod(handlerClazz);
            // 判断是否有 Secured 注解，并且开启认证
            if (method.isAnnotationPresent(Secured.class) && authConfigs.isAuthEnabled()) {
                
                if (Loggers.AUTH.isDebugEnabled()) {
                    Loggers.AUTH.debug("auth start, request: {}", request.getClass().getSimpleName());
                }
                // 获取 @Secured 注解
                Secured secured = method.getAnnotation(Secured.class);
                // 不能认证直接进行下一个 过滤器
                if (!protocolAuthService.enableAuth(secured)) {
                    return null;
                }
                // 获取客户端 ip，放入请求头
                String clientIp = meta.getClientIp();
                request.putHeader(Constants.Identity.X_REAL_IP, clientIp);
                // 开始解析资源
                Resource resource = protocolAuthService.parseResource(request, secured);
                // 验证 token
                IdentityContext identityContext = protocolAuthService.parseIdentity(request);
                boolean result = protocolAuthService.validateIdentity(identityContext, resource);
                // 认证不通过，抛出异常
                if (!result) {
                    // TODO Get reason of failure
                    throw new AccessException("Validate Identity failed.");
                }
                // 验证权限
                String action = secured.action().toString();
                result = protocolAuthService.validateAuthority(identityContext, new Permission(resource, action));
                if (!result) {
                    // TODO Get reason of failure
                    throw new AccessException("Validate Authority failed.");
                }
            }
        } catch (AccessException e) {
            if (Loggers.AUTH.isDebugEnabled()) {
                Loggers.AUTH.debug("access denied, request: {}, reason: {}", request.getClass().getSimpleName(),
                        e.getErrMsg());
            }
            Response defaultResponseInstance = getDefaultResponseInstance(handlerClazz);
            defaultResponseInstance.setErrorInfo(NacosException.NO_RIGHT, e.getErrMsg());
            return defaultResponseInstance;
        } catch (Exception e) {
            Response defaultResponseInstance = getDefaultResponseInstance(handlerClazz);
            
            defaultResponseInstance.setErrorInfo(NacosException.SERVER_ERROR, ExceptionUtil.getAllExceptionMsg(e));
            return defaultResponseInstance;
        }
        
        return null;
    }
}
