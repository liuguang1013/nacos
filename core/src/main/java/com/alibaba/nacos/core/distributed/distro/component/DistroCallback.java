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

package com.alibaba.nacos.core.distributed.distro.component;

/**
 * Distro callback.
 * Distro 协议，服务端间发送异步请求时候使用
 * 1、DistroVerifyCallback：当 DistroProtocol 对象初始化的时候，延迟任务执行服务期间负责的客户端的验证，发送 DistroDataRequest VERIFY 类型的时候使用
 * 2、匿名内部类：当 DistroProtocol 对象初始化的时候，加载任务时使用
 * @author xiweng.yy
 */
public interface DistroCallback {
    
    /**
     * Callback when distro task execute successfully.
     */
    void onSuccess();
    
    /**
     * Callback when distro task execute failed.
     *
     * @param throwable throwable if execute failed caused by exception
     */
    void onFailed(Throwable throwable);
}
