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

package com.alibaba.nacos.client.naming.backups;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.api.naming.pojo.ServiceInfo;
import com.alibaba.nacos.client.naming.cache.ConcurrentDiskUtil;
import com.alibaba.nacos.client.naming.cache.DiskCache;
import com.alibaba.nacos.client.naming.cache.ServiceInfoHolder;
import com.alibaba.nacos.client.naming.utils.CollectionUtils;
import com.alibaba.nacos.client.naming.utils.UtilAndComs;
import com.alibaba.nacos.common.lifecycle.Closeable;
import com.alibaba.nacos.common.utils.JacksonUtils;
import com.alibaba.nacos.common.utils.StringUtils;
import com.alibaba.nacos.common.utils.ThreadUtils;

import java.io.BufferedReader;
import java.io.File;
import java.io.StringReader;
import java.net.URLDecoder;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimerTask;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.alibaba.nacos.client.utils.LogUtils.NAMING_LOGGER;

/**
 * Failover reactor.
 *
 * @author nkorange
 */
public class FailoverReactor implements Closeable {
    
    private static final String FAILOVER_DIR = "/failover";
    
    private static final String IS_FAILOVER_MODE = "1";
    
    private static final String NO_FAILOVER_MODE = "0";
    
    private static final String FAILOVER_MODE_PARAM = "failover-mode";
    
    private Map<String, ServiceInfo> serviceMap = new ConcurrentHashMap<>();
    
    private final Map<String, String> switchParams = new ConcurrentHashMap<>();
    
    private static final long DAY_PERIOD_MINUTES = 24 * 60;

    /**
     *  ${jmSnapshotPath}/nacos/${namingCacheRegistryDir}/naming/${namespace}/failover
     *  或者
     *   ${user.home}/nacos/${namingCacheRegistryDir}/naming/${namespace}/failover
     */
    private final String failoverDir;
    
    private final ServiceInfoHolder serviceInfoHolder;
    
    private final ScheduledExecutorService executorService;
    
    public FailoverReactor(ServiceInfoHolder serviceInfoHolder, String cacheDir) {
        this.serviceInfoHolder = serviceInfoHolder;
        this.failoverDir = cacheDir + FAILOVER_DIR;
        // init executorService
        this.executorService = new ScheduledThreadPoolExecutor(1, r -> {
            Thread thread = new Thread(r);
            thread.setDaemon(true);
            thread.setName("com.alibaba.nacos.naming.failover");
            return thread;
        });
        this.init();
    }
    
    /**
     * Init.
     * 将 serviceInfoHolder 中的服务信息 持久化到 failoverDir路径下磁盘中，之后每天持久化一次
     * 同时每 5s 检查一次，是否 失败恢复开关打开，打开就加载 failoverDir 路径下服务信息到 FailoverReactor.serviceMap 中
     *
     */
    public void init() {
        // 初始无延迟，之后每 5s 执行一次
        executorService.scheduleWithFixedDelay(new SwitchRefresher(), 0L, 5000L, TimeUnit.MILLISECONDS);
        // 初始延迟30 ms，之后每天之执行一次
        executorService.scheduleWithFixedDelay(new DiskFileWriter(), 30, DAY_PERIOD_MINUTES, TimeUnit.MINUTES);
        
        // backup file on startup if failover directory is empty.
        // 延迟 10s 后执行一次
        executorService.schedule(() -> {
            try {
                File cacheDir = new File(failoverDir);
                // 创建 失败重试文件夹
                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }

                File[] files = cacheDir.listFiles();
                if (files == null || files.length <= 0) {
                    // 空文件夹 执行一次任务
                    new DiskFileWriter().run();
                }
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to backup file on startup.", e);
            }

        }, 10000L, TimeUnit.MILLISECONDS);
    }
    
    /**
     * Add day.
     *
     * @param date start time
     * @param num  add day number
     * @return new date
     */
    public Date addDay(Date date, int num) {
        Calendar startDT = Calendar.getInstance();
        startDT.setTime(date);
        startDT.add(Calendar.DAY_OF_MONTH, num);
        return startDT.getTime();
    }
    
    @Override
    public void shutdown() throws NacosException {
        String className = this.getClass().getName();
        NAMING_LOGGER.info("{} do shutdown begin", className);
        ThreadUtils.shutdownThreadPool(executorService, NAMING_LOGGER);
        NAMING_LOGGER.info("{} do shutdown stop", className);
    }

    /**
     * 开关刷新器：
     */
    class SwitchRefresher implements Runnable {
        
        long lastModifiedMillis = 0L;
        
        @Override
        public void run() {
            try {
                File switchFile = Paths.get(failoverDir, UtilAndComs.FAILOVER_SWITCH).toFile();
                // 00-00---000-VIPSRV_FAILOVER_SWITCH-000---00-00 文件不存在
                if (!switchFile.exists()) {
                    // 设置参数： 失败转移模式  false
                    switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    NAMING_LOGGER.debug("failover switch is not found, {}", switchFile.getName());
                    return;
                }
                // 文件上次修改时间
                long modified = switchFile.lastModified();
                // 文件上次修改时间 大于 内存中保存的时间
                if (lastModifiedMillis < modified) {
                    lastModifiedMillis = modified;
                    String failover = ConcurrentDiskUtil.getFileContent(failoverDir + UtilAndComs.FAILOVER_SWITCH,
                            Charset.defaultCharset().toString());
                    // 文件内容不为空
                    if (!StringUtils.isEmpty(failover)) {
                        String[] lines = failover.split(DiskCache.getLineSeparator());
                        
                        for (String line : lines) {
                            String line1 = line.trim();
                            if (IS_FAILOVER_MODE.equals(line1)) {
                                // 失败转移 模式打开
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.TRUE.toString());
                                NAMING_LOGGER.info("failover-mode is on");
                                // 执行 失败转移任务
                                new FailoverFileReader().run();
                            } else if (NO_FAILOVER_MODE.equals(line1)) {
                                switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                                NAMING_LOGGER.info("failover-mode is off");
                            }
                        }
                    } else {
                        switchParams.put(FAILOVER_MODE_PARAM, Boolean.FALSE.toString());
                    }
                }
                
            } catch (Throwable e) {
                NAMING_LOGGER.error("[NA] failed to read failover switch.", e);
            }
        }
    }
    
    class FailoverFileReader implements Runnable {
        
        @Override
        public void run() {
            Map<String, ServiceInfo> domMap = new HashMap<>(16);
            
            BufferedReader reader = null;
            try {
                
                File cacheDir = new File(failoverDir);
                if (!cacheDir.exists() && !cacheDir.mkdirs()) {
                    throw new IllegalStateException("failed to create cache dir: " + failoverDir);
                }
                
                File[] files = cacheDir.listFiles();
                if (files == null) {
                    return;
                }
                
                for (File file : files) {
                    if (!file.isFile()) {
                        continue;
                    }
                    // 跳过 失败转移开关文件
                    if (file.getName().equals(UtilAndComs.FAILOVER_SWITCH)) {
                        continue;
                    }
                    
                    ServiceInfo dom = null;
                    // 加载文件信息
                    try {
                        dom = new ServiceInfo(URLDecoder.decode(file.getName(), StandardCharsets.UTF_8.name()));
                        String dataString = ConcurrentDiskUtil.getFileContent(file,
                                Charset.defaultCharset().toString());
                        reader = new BufferedReader(new StringReader(dataString));
                        
                        String json;
                        if ((json = reader.readLine()) != null) {
                            try {
                                dom = JacksonUtils.toObj(json, ServiceInfo.class);
                            } catch (Exception e) {
                                NAMING_LOGGER.error("[NA] error while parsing cached dom : {}", json, e);
                            }
                        }
                        
                    } catch (Exception e) {
                        NAMING_LOGGER.error("[NA] failed to read cache for dom: {}", file.getName(), e);
                    } finally {
                        try {
                            if (reader != null) {
                                reader.close();
                            }
                        } catch (Exception e) {
                            //ignore
                        }
                    }
                    if (dom != null && !CollectionUtils.isEmpty(dom.getHosts())) {
                        domMap.put(dom.getKey(), dom);
                    }
                }
            } catch (Exception e) {
                NAMING_LOGGER.error("[NA] failed to read cache file", e);
            }
            
            if (domMap.size() > 0) {
                serviceMap = domMap;
            }
        }
    }
    
    class DiskFileWriter extends TimerTask {
        
        @Override
        public void run() {
            Map<String, ServiceInfo> map = serviceInfoHolder.getServiceInfoMap();
            for (Map.Entry<String, ServiceInfo> entry : map.entrySet()) {
                ServiceInfo serviceInfo = entry.getValue();
                // 以下服务名 不持久化
                if (StringUtils.equals(serviceInfo.getKey(), UtilAndComs.ALL_IPS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_LIST_KEY) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ENV_CONFIGS) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.VIP_CLIENT_FILE) || StringUtils
                        .equals(serviceInfo.getName(), UtilAndComs.ALL_HOSTS)) {
                    continue;
                }
                
                DiskCache.write(serviceInfo, failoverDir);
            }
        }
    }
    
    public boolean isFailoverSwitch() {
        return Boolean.parseBoolean(switchParams.get(FAILOVER_MODE_PARAM));
    }
    
    public ServiceInfo getService(String key) {
        ServiceInfo serviceInfo = serviceMap.get(key);
        
        if (serviceInfo == null) {
            serviceInfo = new ServiceInfo();
            serviceInfo.setName(key);
        }
        
        return serviceInfo;
    }
}
