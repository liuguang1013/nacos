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

package com.alibaba.nacos.sys.file;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.utils.ConcurrentHashSet;
import com.alibaba.nacos.common.utils.ThreadUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.ClosedWatchServiceException;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardWatchEventKinds;
import java.nio.file.WatchEvent;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unified file change monitoring management center, which uses {@link WatchService} internally. One file directory
 * corresponds to one {@link WatchService}. It can only monitor up to 32 file directories. When a file change occurs, a
 * {@link FileChangeEvent} will be issued
 *
 * 统一的文件变更监控管理中心，内部使用{@link WatchService}。
 * 一个文件目录对应一个{@link WatchService}。
 * 它最多只能监视32个文件目录。
 * 当文件发生更改时，将发出{@link FileChangeEvent}
 *
 * 文件检测中心，监测的文件：
 *  cluster.conf    服务端成员集群配置文件
 *  /application.properties     自定义/默认的配置文件属性
 *
 * @author <a href="mailto:liaochuntao@live.com">liaochuntao</a>
 */
public class WatchFileCenter {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(WatchFileCenter.class);
    
    /**
     * Maximum number of monitored file directories.
     */
    private static final int MAX_WATCH_FILE_JOB = Integer.getInteger("nacos.watch-file.max-dirs", 16);
    
    private static final Map<String, WatchDirJob> MANAGER = new HashMap<>(MAX_WATCH_FILE_JOB);
    
    private static final FileSystem FILE_SYSTEM = FileSystems.getDefault();
    
    private static final AtomicBoolean CLOSED = new AtomicBoolean(false);
    
    static {
        ThreadUtils.addShutdownHook(WatchFileCenter::shutdown);
    }
    
    /**
     * The number of directories that are currently monitored.
     */
    @SuppressWarnings("checkstyle:StaticVariableName")
    private static int NOW_WATCH_JOB_CNT = 0;
    
    /**
     * Register {@link FileWatcher} in this directory.
     * 对某路径下文件，进行注册 FileWatcher
     *
     * @param paths   directory
     * @param watcher {@link FileWatcher}
     * @return register is success
     * @throws NacosException NacosException
     */
    public static synchronized boolean registerWatcher(final String paths, FileWatcher watcher) throws NacosException {
        // 判断文件监控中心当前状态不处在关闭
        checkState();
        // 达到最大监控数量，直接返回
        if (NOW_WATCH_JOB_CNT == MAX_WATCH_FILE_JOB) {
            return false;
        }

        WatchDirJob job = MANAGER.get(paths);
        if (job == null) {
            job = new WatchDirJob(paths);
            job.start();
            MANAGER.put(paths, job);
            NOW_WATCH_JOB_CNT++;
        }
        // 添加订阅者
        job.addSubscribe(watcher);
        return true;
    }
    
    /**
     * Deregister all {@link FileWatcher} in this directory.
     *
     * @param path directory
     * @return deregister is success
     */
    public static synchronized boolean deregisterAllWatcher(final String path) {
        WatchDirJob job = MANAGER.get(path);
        if (job != null) {
            job.shutdown();
            MANAGER.remove(path);
            NOW_WATCH_JOB_CNT--;
            return true;
        }
        return false;
    }
    
    /**
     * close {@link WatchFileCenter}.
     */
    public static void shutdown() {
        if (!CLOSED.compareAndSet(false, true)) {
            return;
        }
        LOGGER.warn("[WatchFileCenter] start close");
        for (Map.Entry<String, WatchDirJob> entry : MANAGER.entrySet()) {
            LOGGER.warn("[WatchFileCenter] start to shutdown this watcher which is watch : " + entry.getKey());
            try {
                entry.getValue().shutdown();
            } catch (Throwable e) {
                LOGGER.error("[WatchFileCenter] shutdown has error : ", e);
            }
        }
        MANAGER.clear();
        NOW_WATCH_JOB_CNT = 0;
        LOGGER.warn("[WatchFileCenter] already closed");
    }
    
    /**
     * Deregister {@link FileWatcher} in this directory.
     *
     * @param path    directory
     * @param watcher {@link FileWatcher}
     * @return deregister is success
     */
    public static synchronized boolean deregisterWatcher(final String path, final FileWatcher watcher) {
        WatchDirJob job = MANAGER.get(path);
        if (job != null) {
            job.watchers.remove(watcher);
            return true;
        }
        return false;
    }
    
    private static class WatchDirJob extends Thread {
        
        private final ExecutorService callBackExecutor;
        
        private final String paths;
        
        private final WatchService watchService;
        
        private volatile boolean watch = true;
        
        private final Set<FileWatcher> watchers = new ConcurrentHashSet<>();
        
        public WatchDirJob(String paths) throws NacosException {
            setName(paths);
            this.paths = paths;
            final Path p = Paths.get(paths);
            // 判断必须是 文件目录
            if (!p.toFile().isDirectory()) {
                throw new IllegalArgumentException("Must be a file directory : " + paths);
            }
            // 创建回调线程池
            this.callBackExecutor = ExecutorFactory.newSingleExecutorService(
                    new NameThreadFactory("com.alibaba.nacos.sys.file.watch-" + paths));
            
            try {
                // 使用 jdk 自带类创建 监控对象
                WatchService service = FILE_SYSTEM.newWatchService();
                // 注册监控服务监控的事件类型
                p.register(service, StandardWatchEventKinds.OVERFLOW, StandardWatchEventKinds.ENTRY_MODIFY,
                        StandardWatchEventKinds.ENTRY_CREATE, StandardWatchEventKinds.ENTRY_DELETE);
                this.watchService = service;
            } catch (Throwable ex) {
                throw new NacosException(NacosException.SERVER_ERROR, ex);
            }
        }
        
        void addSubscribe(final FileWatcher watcher) {
            watchers.add(watcher);
        }
        
        void shutdown() {
            watch = false;
            
            //fix issue[https://github.com/alibaba/nacos/issues/9393]
            try {
                watchService.close();
            } catch (IOException ignore) {
            }
            
            ThreadUtils.shutdownThreadPool(this.callBackExecutor);
        }
        
        @Override
        public void run() {
            // 无限循环，监控事件
            while (watch && !this.isInterrupted()) {
                try {
                    // 在 监控服务中获取事件
                    final WatchKey watchKey = watchService.take();
                    final List<WatchEvent<?>> events = watchKey.pollEvents();
                    watchKey.reset();
                    if (callBackExecutor.isShutdown()) {
                        return;
                    }
                    if (events.isEmpty()) {
                        continue;
                    }
                    //
                    callBackExecutor.execute(() -> {
                        for (WatchEvent<?> event : events) {
                            WatchEvent.Kind<?> kind = event.kind();
                            
                            // Since the OS's event cache may be overflow, a backstop is needed
                            //由于操作系统的事件缓存可能会溢出，因此需要备份
                            if (StandardWatchEventKinds.OVERFLOW.equals(kind)) {
                                eventOverflow();
                            } else {
                                eventProcess(event.context());
                            }
                        }
                    });
                } catch (InterruptedException | ClosedWatchServiceException ignore) {
                    Thread.interrupted();
                } catch (Throwable ex) {
                    LOGGER.error("An exception occurred during file listening : ", ex);
                }
            }
        }
        
        private void eventProcess(Object context) {
            // 创建 文件改变事件
            final FileChangeEvent fileChangeEvent = FileChangeEvent.builder().paths(paths).context(context).build();
            final String str = String.valueOf(context);
            for (final FileWatcher watcher : watchers) {
                if (watcher.interest(str)) {
                    Runnable job = () -> watcher.onChange(fileChangeEvent);
                    Executor executor = watcher.executor();
                    if (executor == null) {
                        try {
                            job.run();
                        } catch (Throwable ex) {
                            LOGGER.error("File change event callback error : ", ex);
                        }
                    } else {
                        executor.execute(job);
                    }
                }
            }
        }
        
        private void eventOverflow() {
            File dir = Paths.get(paths).toFile();
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                // Subdirectories do not participate in listening
                if (file.isDirectory()) {
                    continue;
                }
                eventProcess(file.getName());
            }
        }
        
    }
    
    private static void checkState() {
        if (CLOSED.get()) {
            throw new IllegalStateException("WatchFileCenter already shutdown");
        }
    }
}
