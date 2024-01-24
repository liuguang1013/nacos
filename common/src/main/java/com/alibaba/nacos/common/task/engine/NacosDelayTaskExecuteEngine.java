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

package com.alibaba.nacos.common.task.engine;

import com.alibaba.nacos.api.exception.NacosException;
import com.alibaba.nacos.common.executor.ExecutorFactory;
import com.alibaba.nacos.common.executor.NameThreadFactory;
import com.alibaba.nacos.common.task.AbstractDelayTask;
import com.alibaba.nacos.common.task.NacosTaskProcessor;
import org.slf4j.Logger;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Nacos delay task execute engine.
 *
 * @author xiweng.yy
 */
public class NacosDelayTaskExecuteEngine extends AbstractNacosTaskExecuteEngine<AbstractDelayTask> {
    
    private final ScheduledExecutorService processingExecutor;

    /**
     * key： key 的作用主要是用来获取 任务处理器
     *      PushDelayTaskExecuteEngine 放入 service 对象、
     *      DistroDelayTaskExecuteEngine 放入 DistroKey 对象
     * value： 延迟任务
     */
    protected final ConcurrentHashMap<Object, AbstractDelayTask> tasks;
    
    protected final ReentrantLock lock = new ReentrantLock();
    
    public NacosDelayTaskExecuteEngine(String name) {
        this(name, null);
    }
    
    public NacosDelayTaskExecuteEngine(String name, Logger logger) {
        this(name, 32, logger, 100L);
    }
    
    public NacosDelayTaskExecuteEngine(String name, Logger logger, long processInterval) {
        this(name, 32, logger, processInterval);
    }
    
    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger) {
        this(name, initCapacity, logger, 100L);
    }
    
    public NacosDelayTaskExecuteEngine(String name, int initCapacity, Logger logger, long processInterval) {
        super(logger);
        tasks = new ConcurrentHashMap<>(initCapacity);
        // 创建单线程的线程池
        processingExecutor = ExecutorFactory.newSingleScheduledExecutorService(new NameThreadFactory(name));
        // 延迟 0.1s 后启动，之后每 0.1s 执行一次
        processingExecutor
                .scheduleWithFixedDelay(new ProcessRunnable(), processInterval, processInterval, TimeUnit.MILLISECONDS);
    }
    
    @Override
    public int size() {
        lock.lock();
        try {
            return tasks.size();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public boolean isEmpty() {
        lock.lock();
        try {
            return tasks.isEmpty();
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public AbstractDelayTask removeTask(Object key) {
        lock.lock();
        try {
            AbstractDelayTask task = tasks.get(key);
            if (null != task && task.shouldProcess()) {
                return tasks.remove(key);
            } else {
                return null;
            }
        } finally {
            lock.unlock();
        }
    }
    
    @Override
    public Collection<Object> getAllTaskKeys() {
        Collection<Object> keys = new HashSet<>();
        lock.lock();
        try {
            keys.addAll(tasks.keySet());
        } finally {
            lock.unlock();
        }
        return keys;
    }
    
    @Override
    public void shutdown() throws NacosException {
        tasks.clear();
        processingExecutor.shutdown();
    }
    
    @Override
    public void addTask(Object key, AbstractDelayTask newTask) {
        lock.lock();
        try {
            // 缓存中获取 该 service 服务的任务
            AbstractDelayTask existTask = tasks.get(key);
            if (null != existTask) {
                // 存在就合并任务，主要是对推送的客户端进行合并
                newTask.merge(existTask);
            }
            tasks.put(key, newTask);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * process tasks in execute engine.
     * 在执行引擎中处理任务。
     */
    protected void processTasks() {
        // 获取 任务缓存 map 中的 key set
        Collection<Object> keys = getAllTaskKeys();
        for (Object taskKey : keys) {
            // 移除任务，里面会判断，任务之间的间隔是否大于 1s
            AbstractDelayTask task = removeTask(taskKey);
            if (null == task) {
                continue;
            }
            // 获取 任务处理器
            NacosTaskProcessor processor = getProcessor(taskKey);
            if (null == processor) {
                getEngineLog().error("processor not found for task, so discarded. " + task);
                continue;
            }
            try {
                // ReAdd task if process failed
                // 开始处理任务，如果处理失败：不断的重试，添加到队列中
                if (!processor.process(task)) {
                    retryFailedTask(taskKey, task);
                }
            } catch (Throwable e) {
                getEngineLog().error("Nacos task execute error ", e);
                retryFailedTask(taskKey, task);
            }
        }
    }
    
    private void retryFailedTask(Object key, AbstractDelayTask task) {
        task.setLastProcessTime(System.currentTimeMillis());
        addTask(key, task);
    }
    
    private class ProcessRunnable implements Runnable {
        
        @Override
        public void run() {
            try {
                // 处理任务
                processTasks();
            } catch (Throwable e) {
                getEngineLog().error(e.toString(), e);
            }
        }
    }
}
