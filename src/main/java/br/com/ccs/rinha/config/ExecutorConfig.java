package br.com.ccs.rinha.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

@Configuration(proxyBeanMethods = false)
public class ExecutorConfig {

    private final Logger log = LoggerFactory.getLogger(ExecutorConfig.class);

    @Bean
    public ThreadPoolExecutor executorService() {

        var virtual = Boolean.parseBoolean(System.getenv("VIRTUAL_THREADS"));
        int threadPoolSize = Integer.parseInt(System.getenv("THREAD_POOL_SIZE"));
        int queueSize = Integer.parseInt(System.getenv("QUEUE_SIZE"));
        boolean queueIsFair = Boolean.parseBoolean(System.getenv("QUEUE_IS_FAIR"));

        log.info("Thread pool size: {}", threadPoolSize);
        log.info("Queue size: {}", queueSize);
        log.info("Queue isFair: {}", queueIsFair);
        log.info("Using Virtual Threads: {}", virtual);

        var executor = new ThreadPoolExecutor(
                threadPoolSize,
                threadPoolSize,
                10, TimeUnit.SECONDS,
                new ArrayBlockingQueue<>(queueSize, queueIsFair),
                virtual ? Thread.ofVirtual().factory() : Thread.ofPlatform().factory(),
                new ThreadPoolExecutor.DiscardPolicy());

        executor.prestartAllCoreThreads();
        log.info("Effective thread poll factory: {}", executor.getThreadFactory().getClass().getSimpleName());
        log.info("Executor is all threads started. {}", executor.prestartCoreThread());
        return executor;
    }

}
