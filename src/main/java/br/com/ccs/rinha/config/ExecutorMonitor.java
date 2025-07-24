package br.com.ccs.rinha.config;

import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

@Component
public class ExecutorMonitor {

    private static final Logger log = LoggerFactory.getLogger(ExecutorMonitor.class);

    private final ThreadPoolExecutor executor;
    private Thread thread;

    public ExecutorMonitor(ThreadPoolExecutor executor) {
        this.executor = executor;
    }

    @PostConstruct
    public void startMonitoring() {
        var activeMonitor = Boolean.parseBoolean(System.getenv("ACTIVE_MONITOR"));

        stop();

        thread = Thread.ofVirtual()
                .inheritInheritableThreadLocals(false)
                .name("Executor-Monitor")
                .unstarted(() -> {
            long lastCompleted = executor.getCompletedTaskCount();

            while (!Thread.currentThread().isInterrupted()) {
                try {
                    int active = executor.getActiveCount();
                    int poolSize = executor.getPoolSize();
                    int queueSize = executor.getQueue().size();
                    int remainingQueue = executor.getQueue().remainingCapacity();
                    long completed = executor.getCompletedTaskCount();
                    long throughput = completed - lastCompleted;
                    lastCompleted = completed;

                    log.info("active: {}, pool: {}, queue: {}, remaining: {}, completed: {}, throughput: {} P/s",
                            active, poolSize, queueSize, remainingQueue, completed, throughput);

                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    break;
                }
            }
        });

        if (activeMonitor) {
            thread.start();
        } else {
            log.warn("Executor Monitor inactive.");
        }
    }

    private void stop() {
        if (Objects.nonNull(thread) && thread.isAlive()) {
            thread.interrupt();
            thread = null;
        }
    }
}