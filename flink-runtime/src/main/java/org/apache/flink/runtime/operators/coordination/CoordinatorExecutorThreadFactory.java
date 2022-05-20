package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.runtime.source.coordinator.SourceCoordinatorProvider;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.util.concurrent.ThreadFactory;

public class CoordinatorExecutorThreadFactory implements ThreadFactory, Thread.UncaughtExceptionHandler {

    private static final Logger LOG = LoggerFactory.getLogger(SourceCoordinatorProvider.class);
    private final String coordinatorThreadName;
    private final ClassLoader cl;
    private final Thread.UncaughtExceptionHandler errorHandler;

    @Nullable
    private Thread t;

    public CoordinatorExecutorThreadFactory(
            final String coordinatorThreadName, final OperatorCoordinator.Context context) {
        this(
                coordinatorThreadName,
                context.getUserCodeClassloader(),
                (t, e) -> {
                    LOG.error(
                            "Thread '{}' produced an uncaught exception. Failing the job.",
                            t.getName(),
                            e);
                    context.failJob(e);
                });
    }

    public CoordinatorExecutorThreadFactory(
            final String coordinatorThreadName,
            final ClassLoader contextClassLoader,
            final Thread.UncaughtExceptionHandler errorHandler) {
        this.coordinatorThreadName = coordinatorThreadName;
        this.cl = contextClassLoader;
        this.errorHandler = errorHandler;
    }

    @Override
    public synchronized Thread newThread(Runnable r) {
        t = new Thread(r, coordinatorThreadName);
        t.setContextClassLoader(cl);
        t.setUncaughtExceptionHandler(this);
        return t;
    }

    @Override
    public synchronized void uncaughtException(Thread t, Throwable e) {
        errorHandler.uncaughtException(t, e);
    }

    public String getCoordinatorThreadName() {
        return coordinatorThreadName;
    }

    boolean isCurrentThreadCoordinatorThread() {
        return Thread.currentThread() == t;
    }
}
