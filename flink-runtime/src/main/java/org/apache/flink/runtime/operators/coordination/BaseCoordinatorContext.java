/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.runtime.operators.coordination;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.ThrowableCatchingRunnable;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

import static org.apache.flink.runtime.operators.coordination.ComponentClosingUtils.shutdownExecutorForcefully;

/**
 * A context class for the {@link OperatorCoordinator}. Compared with {@link SplitEnumeratorContext}
 * this class allows interaction with state and sending {@link OperatorEvent} to the SourceOperator
 * while {@link SplitEnumeratorContext} only allows sending {@link SourceEvent}.
 *
 * <p>The context serves a few purposes:
 *
 * <ul>
 *   <li>Information provider - The context provides necessary information to the enumerator for it
 *       to know what is the status of the source readers and their split assignments. These
 *       information allows the split enumerator to do the coordination.
 *   <li>Action taker - The context also provides a few actions that the enumerator can take to
 *       carry out the coordination. So far there are two actions: 1) assign splits to the source
 *       readers. and 2) sens a custom {@link SourceEvent SourceEvents} to the source readers.
 *   <li>Thread model enforcement - The context ensures that all the manipulations to the
 *       coordinator state are handled by the same thread.
 * </ul>
 *
 */
@Internal
public abstract class BaseCoordinatorContext
        implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(BaseCoordinatorContext.class);

    //protected final ScheduledExecutorService workerExecutor;
    protected final ScheduledExecutorService coordinatorExecutor;
    protected final OperatorCoordinator.Context operatorCoordinatorContext;
    protected final CoordinatorExecutorThreadFactory
            coordinatorThreadFactory;
    protected final OperatorCoordinator.SubtaskGateway[] subtaskGateways;
    protected final String coordinatorThreadName;
    protected volatile boolean closed;

    public BaseCoordinatorContext(
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            int numWorkerThreads,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this(
                Executors.newScheduledThreadPool(1, coordinatorThreadFactory),
                coordinatorThreadFactory,
                operatorCoordinatorContext);
    }

    // Package private method for unit test.

    public BaseCoordinatorContext(
            ScheduledExecutorService coordinatorExecutor,
            CoordinatorExecutorThreadFactory coordinatorThreadFactory,
            OperatorCoordinator.Context operatorCoordinatorContext) {
        this.coordinatorExecutor = coordinatorExecutor;
        this.coordinatorThreadFactory = coordinatorThreadFactory;
        this.operatorCoordinatorContext = operatorCoordinatorContext;
        this.coordinatorThreadName = coordinatorThreadFactory.getCoordinatorThreadName();
        this.subtaskGateways =
                new OperatorCoordinator.SubtaskGateway
                        [operatorCoordinatorContext.currentParallelism()];
    }

    public void sendEventToOperator(int subtaskId, OperatorEvent event) {
        checkSubtaskIndex(subtaskId);

        callInCoordinatorThread(
                () -> {
                    final OperatorCoordinator.SubtaskGateway gateway =
                            getGatewayAndCheckReady(subtaskId);
                    gateway.sendEvent(event);
                    return null;
                },
                String.format("Failed to send event %s to subtask %d", event, subtaskId));
    }

    public ScheduledExecutorService getCoordinatorExecutor() {
        return coordinatorExecutor;
    }

    @Override
    public void close() throws InterruptedException {
        closed = true;
        // Close quietly so the closing sequence will be executed completely.
        //shutdownExecutorForcefully(workerExecutor, Duration.ofNanos(Long.MAX_VALUE));
        shutdownExecutorForcefully(coordinatorExecutor, Duration.ofNanos(Long.MAX_VALUE));
    }

    // --------- Package private additional methods for the SourceCoordinator ------------

    public void subtaskReady(OperatorCoordinator.SubtaskGateway gateway) {
        final int subtask = gateway.getSubtask();
        if (subtaskGateways[subtask] == null) {
            subtaskGateways[gateway.getSubtask()] = gateway;
        } else {
            throw new IllegalStateException("Already have a subtask gateway for " + subtask);
        }
    }

    public void subtaskNotReady(int subtaskIndex) {
        subtaskGateways[subtaskIndex] = null;
    }

    protected OperatorCoordinator.SubtaskGateway getGatewayAndCheckReady(int subtaskIndex) {
        final OperatorCoordinator.SubtaskGateway gateway = subtaskGateways[subtaskIndex];
        if (gateway != null) {
            return gateway;
        }

        throw new IllegalStateException(
                String.format("Subtask %d is not ready yet to receive events.", subtaskIndex));
    }

    /**
     * Fail the job with the given cause.
     *
     * @param cause the cause of the job failure.
     */
    public void failJob(Throwable cause) {
        operatorCoordinatorContext.failJob(cause);
    }

    protected void handleUncaughtExceptionFromAsyncCall(Throwable t) {
        if (closed) {
            return;
        }

        ExceptionUtils.rethrowIfFatalErrorOrOOM(t);
        LOG.error(
                "Exception while handling result from async call in {}. Triggering job failover.",
                coordinatorThreadName,
                t);
        failJob(t);
    }

//    /**
//     * Behavior of SourceCoordinatorContext on checkpoint.
//     *
//     * @param checkpointId The id of the ongoing checkpoint.
//     */
//    protected abstract void onCheckpoint(long checkpointId) throws Exception;
//
//
//    /**
//     * Invoked when a successful checkpoint has been taken.
//     *
//     * @param checkpointId the id of the successful checkpoint.
//     */
//    protected abstract void onCheckpointComplete(long checkpointId);


    public OperatorCoordinator.Context getCoordinatorContext() {
        return operatorCoordinatorContext;
    }

    // ---------------- private helper methods -----------------

    protected void checkSubtaskIndex(int subtaskIndex) {
        if (subtaskIndex < 0 || subtaskIndex >= getCoordinatorContext().currentParallelism()) {
            throw new IllegalArgumentException(
                    String.format(
                            "Subtask index %d is out of bounds [0, %s)",
                            subtaskIndex, getCoordinatorContext().currentParallelism()));
        }
    }


    /** {@inheritDoc} If the runnable throws an Exception, the corresponding job is failed. */
    public void runInCoordinatorThread(Runnable runnable) {
        // when using a ScheduledThreadPool, uncaught exception handler catches only
        // exceptions thrown by the threadPool, so manually call it when the exception is
        // thrown by the runnable
        coordinatorExecutor.execute(
                new ThrowableCatchingRunnable(
                        throwable ->
                                coordinatorThreadFactory.uncaughtException(
                                        Thread.currentThread(), throwable),
                        runnable));
    }

    /**
     * A helper method that delegates the callable to the coordinator thread if the current thread
     * is not the coordinator thread, otherwise call the callable right away.
     *
     * @param callable the callable to delegate.
     */
    protected <V> V callInCoordinatorThread(Callable<V> callable, String errorMessage) {
        // Ensure the split assignment is done by the coordinator executor.
        if (!coordinatorThreadFactory.isCurrentThreadCoordinatorThread()) {
            try {
                final Callable<V> guardedCallable =
                        () -> {
                            try {
                                return callable.call();
                            } catch (Throwable t) {
                                LOG.error("Uncaught Exception in Coordinator Executor", t);
                                ExceptionUtils.rethrowException(t);
                                return null;
                            }
                        };

                return coordinatorExecutor.submit(guardedCallable).get();
            } catch (InterruptedException | ExecutionException e) {
                throw new FlinkRuntimeException(errorMessage, e);
            }
        }

        try {
            return callable.call();
        } catch (Throwable t) {
            LOG.error("Uncaught Exception in Coordinator Executor", t);
            throw new FlinkRuntimeException(errorMessage, t);
        }
    }
}
