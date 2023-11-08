package org.rostore.v2.container.async;

import org.rostore.entity.RoStoreException;
import org.rostore.v2.container.ContainerShard;
import org.rostore.v2.container.ContainerShardKeyOperations;

import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;
import java.util.logging.Logger;

public class ContainerShardExecutor {

    private static final Logger logger = Logger.getLogger(ContainerShardExecutor.class.getName());
    private final AsyncContainer asyncContainer;
    private final ContainerShardCleanupManager cleanupManager;
    private final ContainerShard shard;
    private KeyExecutionState keyExecutionState;
    private int keyReadCount;
    private int valueWriteCount;
    private boolean shutdown;
    private CountDownLatch shutdownLatch;
    private final Queue<Operation> keyOperations = new LinkedList<>();

    /**
     * These are executed as soon as all currently executing read value operations for this valueId are done
     */
    private final Map<Long, Operation> waitingDeleteValueOperations = new HashMap<>();

    /**
     * Registers the running delete operations
     */
    private final Set<Long> runningDeleteValueOperations = new HashSet<>();

    private final Map<Long, Integer> readValueOperations = new HashMap<>();

    public ContainerShard getShard() {
        return shard;
    }

    public AsyncContainers getAsyncContainers() {
        return asyncContainer.getAsyncContainers();
    }

    protected ContainerShardExecutor(final AsyncContainer asyncContainer, final ContainerShard shard) {
        this.asyncContainer = asyncContainer;
        this.shard = shard;
        keyExecutionState = KeyExecutionState.IDLE;
        shutdown = false;
        shutdownLatch = new CountDownLatch(1);
        keyReadCount = 0;
        valueWriteCount = 0;
        cleanupManager = new ContainerShardCleanupManager(this);
    }

    public synchronized void shutdown() {
        shutdown = true;
        shutdownIfHasTo();
    }

    public synchronized boolean isIdle() {
        return keyExecutionState == KeyExecutionState.IDLE &&
                keyOperations.isEmpty() &&
                waitingDeleteValueOperations.isEmpty() &&
                runningDeleteValueOperations.isEmpty() &&
                readValueOperations.isEmpty() &&
                valueWriteCount == 0 &&
                !cleanupManager.isQueued();
    }

    public <R> Future<R> executeKey(final int sessionId, final OperationType opsType, final boolean failInShutdown, final Function<ContainerShardKeyOperations, R> opsConsumer) {
        return execute(Operation.key(sessionId,
                opsType,
                () -> shard.keyFunction(opsConsumer)), failInShutdown);
    }

    public <R> Future<R> executeAutonomousValue(final int sessionId,
                                                             final OperationType opsType,
                                                             final long valueId,
                                                             final boolean failInShutdown,
                                                             final Runnable valueOp) {
        return execute(Operation.autonomousValue(sessionId,
                opsType,
                valueId,
                valueOp), failInShutdown);
    }

    public <R> Future<R> executeValue(final int sessionId,
                                                             final OperationType opsType,
                                                             final long valueId,
                                                             final boolean failInShutdown,
                                                             final Supplier<R> supplier) {
        return execute(Operation.value(sessionId,
                opsType,
                valueId,
                supplier),
                failInShutdown);
    }

    public synchronized <R> Future<R> execute(final Operation<R> operation, final boolean failInShutdown) {
        if (failInShutdown && shutdown) {
            return InterruptedShardOperation.INTERRUPTED_SHARD_OPERATION;
        }
        if (operation.getTarget() == OperationTarget.KEY) {
             keyOperations.offer(operation);
             processKeyOperations();
             return operation;
        }
        // VALUE
        switch(operation.getType()) {
            case READ:
                if (runningDeleteValueOperations.contains(operation.getValueId()) || waitingDeleteValueOperations.containsKey(operation.getValueId())) {
                    // try to read a value that is going to be removed soon
                    operation.cancel(null);
                    return operation;
                }
                // this one will not be deleted
                submit(operation);
                return operation;
            case WRITE:
                submit(operation);
                return operation;
            case DELETE:
                if (waitingDeleteValueOperations.containsKey(operation.getValueId()) || runningDeleteValueOperations.contains(operation.getValueId())) {
                    throw new RoStoreException("Secondary delete operation!");
                }
                if (readValueOperations.containsKey(operation.getValueId())) {
                    // there are read operations
                    waitingDeleteValueOperations.put(operation.getValueId(), operation);
                    return operation;
                } else {
                    submit(operation);
                    return operation;
                }
        }
        throw new RoStoreException("Unknown operation mode");
    }

    private boolean processKeyOperations() {
        if (keyOperations.isEmpty()) {
            return false;
        }
        switch (keyExecutionState) {
            case IDLE:
                submit(keyOperations.poll());
                return true;
            case EXCLUSIVE:
                return false;
            case MULTIPLE:
                if (keyOperations.peek().getType() == OperationType.READ) {
                    submit(keyOperations.poll());
                    return true;
                }
                return false;
        }
        return false;
    }

    private void processAllKeyOperations() {
        while(processKeyOperations()) {};
    }

    private synchronized void done(final Operation operation) {
        // System.out.println("Done " + getShard().getIndex() + ": " + operation + ", totalRunning: " + runningTasks + ", queuedKeys: " + keyOperations.size());
        operation.done();
        if (operation.isAutonomous() && operation.getException()!=null) {
            logger.log(Level.WARNING, "Exception has been detected in the autonomous operation " + operation, operation.getException());
        }
        if (OperationTarget.KEY.equals(operation.getTarget())) {
            // KEY
            switch (operation.getType()) {
                case READ:
                    keyReadCount--;
                    if (keyReadCount == 0) {
                        keyExecutionState = KeyExecutionState.IDLE;
                    }
                    break;
                case WRITE:
                case DELETE:
                    keyExecutionState = KeyExecutionState.IDLE;
                    break;
            }
            processAllKeyOperations();
        } else {
            // VALUE
            switch (operation.getType()) {
                case READ:
                    if (decrementReadValueCounter(operation)) {
                        // the counter has been removed => no currently pending read ops
                        final Operation pendingDeleteOperation = waitingDeleteValueOperations.remove(operation.getValueId());
                        // it was a pending delete operation
                        if (pendingDeleteOperation != null) {
                            submit(pendingDeleteOperation);
                        }
                    }
                    break;
                case DELETE:
                    runningDeleteValueOperations.remove(operation.getValueId());
                    break;
                case WRITE:
                    valueWriteCount--;
                    break;
            }
        }
        shutdownIfHasTo();
    }

    public void shutdownIfHasTo() {
        if (isIdle()) {
            if (shutdown && shutdownLatch.getCount() != 0) {
                shutdownLatch.countDown();
            }
            asyncContainer.notifyIdle(this);
        }
        this.notify();
    }

    public void shutdownAndWait() {
        shutdown();
        waitForShutdown();
    }

    public void waitForShutdown() {
        if (!shutdown) {
            throw new RoStoreException("Trying to wait for shutdown on active shard.");
        }
        try {
            shutdownLatch.await();
        } catch (final InterruptedException e) {
            throw new RoStoreException("Interrupted while waiting for shutdown of container shard", e);
        }
    }

    private void submit(final Operation operation) {
        //System.out.println("Submit " + getShard().getIndex() + ": " + operation + ", totalRunning: " + runningTasks + ", queuedKeys: " + keyOperations.size());
        if (OperationTarget.KEY.equals(operation.getTarget())) {
            switch (operation.getType()) {
                case WRITE:
                case DELETE:
                    keyExecutionState = KeyExecutionState.EXCLUSIVE;
                    break;
                case READ:
                    keyExecutionState = KeyExecutionState.MULTIPLE;
                    keyReadCount++;
                    break;
            }
        } else {
            // VALUE
            switch (operation.getType()) {
                case DELETE:
                    runningDeleteValueOperations.add(operation.getValueId());
                    break;
                case READ:
                    incrementReadValueCounter(operation);
                    break;
                case WRITE:
                    valueWriteCount++;
                    break;
            }
        }
        asyncContainer.getAsyncContainers().getExecutorService().submit(() -> {
            try {
                operation.execute();
                if (!shutdown) {
                    cleanupManager.scheduleCleanup();
                    asyncContainer.getAsyncContainers().getCleanupManager().scheduleCleanup();
                }
            } catch (final Exception e) {
                operation.setException(e);
            }
            finally {
                done(operation);
            }
        });
    }

    private void incrementReadValueCounter(Operation operation) {
        Integer counter = readValueOperations.get(operation.getValueId());
        if (counter == null) {
            counter = 1;
        } else {
            counter++;
        }
        readValueOperations.put(operation.getValueId(), counter);
    }

    private boolean decrementReadValueCounter(final Operation operation) {
        Integer counter = readValueOperations.get(operation.getValueId());
        if (counter == 1) {
            readValueOperations.remove(operation.getValueId());
            return true;
        } else {
            counter--;
        }
        readValueOperations.put(operation.getValueId(), counter);
        return false;
    }
}
