package org.rostore.v2.container.async;

import org.rostore.Utils;
import org.rostore.v2.container.ContainerShardKeyOperations;

/**
 * This is a manager to clean up if needed
 */
public class ContainerShardCleanupManager extends CleanupManager {

    private int nextRunBlockIndex;
    private final int maxNumberOfCleans;
    private ContainerShardExecutor containerShardExecutor;

    public ContainerShardCleanupManager(final ContainerShardExecutor containerShardExecutor) {
        super(containerShardExecutor.getAsyncContainers().getExecutorService(), containerShardExecutor.getAsyncContainers().getContainerListHeader().getContainerListProperties().getCleanupIntervalMillis());
        nextRunBlockIndex = 0;
        this.maxNumberOfCleans = containerShardExecutor.getAsyncContainers().getContainerListHeader().getContainerListProperties().getMaxCleanupsPerCycle();
        this.containerShardExecutor = containerShardExecutor;
    }

    protected void execute() {
        containerShardExecutor.executeKey(0, OperationType.WRITE, true, this::queue);
    }

    protected void finalized() {
        containerShardExecutor.shutdownIfHasTo();
    }

    public boolean queue(final ContainerShardKeyOperations ops) {
            if (nextRunBlockIndex >= ops.getBlockSequence().length()) {
                nextRunBlockIndex = 0;
            }
            boolean ret = false;
            int cleanNumber = 0;
            do {
                long id = ops.removeKeyIfExpired(nextRunBlockIndex);
                if (id == Utils.ID_UNDEFINED) {
                    break;
                }
                containerShardExecutor.executeAutonomousValue(0,
                        OperationType.DELETE,
                        id,
                        false,
                        () -> containerShardExecutor.getShard().removeValue(id));
                cleanNumber++;
                if (cleanNumber > maxNumberOfCleans) {
                    ret = true;
                    break;
                }
            } while (true);
            if (!ret) {
                nextRunBlockIndex++;
            }
            return ret;
    }
}
