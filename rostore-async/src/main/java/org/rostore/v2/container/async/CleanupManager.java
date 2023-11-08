package org.rostore.v2.container.async;

import java.util.concurrent.ExecutorService;

public abstract class CleanupManager {

    private long lastRunTimestamp;
    private final long intervalMillis;
    private final ExecutorService executorService;
    private boolean queued;

    public CleanupManager(final ExecutorService executorService, final long cleanupIntervalMillis) {
        lastRunTimestamp = 0;
        this.executorService = executorService;
        this.intervalMillis = cleanupIntervalMillis;
        queued = false;
    }

    public void scheduleCleanup() {
        if (queued) {
            // there is a cleanup planed
            return ;
        }
        boolean timeHasCome = System.currentTimeMillis() - lastRunTimestamp > intervalMillis;
        if (timeHasCome) {
            synchronized (this) {
                if (!queued) {
                    queued = true;
                    executorService.submit(() -> {
                        try {
                            execute();
                        } finally {
                            queued = false;
                            lastRunTimestamp = System.currentTimeMillis();
                            finalized();
                        }
                    });
                }
            }
        }
    }

    protected abstract void execute();

    protected abstract void finalized();

    public boolean isQueued() {
        return queued;
    }

}
