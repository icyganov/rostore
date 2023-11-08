package org.rostore.v2.container.async;

public enum AsyncStatus {
    OPENED(false),
    STARTED(false),
    /**
     * Canceled manually
     */
    CANCELED(true),
    /**
     * Error happened
     */
    ERROR(true),
    /**
     * Finished successfully
     */
    SUCCESS(true);

    public boolean isFinished() {
        return finished;
    }

    private final boolean finished;

    AsyncStatus(final boolean finished) {
        this.finished = finished;
    }
}

