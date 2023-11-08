package org.rostore.service;

import org.rostore.entity.RoStoreException;

public enum RoStoreState {
    STARTING(0),
    INITIALIZED(1),
    OPENED(2),
    SHUTTING_DOWN(3);

    private int order;

    public void checkRequestsAllowed() {
        boolean allowed = this.order >= INITIALIZED.order && this.order <= OPENED.order;
        if (!allowed) {
            throw new RoStoreException("Can't process requests in the state=" + this.name());
        }
    }

    public void checkContainerRequestsAllowed() {
        boolean allowed = this == OPENED;
        if (!allowed) {
            throw new RoStoreException("Can't process container requests in the state=" + this.name());
        }
    }

    RoStoreState(int order) {
        this.order = order;
    }
}
