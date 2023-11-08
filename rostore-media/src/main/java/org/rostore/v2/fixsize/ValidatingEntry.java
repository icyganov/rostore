package org.rostore.v2.fixsize;

import org.rostore.entity.RoStoreException;

public abstract class ValidatingEntry {

    private int index;

    public abstract int getEntriesNumber();

    public int getIndex() {
        return index;
    }

    public boolean isFirst() {
        throwExceptionIfInvalid("isFirst");
        return index == 0;
    }

    public void invalidate() {
        index = -1;
    }

    public void validate() {
        if (index < 0 || index >= getEntriesNumber() ) {
            invalidate();
        }
    }

    public String toString() {
        return "Entry" + (invalid()?"(invalid)":"") + ": idx " + index;
    }

    public boolean invalid() {
        return index == -1;
    }

    public boolean valid() {
        return index != -1;
    }

    public void moveTo(final int index){
        if (index >= 0 && index < getEntriesNumber()) {
            this.index = index;
        } else {
            invalidate();
        }
    }

    public void last(){
        moveTo(getEntriesNumber()-1);
    }

    public void first(){
        moveTo(0);
    }

    public void previous(){
        moveTo(index-1);
    }

    public void next(){
        moveTo(index+1);
    }

    protected void throwExceptionIfInvalid(final String message) {
        if (invalid()) {
            throw new RoStoreException("The <" + this + "> is invalid: " + message);
        }
    }

}
