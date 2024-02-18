package org.rostore.v2.fixsize;

import org.rostore.entity.RoStoreException;

/**
 * An entry of the block that validates the index to be valid.
 *
 * <p>It encapsulates the number of entries and index of the entry withing the block as well as simple operation
 * to move this index withing the boundaries of the block (given number of entries within the block).</p>
 *
 * <p>It also supports an invalid status of the entry, but prevents any operation on it, in case the index is not within
 * expected boundaries.</p>
 *
 * <p>This is extracted as a separate entry to protected the {@link FixSizeEntry} from direct manipulating of the index,
 * to have a better protection in case of invalid operations.</p>
 */
public abstract class ValidatingEntry {

    private int index;

    /**
     * Get the number of entries that exist in the block.
     * <p>It is not to mix to capacity, and represents a total number of entries added to this block by now.</p>
     *
     * @return the number of entries
     */
    public abstract int getEntriesNumber();

    /**
     * Get current index of the entry
     * @return the index of currently selected entry
     */
    public int getIndex() {
        return index;
    }

    /**
     * Checks if currently selected entry is the first one in the block
     * <p>It will throw an exception if the entry is invalid.</p>
     * @return {@code true} if currently selected entry is the first one
     */
    public boolean isFirst() {
        throwExceptionIfInvalid("isFirst");
        return index == 0;
    }

    /**
     * Marks this entry as invalid
     */
    public void invalidate() {
        index = -1;
    }

    /**
     * Checks if the current index is valid (the index should be strictly below the {@link #getEntriesNumber()}
     * and if it is not correct, marks this entry as invalid.
     */
    public void validate() {
        if (index < 0 || index >= getEntriesNumber() ) {
            invalidate();
        }
    }

    public String toString() {
        return "Entry" + (invalid()?"(invalid)":"") + ": idx " + index;
    }

    /**
     * Checks if current entry has been marked as invalid.
     * <p>See {@link #validate()}</p>
     * @return {@code true} if entry is invalid
     */
    public boolean invalid() {
        return index == -1;
    }

    /**
     * Checks if current entry has been marked as valid.
     * <p>See {@link #validate()}</p>
     * @return {@code true} if entry is valid
     */
    public boolean valid() {
        return index != -1;
    }

    /**
     * Sets the current index to the value as provided.
     * <p>The function would mark the entry as invalid if the index
     * does not lay withing the boundaries: 0..{@link #getEntriesNumber()}</p>
     *
     * @param index the index to be set
     */
    public void moveTo(final int index){
        if (index >= 0 && index < getEntriesNumber()) {
            this.index = index;
        } else {
            invalidate();
        }
    }

    /**
     * Moves the current index to the last available entry in the block
     */
    public void last(){
        moveTo(getEntriesNumber()-1);
    }

    /**
     * Moves the current index to the first available entry in the block
     */
    public void first(){
        moveTo(0);
    }

    /**
     * Moves current index one step back: index=index-1
     */
    public void previous(){
        moveTo(index-1);
    }

    /**
     * Moves current index one step forward: index=index+1
     */
    public void next(){
        moveTo(index+1);
    }

    /**
     * This should be executed if an unexpected operation is executed on the invalid entry
     * @param message the message be provided in the exception.
     */
    protected void throwExceptionIfInvalid(final String message) {
        if (invalid()) {
            throw new RoStoreException("The <" + this + "> is invalid: " + message);
        }
    }

}
