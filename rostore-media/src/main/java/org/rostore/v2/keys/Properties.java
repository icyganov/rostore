package org.rostore.v2.keys;

public class Properties {

    /**
     * Specifies the maximum length of the key representation for internal logging.
     * <p>If the length of the key is greater than this number, the key is represented cut down.</p>
     * <p>This is used in the {@link Object#toString()} to represent a key and is not influencing any real-time business logic.</p>
     */
    public static final int MAX_STRING_KEY_REPRESENTATION = 50;
}
