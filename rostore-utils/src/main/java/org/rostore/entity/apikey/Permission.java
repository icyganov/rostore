package org.rostore.entity.apikey;

/**
 * A permission
 */
public enum Permission {
    /**
     * Permission to read value
     */
    READ,
    /**
     * Permission to list entries, e.g. keys in the container
     */
    LIST,
    /**
     * Permission to write data to the container or storage
     */
    WRITE,
    /**
     * Permission to delete entries. e.g. keys
     */
    DELETE,
    /**
     * Permission to create entries. e.g. keys
     */
    CREATE,
    /**
     * Permission to grant permissions
     */
    GRANT,
    /**
     * Permission to manage rostore instance in any form
     */
    SUPER
}
