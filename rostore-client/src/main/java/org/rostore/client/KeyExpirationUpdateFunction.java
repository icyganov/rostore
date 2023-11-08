package org.rostore.client;

import org.rostore.Utils;

/**
 * This interface (function) provides a flexible way to define a new expiration policy for the given object
 */
public interface KeyExpirationUpdateFunction {

    /**
     * @param originalObject
     * @return the unix EOL timestamp
     * @param <K>
     * @param <V>
     */
    <K,V> long unixEol(final VersionedObject<K,V> originalObject);

    KeyExpirationUpdateFunction noExpiration = new KeyExpirationUpdateFunction() {
        @Override
        public <K,V> long unixEol(VersionedObject<K,V> originalObject) {
            return Utils.EOL_FOREVER;
        }
    };
}
