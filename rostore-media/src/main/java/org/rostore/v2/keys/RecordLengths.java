package org.rostore.v2.keys;

import org.rostore.Utils;
import org.rostore.v2.media.MediaProperties;

/**
 * Defines the lengths of the key record components in bytes
 */
public class RecordLengths {

    private final int idLength;
    private final int eolLength;
    private final int versionLength;

    /**
     * Initializes the record length with the standard
     * @param mediaProperties the properties of the media
     * @return the object with length
     */
    public static final RecordLengths standardRecordLengths(final MediaProperties mediaProperties) {
        return new RecordLengths(mediaProperties.getMapperProperties().getBytesPerBlockIndex(),
                Utils.BYTES_PER_EOL,
                Utils.BYTES_PER_VERSION);
    }

    /**
     * Initializes the record lengths manually
     *
     * @param idLength the length of the id element for every key entry
     * @param eolLength the length of the end of life element for every key entry
     * @param versionLength the length of the version element for every key element
     */
    public RecordLengths(int idLength, int eolLength, int versionLength) {
        this.idLength = idLength;
        this.eolLength = eolLength;
        this.versionLength = versionLength;
    }

    /**
     * The length of the id element for every key entry
     * @return the length in bytes
     */
    public int getIdLength() {
        return idLength;
    }

    /**
     * The length of the end of life element for every key entry
     * @return the length in bytes
     */
    public int getEolLength() {
        return eolLength;
    }

    /**
     * The length of the version element for every key element
     * @return the length in bytes
     */
    public int getVersionLength() {
        return versionLength;
    }

    /**
     * The length of all elements of record
     * @return the length in bytes
     */
    public int getTotalLength() {
        return idLength + eolLength + versionLength;
    }
}
