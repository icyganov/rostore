package org.rostore.entity.media;

import java.util.EnumSet;

/**
 * Specify behaviour of the system when update or creation of key-value happens.
 *
 * <p>Needed to change some validation rules if required.</p>
 */
public enum RecordOption {

    /**
     * Should check on the key creation if the key already exist, and if not it should not allow an operation
     */
    ONLY_INSERT,

    /**
     * Should check if the update operation is executed on the existing key, and if not it should fail
     */
    ONLY_REPLACE,

    /**
     * This one if provided should circumvent the version-check and rewrite the version even if not correct
     */
    OVERRIDE_VERSION;

    /**
     * Parses the comma-separated set of {@link RecordOption}
     *
     * @param commaSeparated list of options separated by the comma
     * @return the enum set with the set of options from the list
     */
    public static EnumSet<RecordOption> parse(final String commaSeparated) {
        if (commaSeparated == null) {
            return EnumSet.noneOf(RecordOption.class);
        }
        final String[] separated = commaSeparated.split(",");
        final EnumSet<RecordOption> options = EnumSet.noneOf(RecordOption.class);
        for(String name : separated) {
            RecordOption recordOption = RecordOption.valueOf(name);
            options.add(recordOption);
        }
        return options;
    }
}
