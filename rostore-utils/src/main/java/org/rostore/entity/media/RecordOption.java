package org.rostore.entity.media;

import java.util.EnumSet;

public enum RecordOption {
    ONLY_INSERT,
    ONLY_REPLACE,
    OVERRIDE_VERSION;

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
