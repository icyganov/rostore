package org.rostore.cli;

public enum Operation {
    GET_KEY("get-key", "get value of single key", new String[] {"k", "c"}, false),
    COPY_KEY("copy-key", "copy key from main to target", new String[] {"k", "c", "tk", "tc"}, true),
    PUT_KEY("put-key", "put key from main to target", new String[] {"k", "c", "i"}, false),
    REMOVE_KEY("remove-key", "remove key", new String[] {"k", "c"}, false),
    LIST_CONTAINER_KEYS("list-container-keys", "list container keys", new String[] {"c"}, false),
    LIST_CONTAINERS("list-containers", "list containers", null, false),
    COPY_CONTAINER("copy-container", "copy container from main to target", new String[] {"c", "tc"}, true),
    CREATE_CONTAINER("create-container", "create a new container", new String[] {"c"}, false),
    REMOVE_CONTAINER("remove-container", "remove given container", new String[] {"c"}, false),
    COPY_STORAGE("copy-storage", "copy storage completely", null, true),
    PING("ping", "ping rostore instance", null, false),
    SHUTDOWN("shutdown", "Shutdowns the rostore instance", null, false),
    HELP("help", "prints the help", null, false);

    private String cliName;
    private String description;

    private String[] requiredOptions;

    public String[] getRequiredOptions() {
        return requiredOptions;
    }

    public String getCliName() {
        return cliName;
    }

    public String getDescription() {
        return description;
    }

    private boolean needsTargetConnection;

    public boolean needsTargetConnection() {
        return needsTargetConnection;
    }

    Operation(final String cliName, final String description, final String[] requiredOptions, final boolean needsTargetConnection) {
        this.cliName = cliName;
        this.description = description;
        this.needsTargetConnection = needsTargetConnection;
        this.requiredOptions = requiredOptions;
    }

    public static Operation getByCliName(final String cliName) {
        for(final Operation cmd : Operation.values()) {
            if (cmd.cliName.equals(cliName)) {
                return cmd;
            }
        }
        throw new CliException("Unknown command " + cliName);
    }
}
