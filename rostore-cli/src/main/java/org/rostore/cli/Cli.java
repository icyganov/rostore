package org.rostore.cli;

import org.apache.commons.cli.*;
import org.rostore.Utils;
import org.rostore.client.*;
import org.rostore.entity.apikey.ApiKeyPermissions;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.entity.media.RecordOption;

import java.io.*;
import java.text.SimpleDateFormat;
import java.time.Duration;
import java.time.temporal.TemporalUnit;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Cli {

    private final static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy kk:mm:ss");

    private final static Options options = options();
    private final static ExecutorService executor = Executors.newFixedThreadPool(5);

    public static void main(final String[] params) {
        boolean verbose = false;
        try {

            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(options, params);

            verbose = cmd.hasOption("v");

            if (params.length < 1) {
                print("RoStore 2.0 CLI (c) 2022 RoStore Group");
                help(options);
                return;
            }

            Operation operation = Operation.getByCliName(params[0]);
            mainWithException(operation, cmd);
        } catch (final CliException cliException) {
            System.err.println("Error: " + cliException.getMessage());
            System.exit(1);
        } catch(final UnrecognizedOptionException|MissingArgumentException op) {
            System.err.println("Error: " + op.getMessage());
            System.exit(1);
        } catch(final ClientException ce) {
            System.err.println("Error: " + ce.getMessage());
            if (verbose && ce.getServerMessage() != null) {
                System.err.println("Message: " + ce.getServerMessage());
            }
            if (verbose) {
                ce.printStackTrace();
            }
            System.exit(1);
        } catch (final Exception e) {
            System.err.println("Internal Error: " + e.getMessage());
            if (verbose) {
                e.printStackTrace();
            }
            System.exit(1);
        } finally {
            executor.shutdown();
        }
    }

    private static Options options() {
        Options options = new Options();
        options.addOption("p", "profile", true, "specify main profile (profile in .store directory or a plain file)");
        options.addOption("mt", "max-ttl", true, "specify max ttl for container creation in java duration format");
        options.addOption("ms", "max-size", true, "specify max size for container creation");
        options.addOption("sn", "shard-number", true, "specify shard number for container creation");
        options.addOption("tp", "target-profile", true, "specify target profile (profile in .store directory or a plain file)");
        options.addOption("c", "container", true, "specify container");
        options.addOption("tc", "target-container", true, "specify container");
        options.addOption("k", "key", true, "specify key");
        options.addOption("tk", "target-key", true, "specify target key");
        options.addOption("sk", "start-with-key", true, "specify start with key");
        options.addOption("ck", "continuation-key", true, "specify continuation key");
        options.addOption("v", "verbose", false, "enable verbose mode");
        options.addOption("o", "output", true, "output file");
        options.addOption("i", "input", true, "input file");
        options.addOption("ver", "version", true, "use version for a key");
        options.addOption("ro", "record-options", true, "comma separated record options: " + Arrays.toString(RecordOption.values()));
        options.addOption("ttl", null, true, "use ttl for a key in java duration format");
        return options;
    }

    public static void mainWithException(Operation operation, CommandLine cmd) throws Exception {

        boolean verbose = cmd.hasOption("v");

        if (verbose) {
            print("RoStore 2.0 CLI (c) 2023 RoStore Group");
        }

        if (Operation.HELP.equals(operation)) {
            help(options);
            return;
        }

        final String[] reqOptions = operation.getRequiredOptions();
        if (reqOptions != null) {
            for (final String ro : reqOptions) {
                if (!cmd.hasOption(ro)) {
                    throw new CliException("Required option is not provided: " + ro);
                }
            }
        }

        File outputFile =  null;

        if (cmd.hasOption("o")) {
            outputFile = new File(cmd.getOptionValue("o"));
            if (verbose) {
                print("Output file: " + outputFile.getCanonicalPath());
            }
        }

        String profile = cmd.getOptionValue('p');
        String targetProfile = cmd.getOptionValue("tp");

        RoStoreClientProperties clientProperties = null;
        RoStoreClientProperties targetClientProperties = null;

        if (profile != null) {
            clientProperties = RoStorePropertiesLoader.userProfile(profile, verbose);
            if (verbose) {
                print("Using main profile: " + profile);
            }
        } else {
            clientProperties = RoStorePropertiesLoader.systemWide();
            if (verbose) {
                print("Using main system rostore");
            }
        }

        if (operation.needsTargetConnection()) {
            if (targetProfile != null) {
                targetClientProperties = RoStorePropertiesLoader.userProfile(targetProfile, verbose);
                if (verbose) {
                    print("Using target profile: " + profile);
                }
            } else {
                targetClientProperties = RoStorePropertiesLoader.systemWide();
                if (verbose) {
                    print("Using target system rostore");
                }
            }
        }

        if (verbose) {
            print("Main connection:");
            print(" baseUrl: " + clientProperties.getBaseUrl());
            if (clientProperties.getApiKey() != null) {
                print(" apiKey:  " + clientProperties.getApiKey().substring(0, 4) + "xxx...");
            }
            if (operation.needsTargetConnection()) {
                print("Target connection:");
                print(" baseUrl: " + targetClientProperties.getBaseUrl());
                if (clientProperties.getApiKey() != null) {
                    print(" apiKey:  " + targetClientProperties.getApiKey().substring(0, 4) + "xxx...");
                }
            }
        }

        final RoStoreClient roStoreClient = new RoStoreClient(clientProperties);
        final RoStoreClient targetRoStoreClient = operation.needsTargetConnection() ? new RoStoreClient(targetClientProperties) : null;

        switch (operation) {
            case SHUTDOWN:
                roStoreClient.shutdown();
                return;
            case PING:
                if (roStoreClient.ping()) {
                    print("pong");
                    return;
                } else {
                    throw new CliException("Ping failed");
                }
            case LIST_CONTAINER_KEYS:
                listContainerKeys(cmd, verbose, roStoreClient);
                return;
            case LIST_CONTAINERS:
                listContainers(cmd, verbose, roStoreClient);
                return;
            case COPY_STORAGE:
                copyStorage(verbose, roStoreClient, targetRoStoreClient);
                return;
            case COPY_CONTAINER:
                copyContainer(cmd, verbose, roStoreClient, targetRoStoreClient);
                return;
            case REMOVE_CONTAINER:
                removeContainer(cmd, verbose, roStoreClient);
                return;
            case CREATE_CONTAINER:
                createContainer(cmd, verbose, roStoreClient);
                return;
            case COPY_KEY:
                copyKey(cmd, verbose, roStoreClient, targetRoStoreClient);
                return;
            case PUT_KEY:
                putKey(cmd, verbose, roStoreClient);
                return;
            case REMOVE_KEY:
                removeKey(cmd, verbose, roStoreClient);
                return;
            case GET_KEY:
                getKey(cmd, verbose, outputFile, roStoreClient);
                return;
        }

        help(options);

    }

    private static void removeContainer(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient) {
        if (verbose) {
            print("Remove container:");
            print(" container: " + cmd.getOptionValue("c"));
        }
        final GeneralContainer container = roStoreClient.getGeneralContainer(cmd.getOptionValue("c"));
        container.remove();
    }

    private static void createContainer(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient) {
        long maxSize = 0;
        long maxTTL = 0;
        int shardNumber = 5;
        if (cmd.hasOption("ms")) {
            maxSize = Long.parseLong(cmd.getOptionValue("ms"));
        }
        if (cmd.hasOption("mt")) {
            maxTTL = Duration.parse(cmd.getOptionValue("mt")).getSeconds();
        }
        if (cmd.hasOption("sn")) {
            shardNumber = Integer.parseInt(cmd.getOptionValue("sn"));
        }
        if (verbose) {
            print("Create container:");
            print(" container:   " + cmd.getOptionValue("c"));
            if (cmd.hasOption("ms")) {
                print(" maxSize:     " + maxSize);
            } else {
                print(" maxSize:     unlimited");
            }
            if (cmd.hasOption("mt")) {
                print(" maxTTL:      " + maxTTL + "s");
            } else {
                print(" maxTTL:      unlimited");
            }
            print(" shardNumber: " + shardNumber);
        }
        final ContainerMeta containerMeta = new ContainerMeta();
        containerMeta.setShardNumber(shardNumber);
        containerMeta.setMaxTTL(maxTTL);
        containerMeta.setMaxSize(maxSize);
        final GeneralContainer container = roStoreClient.getGeneralContainer(cmd.getOptionValue("c"));
        container.create(containerMeta);
    }

    private static void copyContainer(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient, RoStoreClient targetRoStoreClient) {
        if (verbose) {
            print("Copy container:");
            print(" container: " + cmd.getOptionValue("c"));
            print("To:");
            print(" container: " + cmd.getOptionValue("tc"));
            print("Keys:");
        }
        final GeneralContainer container = roStoreClient.getGeneralContainer(cmd.getOptionValue("c"));
        final GeneralContainer targetContainer = targetRoStoreClient.getGeneralContainer(cmd.getOptionValue("tc"));
        String continuationKey = cmd.getOptionValue("ck");
        String startWithKey = cmd.getOptionValue("sk");
        copyContainer(verbose, container, continuationKey, startWithKey, targetContainer);
    }

    private static void copyContainer(final boolean verbose,
                                       final GeneralContainer container,
                                       final String startingContinuationKey,
                                       final String startWithKey,
                                       final GeneralContainer targetContainer) {
        String continuationKey = startingContinuationKey;
        StringKeyList stringKeyList = container.listKeys(startWithKey, continuationKey);
        do {
            final List<Future> copyKeyJobs = new ArrayList<>();
            for(final String key : stringKeyList.getKeys()) {
                if (verbose) {
                    print("Copy " + key + "...");
                    copyKeyJobs.add(copyKeyAsync(container, key, targetContainer, key));
                }
            }
            for(Future f : copyKeyJobs) {
                try {
                    f.get();
                } catch (InterruptedException|ExecutionException e) {
                    if (e.getCause() != null && e.getCause() instanceof ClientException) {
                       throw (ClientException) e.getCause();
                    }
                    throw new RuntimeException(e);
                }
            }
            if (!stringKeyList.isMore()) {
                break;
            } else {
                continuationKey = stringKeyList.getKeys().get(stringKeyList.getKeys().size()-1);
                stringKeyList = container.listKeys(startWithKey, continuationKey);
            }
        } while ( true );
    }

    private static void copyApiKeys(final boolean verbose,
                                      final ApiKeys apiKeys,
                                      final ApiKeys targetApiKeys) {
        String continuationKey = null;
        do {
            StringKeyList apiKeyList = apiKeys.listApiKeys(continuationKey);
            for(final String key : apiKeyList.getKeys()) {
                if (verbose) {
                    print("Copy Api-Key " + key + "...");
                    copyApiKey(apiKeys, key, targetApiKeys, key);
                }
            }
            if (!apiKeyList.isMore()) {
                break;
            } else {
                continuationKey = apiKeyList.getKeys().get(apiKeyList.getKeys().size()-1);
            }
        } while ( true );
    }

    private static void copyStorage(boolean verbose, RoStoreClient roStoreClient, RoStoreClient targetRoStoreClient) {
        if (verbose) {
            print("Copy Storage completely");
        }
        String[] containers = roStoreClient.listContainers();
        for(String containerName : containers) {
            if ("_rostore.internal.api-keys".equals(containerName)) {
                if (verbose) {
                    print("Copy api-keys:");
                }
                final ApiKeys apiKeys = roStoreClient.getApiKeys();
                final ApiKeys targetApiKeys = targetRoStoreClient.getApiKeys();
                copyApiKeys(verbose, apiKeys, targetApiKeys);
            } else {
                if (verbose) {
                    print("Copy container:");
                    print(" container: " + containerName);
                }
                final GeneralContainer container = roStoreClient.getGeneralContainer(containerName);
                final ContainerMeta containerMeta = container.getMeta();
                if (verbose) {
                    print("Meta: ");
                    print(" creationTime: " + containerMeta.getCreationTime());
                    print(" maxSize:      " + containerMeta.getMaxSize());
                    print(" shardNumber:  " + containerMeta.getShardNumber());
                    print(" maxTTL:       " + containerMeta.getMaxTTL());
                }
                final GeneralContainer targetContainer = targetRoStoreClient.getGeneralContainer(containerName);
                try {
                    targetContainer.getMeta();
                } catch(final Exception e) {
                    print("No container meta detected for " + containerName + " , create a new target container");
                    targetContainer.create(containerMeta);
                }
                copyContainer(verbose, container, null, null, targetContainer);
            }
        }
    }

    private static void listContainerKeys(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient) {
        if (verbose) {
            print("List container:");
            print(" container: " + cmd.getOptionValue("c"));
            print("Keys:");
        }
        final GeneralContainer container = roStoreClient.getGeneralContainer(cmd.getOptionValue("c"));
        final StringKeyList stringKeyList = container.listKeys(cmd.getOptionValue("sk"), cmd.getOptionValue("ck"));
        for(final String key : stringKeyList.getKeys()) {
            print(key);
        }
        if (stringKeyList.isMore()) {
            print("more: true, --continuation-key=" + stringKeyList.getContinuationKey());
        } else {
            print("more: false");
        }
    }

    private static void listContainers(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient) {
        if (verbose) {
            print("Containers:");
        }
        final String[] stringContainerList = roStoreClient.listContainers();
        for(final String container : stringContainerList) {
            print(container);
        }
    }

    private static void removeKey(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient) {
        Long version = Utils.VERSION_UNDEFINED;
        EnumSet<RecordOption> recordOptions = RecordOption.parse(cmd.getOptionValue("ro"));
        if (cmd.hasOption("ver")) {
            version = Long.parseLong(cmd.getOptionValue("ver"));
        }
        if (verbose) {
            print("Remove a key:");
            print(" key:       " + cmd.getOptionValue("k"));
            print(" container: " + cmd.getOptionValue("c"));
            print(" version:   " + (version != Utils.VERSION_UNDEFINED ? version : "none"));
            print(" options:   " + recordOptions);
        }
        GeneralContainer container = roStoreClient.getGeneralContainer(cmd.getOptionValue("c"));
        container.removeKey(cmd.getOptionValue("k"), version, recordOptions);
    }

    private static void copyKey(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient, RoStoreClient targetRoStoreClient) {
        if (verbose) {
            print("Copy a key from:");
            print(" key:       " + cmd.getOptionValue("k"));
            print(" container: " + cmd.getOptionValue("c"));
            print("To:");
            print(" key:       " + cmd.getOptionValue("tk"));
            print(" container: " + cmd.getOptionValue("tc"));
        }
        copyKey(roStoreClient.getGeneralContainer(cmd.getOptionValue("c")), cmd.getOptionValue("k"), targetRoStoreClient.getGeneralContainer(cmd.getOptionValue("tc")), cmd.getOptionValue("tk"));
    }
    private static void copyKey(final GeneralContainer<String> container,
                                final String key,
                                final GeneralContainer<String> targetContainer,
                                final String targetKey) {
        container.getWrapped(key, null, versionedInputStream ->
            targetContainer.post(VersionedObject.create(targetKey, versionedInputStream.getValue(), versionedInputStream.getVersion(), versionedInputStream.getUnixEOL()),
                    EnumSet.of(RecordOption.OVERRIDE_VERSION),
                    (responseIs) -> responseIs)
        );

    }

    private static Future copyKeyAsync(final GeneralContainer container,
                                       final String key,
                                       final GeneralContainer targetContainer,
                                       final String targetKey) {
        return executor.submit(() -> {
            copyKey(container, key, targetContainer, targetKey);
        });
    }

    private static void copyApiKey(final ApiKeys apiKeys,
                                final String key,
                                final ApiKeys targetApiKeys,
                                final String targetKey) {
        VersionedObject<String,ApiKeyPermissions> sourceObject = apiKeys.get(key);
        targetApiKeys.put(VersionedObject.create(targetKey, sourceObject.getValue(), null, sourceObject.getUnixEOL()));
    }

    private static void getKey(CommandLine cmd, boolean verbose, File outputFile, RoStoreClient roStoreClient) throws IOException {
        EnumSet<RecordOption> recordOptions = RecordOption.parse(cmd.getOptionValue("ro"));
        if (verbose) {
            print("Print a key:");
            print(" key:       " + cmd.getOptionValue("k"));
            print(" container: " + cmd.getOptionValue("c"));
            print(" options:   " + recordOptions);
        }
        roStoreClient.getGeneralContainer(cmd.getOptionValue("c")).
                getWrapped(cmd.getOptionValue("k"),
                        recordOptions,
                        vis -> {
                            if (verbose) {
                                print("Results: ");
                                print(" version:    " + (vis.getVersion() != Utils.VERSION_UNDEFINED ? vis.getVersion() : "none"));
                                long unixEol = vis.getUnixEOL();
                                final Date date = new Date(unixEol);
                                final String expiration = simpleDateFormat.format(date);
                                print(" expiration: " + (unixEol != Utils.EOL_FOREVER ? expiration + " (unixEOL=" + unixEol + ")" : "none"));
                            }
                            try(final InputStream is = vis.getValue()) {
                                if (verbose) {
                                    System.out.print(" value:      ");
                                }
                                final OutputStream outStream = outputFile != null ? new FileOutputStream(outputFile) : null;
                                int result = is.read();
                                while (result != -1) {
                                    byte b = (byte) result;
                                    if (outStream == null) {
                                        System.out.print((char) b);
                                    }
                                    if (outStream != null) {
                                        outStream.write(b);
                                    }
                                    result = is.read();
                                }
                                System.out.println();
                            } catch (final IOException io) {
                                throw new CliException("Error loading data", io);
                            }
                            return null;
                        });
    }

    private static void putKey(CommandLine cmd, boolean verbose, RoStoreClient roStoreClient) {
        long version = Utils.VERSION_UNDEFINED;
        long unixEol = Utils.EOL_FOREVER;
        EnumSet<RecordOption> recordOptions = RecordOption.parse(cmd.getOptionValue("ro"));
        if (cmd.hasOption("ver")) {
            version = Long.parseLong(cmd.getOptionValue("ver"));
        }
        if (cmd.hasOption("ttl")) {
            Duration ttlDur = Duration.parse(cmd.getOptionValue("ttl"));
            long ttlSec = ttlDur.toMillis() / 1000;
            unixEol = Utils.ttl2unixEol(ttlSec);
        }
        if (verbose) {
            print("Put a key:");
            print(" key:        " + cmd.getOptionValue("k"));
            print(" container:  " + cmd.getOptionValue("c"));
            print(" input:      " + cmd.getOptionValue("i"));
            print(" version:    " + (version != Utils.VERSION_UNDEFINED ? version : "none"));
            print(" options:    " + recordOptions);
            final Date date = new Date(unixEol);
            final String expiration = simpleDateFormat.format(date);
            print(" expiration: " + (unixEol != Utils.EOL_FOREVER ? expiration + " (unixEOL=" + unixEol + ")" : "none"));
        }

        try (final InputStream is = new FileInputStream(cmd.getOptionValue("i"))) {
            VersionedObject<String,InputStream> versionedObject = VersionedObject.create(cmd.getOptionValue("k"),
                    is, version, unixEol);
            final GeneralContainer<String> container = roStoreClient.getGeneralContainer(cmd.getOptionValue("c"));
            final VersionedObject<String,InputStream> result = container.post(
                    versionedObject,
                    recordOptions,
                    (is2) -> is2
            );
            if (verbose) {
                print("Result:");
                print(" version:    " + (result.getVersion() != Utils.VERSION_UNDEFINED ? result.getVersion() : "none"));
                final Date date = new Date(result.getUnixEOL());
                final String expiration = simpleDateFormat.format(date);
                print(" expiration: " + (result.getUnixEOL() != Utils.EOL_FOREVER ? expiration + " (unixEOL=" + result.getUnixEOL() + ")" : "none"));
            }
        }  catch (IOException io) {
            throw new CliException("Can't open input file \"" + cmd.getOptionValue("i") + "\"", io);
        }
    }

    protected static String pad(int len) {
        char[] padding = new char[len];
        Arrays.fill(padding, ' ');
        return new String(padding);
    }

    public static void help(final Options options) {
        final HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("rostore-cli operation [options, ...]", "Operations:\n" + operations() + "Options:\n", options, null);
        print("Examples:");
        print(" Get a key:");
        print("  > rostore-cli get --key xyz --container cont1");
    }

    private static String operations() {
        StringBuilder stringBuilder = new StringBuilder();
        int max = 0;
        for(Operation operation : Operation.values()) {
            max = Math.max(max, operation.getCliName().length());
        }
        max += 2;
        for(Operation operation : Operation.values()) {
            stringBuilder.append(" ");
            stringBuilder.append(operation.getCliName());
            stringBuilder.append(pad(max - operation.getCliName().length()));
            stringBuilder.append(operation.getDescription());
            stringBuilder.append("\n");
        }
        return stringBuilder.toString();
    }

    public static void print(final String message) {
        System.out.println(message);
    }

}
