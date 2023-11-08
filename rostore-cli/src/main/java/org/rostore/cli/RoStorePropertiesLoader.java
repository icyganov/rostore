package org.rostore.cli;

import org.rostore.client.RoStoreClientProperties;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
import java.time.Duration;
import java.util.Properties;

public class RoStorePropertiesLoader {

    public static RoStoreClientProperties systemWide() {
        final String userDir = System.getProperty("user.dir");
        final String rostoreHomeDir = System.getenv("ROSTORE_HOME");

        File rostoreHomeDirectory = new File(rostoreHomeDir != null ? rostoreHomeDir : userDir);

        if (!rostoreHomeDirectory.exists()) {
            throw new CliException("Current/Home directory " + rostoreHomeDirectory + " does not exist.");
        }
        File rostorePropertiesFile = new File(rostoreHomeDirectory, "rostore.properties");
        if (!rostorePropertiesFile.exists()) {
            throw new CliException(rostorePropertiesFile + " does not exist.");
        }
        return fromRoStorePropertiesFile(rostorePropertiesFile);
    }

    public static RoStoreClientProperties userProfile(final String profileName, final boolean verbose) {
        String userHomeDir = System.getProperty("user.home");
        File profileFile = new File(userHomeDir, ".rostore/" + profileName);
        if (!profileFile.exists()) {
            profileFile = new File(profileName);
            if (!profileFile.exists()) {
                throw new CliException("Neither \"" + profileFile + "\" nor \"" + profileName + "\" are found as a profile.");
            }
            if (verbose) {
                Cli.print("Opened profile as a file: " + profileName);
            }
        } else {
            if (verbose) {
                Cli.print("Opened profile from home directory: " + profileName);
            }
        }

        Properties properties = getProperties(profileFile);

        String baseUrl = properties.getProperty("baseUrl");
        String apiKey = properties.getProperty("apiKey");
        String requestTimeout = properties.getProperty("requestTimeout");
        String connectionTimeout = properties.getProperty("connectionTimeout");

        if (baseUrl == null) {
            throw new CliException("Can't get the baseUrl from the " + profileFile);
        }

        RoStoreClientProperties rp = new RoStoreClientProperties(baseUrl, apiKey);
        if (requestTimeout != null) {
            rp.setRequestTimeout(Duration.parse(requestTimeout));
        }
        if (connectionTimeout != null) {
            rp.setConnectTimeout(Duration.parse(connectionTimeout));
        }
        return rp;
    }

    private static Properties getProperties(File file) throws CliException {
        final Properties prop = new Properties();
        try (InputStream stream = new FileInputStream(file)) {
            prop.load(stream);
        } catch(IOException ioException) {
            throw new CliException(file + " can't be opened.", ioException);
        }
        return prop;
    }

    public static RoStoreClientProperties fromRoStorePropertiesFile(final File rostorePropertiesFile) {

        Properties prop = getProperties(rostorePropertiesFile);

        final String host = (String)prop.getOrDefault("ROSTORE_HOST", "localhost");
        final String http = (String)prop.getOrDefault("ROSTORE_HTTP_LISTENER", "disabled");
        final String httpPort = (String)prop.getOrDefault("ROSTORE_HTTP_PORT", "80");
        final String httpsPort = (String)prop.getOrDefault("ROSTORE_HTTPS_PORT", "443");
        final String httpsCert = prop.getProperty("ROSTORE_CERT_FILE");
        String baseUrl = null;
        if (httpsCert != null) {
            // https configured. Use it.
            try {
                baseUrl = "https://" + host + ":" + httpsPort;
                new URL(baseUrl);
            } catch (final MalformedURLException e) {
                baseUrl = null;
            }
        }
        if ("enabled".equals(http)) {
            try {
                baseUrl = "http://" + host + ":" + httpPort;
                new URL(baseUrl);
            } catch (MalformedURLException e) {
                baseUrl = null;
            }
        }
        String apiKey = prop.getProperty("ROSTORE_ROOT_API_KEY");
        if (baseUrl == null) {
            throw new CliException("Can't get the baseUrl from the " + rostorePropertiesFile);
        }
        return new RoStoreClientProperties(baseUrl, apiKey);
    }

}
