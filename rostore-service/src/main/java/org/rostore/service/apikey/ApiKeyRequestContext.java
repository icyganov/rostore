package org.rostore.service.apikey;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import java.util.Optional;

/**
 * Class is injected in the REST filters and extract information
 * about the API key of the current request.
 * <p>Is used to assess the permissions associated with the current call.</p>
 */
@RequestScoped
public class ApiKeyRequestContext {

    @Inject @ConfigProperty(name="defaultApiKey")
    private Optional<String> defaultApiKey;

    private boolean defaultApiKeyUsed = false;

    private String apiKey = null;

    private long startTimestamp;

    public boolean isDefaultApiKeyUsed() {
        return defaultApiKeyUsed;
    }

    public String getApiKey() {
        return apiKey;
    }

    public long startTimestamp() {
        return startTimestamp;
    }

    public void init(final String apiKey) {
        this.startTimestamp = System.currentTimeMillis();
        if (apiKey != null) {
            this.apiKey = apiKey;
            return;
        }
        if (defaultApiKey.isEmpty()) {
            return;
        }
        defaultApiKeyUsed = true;
        this.apiKey = defaultApiKey.get();
    }

}
