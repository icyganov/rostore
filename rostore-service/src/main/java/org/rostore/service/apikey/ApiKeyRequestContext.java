package org.rostore.service.apikey;

import org.eclipse.microprofile.config.inject.ConfigProperty;

import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import java.util.Optional;

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
