package org.rostore.service.apikey;

import io.quarkus.runtime.Startup;
import io.quarkus.vertx.ConsumeEvent;
import org.eclipse.microprofile.config.inject.ConfigProperty;
import org.jboss.logging.Logger;
import org.rostore.entity.apikey.ApiKeyDefinition;
import org.rostore.entity.apikey.ApiKeyPermissions;
import org.rostore.entity.apikey.Permission;
import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.entity.media.ContainerMeta;
import org.rostore.v2.container.DataWithRecord;
import org.rostore.service.*;
import org.rostore.v2.container.async.AsyncContainer;
import org.rostore.entity.StringKeyList;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import java.util.*;

@Startup
@ApplicationScoped
public class ApiKeyManager {

    private final static Logger logger = Logger.getLogger(ContainerAdminService.class.getName());

    public static final String APIKEY_CONTAINER_NAME = "_rostore.internal.api-keys";
    private static final long CACHE_MILLIS = 15*60*1000;

    private static final int APIKEY_CONTAINER_SHARD_NUMBER = 2;

    private static final int APIKEY_CONTAINER_MAX_SIZE = 5 * 1024 * 1024;

    @Inject
    private RoStoreAccessor roStoreAccessor;

    @Inject @ConfigProperty(name="rootApiKey")
    private String rootApiKey;

    @Inject
    private ApiKeyRequestContext apiKeyRequestContext;

    private AsyncContainer apiKeyPool;

    private Map<String, DataWithRecord<ApiKeyDefinition>> keyCache = new HashMap<>();

    @ConsumeEvent("state-change")
    public void init(final RoStoreState state) {
        try {
            if (state == RoStoreState.OPENED) {
                boolean newCreation = false;
                logger.debugf("Opening an api-key container '%s'", APIKEY_CONTAINER_NAME);
                apiKeyPool = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(APIKEY_CONTAINER_NAME);
                if (apiKeyPool == null) {
                    newCreation = true;
                    ContainerMeta containerMeta = new ContainerMeta();
                    containerMeta.setShardNumber(APIKEY_CONTAINER_SHARD_NUMBER);
                    containerMeta.setMaxSize(APIKEY_CONTAINER_MAX_SIZE);
                    logger.debugf("Creating a new api-key container '%s'", APIKEY_CONTAINER_NAME);
                    apiKeyPool = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().create(APIKEY_CONTAINER_NAME, containerMeta);
                    logger.infof("A new api-key container '%s' has been created.", APIKEY_CONTAINER_NAME);
                } else {
                    logger.infof("An existing api-key container '%s' has been opened", APIKEY_CONTAINER_NAME);
                }
                if (newCreation) {
                    repairRootApiKeyEntry();
                }
            }
        } catch (Exception e) {
            logger.error("Error in the api key manager initialization", e);
        }
    }

    public void checkContainerPermission(final String containerName, final Set<Permission> requestPermissions) {
        final ApiKeyDefinition apiKeyDefinition = getAndCheckKey();
        final Set<Permission> storagePermissions = apiKeyDefinition.getApiKeyPermissions().getStorePermissions();
        if (storagePermissions.contains(Permission.SUPER)) {
            return ;
        }
        Set<Permission> containerPermissions = apiKeyDefinition.getApiKeyPermissions().getContainerPermissions(containerName);
        if (containerPermissions == null || !containerPermissions.containsAll(requestPermissions)) {
            throw new PermissionDeniedException("No access to the container " + containerName + " with set of permissions " + requestPermissions.toString());
        }
    }

    public void checkStorePermission(final Set<Permission> requestPermissions) {
        if (requestPermissions.contains(Permission.SUPER)) {
            // this is a special operation that requires a privileg of root
            if (isRootApiKey()) {
                return;
            }
        }
        final ApiKeyDefinition apiKeyDefinition = getAndCheckKey();
        final Set<Permission> storagePermissions = apiKeyDefinition.getApiKeyPermissions().getStorePermissions();
        if (storagePermissions.contains(Permission.SUPER)) {
            return ;
        }
        if (!storagePermissions.containsAll(requestPermissions)) {
            throw new PermissionDeniedException("No access to the storage with set of permissions " + requestPermissions);
        }
    }

    public boolean isRootApiKey() {
        return rootApiKey.equals(apiKeyRequestContext.getApiKey());
    }

    public DataWithRecord<ApiKeyDefinition> repairRootApiKeyEntry() {
        final ApiKeyPermissions apiKeyPermissions = new ApiKeyPermissions();
        apiKeyPermissions.setStorePermissions(EnumSet.of(Permission.SUPER));
        ApiKeyDefinition rootDefinition = new ApiKeyDefinition(rootApiKey, apiKeyPermissions);
        final Record record = new Record();
        update(rootDefinition, record);
        logger.info("Primary Root api key is updated: " + rootDefinition.toString());
        return new DataWithRecord<>(record, rootDefinition);
    }

    private void checkInitialized() {
        roStoreAccessor.getState().checkContainerRequestsAllowed();
        if (apiKeyPool == null) {
            throw new PermissionDeniedException("The store security mechanism is down.");
        }
    }

    public DataWithRecord<ApiKeyDefinition> create(final ApiKeyPermissions apiKeyPermissions, final Record record) {
        checkInitialized();
        final String key = UUID.randomUUID().toString();
        apiKeyPool.put(0, key, apiKeyPermissions, record);
        final ApiKeyDefinition apiKeyDefinition = new ApiKeyDefinition(key, apiKeyPermissions);
        synchronized (this) {
            keyCache.put(key, new DataWithRecord<>(record, apiKeyDefinition));
        }
        return new DataWithRecord<>(record, apiKeyDefinition);
    }

    public void update(final ApiKeyDefinition apiKeyDefinition, final Record record) {
        checkInitialized();
        apiKeyPool.put(0, apiKeyDefinition.getKey(), apiKeyDefinition.getApiKeyPermissions(), record);
        synchronized (this) {
            keyCache.put(apiKeyDefinition.getKey(), new DataWithRecord<>(record, apiKeyDefinition));
        }
    }

    public void remove(final String key, final Record record) {
        checkInitialized();
        apiKeyPool.remove(0, key, record);
    }

    public synchronized DataWithRecord<ApiKeyDefinition> get(final String key) {
        checkInitialized();
        DataWithRecord<ApiKeyDefinition> apiKeyDefinition = keyCache.get(key);
        if (apiKeyDefinition != null) {
            if (apiKeyDefinition.getData().getLastUpdate() + CACHE_MILLIS > System.currentTimeMillis()) {
                return apiKeyDefinition;
            }
        }
        DataWithRecord<ApiKeyPermissions> permissionsData = apiKeyPool.get(0, key, ApiKeyPermissions.class);
        if (permissionsData == null) {
            keyCache.remove(key);
            return null;
        }
        apiKeyDefinition = new DataWithRecord<>(permissionsData.getRecord(), new ApiKeyDefinition(key, permissionsData.getData()));
        keyCache.put(key, apiKeyDefinition);
        return apiKeyDefinition;
    }

    public synchronized StringKeyList list(final String startKey) {
        checkInitialized();
        return apiKeyPool.list(0, startKey, null, 100, 10*1024*1024);
    }

    public ApiKeyDefinition getAndCheckKey() {
        final String apiKey = apiKeyRequestContext.getApiKey();
        if (apiKey == null) {
            throw new PermissionDeniedException("No Api-Key provided in the request and No public key configured on the store.");
        }
        final DataWithRecord<ApiKeyDefinition> withRecord = get(apiKey);
        if (withRecord == null) {
            if (apiKeyRequestContext.isDefaultApiKeyUsed()) {
                throw new PermissionDeniedException("Default Api Key is not associated with any resource.");
            } else {
                throw new PermissionDeniedException("Api key " + apiKey + " is not known or expired.");
            }
        }
        return withRecord.getData();
    }

}
