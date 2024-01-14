package org.rostore.client;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.classic.methods.*;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.CloseableHttpResponse;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.ManagedHttpClientConnectionFactory;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.io.HttpClientConnectionManager;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.HttpsSupport;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpMessage;
import org.apache.hc.core5.http.ProtocolException;
import org.apache.hc.core5.http.URIScheme;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.http.impl.io.MonitoringResponseOutOfOrderStrategy;
import org.apache.hc.core5.http.io.entity.InputStreamEntity;
import org.apache.hc.core5.util.Timeout;
import org.rostore.Utils;
import org.rostore.client.mapper.JsonMapper;
import org.rostore.client.mapper.Mapper;
import org.rostore.client.mapper.NativeMapper;
import org.rostore.entity.media.RecordOption;
import org.rostore.entity.media.Version;
import org.rostore.mapper.MapperProperties;

import javax.net.ssl.*;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.function.Function;

/**
 * The major object to access the remote rostore service.
 */
public class RoStoreClient {

    public static final String HEADER_VERSION = "version";
    public static final String HEADER_EOL = "eol";
    public static final String HEADER_OPTIONS = "options";
    public static final String HEADER_TRACKING_ID = "trackingId";
    public static final String HEADER_API_KEY = "api-key";
    public static final String HEADER_CONTENT_TYPE = "Content-Type";
    public static final String PATH__PING = "/admin/store/ping";
    public static final String PATH__SHUTDOWN = "/admin/store/shutdown";
    public static final String PATH__MAPPER_PROPERTIES = "/admin/store/mapper-properties";
    public static final String PATH__VERSION = "/admin/store/version";

    private TrustManager[] trustManagers = null;

    private final RoStoreClientProperties properties;

    private CloseableHttpClient httpClient;

    private final JsonMapper jsonMapper;

    private final NativeMapper nativeMapper;

    private final Version version;

    public JsonMapper getJsonMapper() {
        return jsonMapper;
    }

    public NativeMapper getNativeMapper() {
        return nativeMapper;
    }

    /**
     * Get the list of all container names on the remote service
     *
     * @return the array of all container names
     */
    public String[] listContainers() {
        RequestProperties requestProperties = create("/container/list").contentType(ContentType.JSON.getMediaType());
        return get(requestProperties, (httpResponse) ->
                jsonMapper.toObject(getInputStream(httpResponse), String[].class, "container-list")
        );
    }

    /**
     * Creates a new light-weight object that represents access to the container.
     *
     * <p>All value objects will be mapped by the provided {@link Mapper}.</p>
     * <p>Keys are mapped by {@link Object#toString()}.</p>
     *
     * @param name the name of the container
     * @param mapper the mapper to map the value objects from services
     * @return the mapped container
     * @param <K> type of the key
     */
    public <K> MappedContainer<K> getMappedContainer(final String name, final Mapper mapper) {
        return new MappedContainer(this, name, mapper, null);
    }

    /**
     * Creates a new light-weight object that represents access to the container.
     *
     * <p>All value objects will be mapped by the provided {@link Mapper}.</p>
     * <p>Keys will by mapped by {@link KeySerializer}.</p>
     *
     * @param name the name of the container
     * @param mapper the mapper to map the value objects from services
     * @return the mapped container
     * @param <K> type of the key
     */
    public <K> MappedContainer<K> getMappedContainer(final String name, final Mapper mapper, final KeySerializer<K> keySerializer) {
        return new MappedContainer(this, name, mapper, keySerializer);
    }

    /**
     * Creates object {@link GeneralContainer}.
     * Container itself is not created or opened, user respective function on the object itself.
     *
     * @param name name of the container
     * @return the general container
     * @param <K> type of the key
     */
    public <K> GeneralContainer<K> getGeneralContainer(final String name) {
        return getGeneralContainer(name, null);
    }

    /**
     * Creates object {@link GeneralContainer}.
     * <p>Container itself is not created or opened, user respective function on the object itself.</p>
     * <p>This function will create a new object every time the call is done. The caller may decide to
     * store it and reuse on every operation.</p>
     * <p>This object is very light-weight.</p>
     *
     * @param name name of the container
     * @param keySerializer serializer for the key object or null (in later case the {@link DefaultKeySerializer} is used)
     * @return container
     * @param <K> type of the key
     */
    public <K> GeneralContainer<K> getGeneralContainer(final String name, final KeySerializer<K> keySerializer) {
        return new GeneralContainer(this, name, keySerializer == null ? DefaultKeySerializer.INSTANCE : keySerializer);
    }

    /**
     * Returns object that manage the api keys
     * @return api keys object
     */
    public ApiKeys getApiKeys() {
        return new ApiKeys(this);
    }

    /**
     * Checks that the server is up and running
     *
     * @return {@code true} if server answers the ping request
     */
    public boolean ping() {
        return getString(create(PATH__PING).contentType(ContentType.JSON.getMediaType())).equals("pong");
    }

    /**
     * Stops the server
     */
    public void shutdown() {
        getString(create(PATH__SHUTDOWN).contentType(ContentType.JSON.getMediaType()));
    }

    /**
     * Creates a base request properties for the given path
     * @param path the path for the call
     * @return the request properties object
     */
    public RequestProperties create(final String path) {
        return new RequestProperties(properties).path(path);
    }

    /**
     * Returns the rostore connection properties
     * @return properties
     */
    public RoStoreClientProperties getProperties() {
        return properties;
    }

    /**
     * Creates a new rostore client
     * <p>It is recommended to create one instance of this type for each rostore connection with specific base url and api key</p>
     * @param roStoreProperties
     */
    public RoStoreClient(final TrustManager[] trustManagers,
                         final RoStoreClientProperties roStoreProperties) {
        this.trustManagers = trustManagers;
        this.properties = roStoreProperties;
        ObjectMapper objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        this.jsonMapper = new JsonMapper(new ObjectMapper());
        this.httpClient = getHttpClient();
        final MapperProperties mapperProperties = loadMapperProperties();
        if (mapperProperties != null) {
            this.nativeMapper = new NativeMapper(mapperProperties);
        } else {
            this.nativeMapper = null;
        }
        this.version = loadVersion();
    }

    public RoStoreClient(final RoStoreClientProperties roStoreProperties) {
        this(null, roStoreProperties);
    }

    private Version loadVersion() {
        try {
            final RequestProperties requestProperties = create(PATH__VERSION).
                    contentType(ContentType.JSON.getMediaType());
            return get(requestProperties, (response) ->
                    jsonMapper.toObject(getInputStream(response), Version.class, "info"));
        } catch (Exception e) {
            return null;
        }
    }

    private MapperProperties loadMapperProperties() {
        try {
            final RequestProperties requestProperties = create(PATH__MAPPER_PROPERTIES).
                    contentType(ContentType.JSON.getMediaType());
            return get(requestProperties, (response) ->
                    jsonMapper.toObject(getInputStream(response), MapperProperties.class, "mapper-properties"));
        } catch (final ClientException clientException) {
            return null;
        }
    }

    private void setHeaders(final HttpMessage message, final RequestProperties requestProperties) {
        if (requestProperties.getContentType() != null) {
            message.addHeader(HEADER_CONTENT_TYPE, requestProperties.getContentType());
        }
        if (properties.getApiKey() != null) {
            message.addHeader(HEADER_API_KEY, properties.getApiKey());
        }
        if (requestProperties.trackingId() != null) {
            message.addHeader(HEADER_TRACKING_ID, requestProperties.trackingId());
        }
        if (requestProperties.version() != null) {
            message.addHeader(HEADER_VERSION, requestProperties.version().toString());
        }
        if (requestProperties.getEOL() != null && requestProperties.getEOL() != Utils.EOL_FOREVER) {
            message.addHeader(HEADER_EOL, convertEOLToHeader(requestProperties.getEOL()));
        }
        if (!requestProperties.getRecordOptions().isEmpty()) {
            StringBuilder sb = new StringBuilder();
            for(RecordOption recordOption : requestProperties.getRecordOptions()) {
                if (!sb.isEmpty()) {
                    sb.append(",");
                }
                sb.append(recordOption);
            }
            message.addHeader(HEADER_OPTIONS, sb.toString());
        }
    }

    private long convertEOLToHeader(long eol) {
        return eol;
    }

    private Long convertEOLFromHeader(final Long eol) {
        return eol;
    }

    private CloseableHttpClient getHttpClient() {
        if (httpClient == null) {
            final RequestConfig requestConfig = RequestConfig.custom().
                    setConnectionRequestTimeout(Timeout.of(properties.getRequestTimeout())).
                    build();
            httpClient = HttpClients.custom().
                    setConnectionManager(getConnectionManager()).
                    setDefaultRequestConfig(requestConfig).
                    setUserAgent("RoStore Client 1.0").
                    build();
        }
        return httpClient;
    }

    protected HttpClientConnectionManager getConnectionManager() {
        SSLContext sslContext;

        try {
            if (trustManagers != null) {
                try {
                    sslContext= SSLContext.getInstance("TLS");
                    sslContext.init(null, trustManagers, null);
                } catch (NoSuchAlgorithmException | KeyManagementException e) {
                    sslContext = SSLContext.getDefault();
                }
            } else {
                sslContext = SSLContext.getDefault();
            }
        } catch (NoSuchAlgorithmException e) {
            throw new ClientException("Can't initialize SSL context", null, e);
        }

        final SSLConnectionSocketFactory sslFactory = new SSLConnectionSocketFactory(sslContext, HttpsSupport.getDefaultHostnameVerifier());
        final Registry<ConnectionSocketFactory> registry = RegistryBuilder.<ConnectionSocketFactory>create().register(URIScheme.HTTP.id,
                        PlainConnectionSocketFactory.getSocketFactory()).
                register(URIScheme.HTTPS.id, sslFactory).build();
        final PoolingHttpClientConnectionManager connectionManager = new PoolingHttpClientConnectionManager(
                registry, ManagedHttpClientConnectionFactory.builder().responseOutOfOrderStrategy(new MonitoringResponseOutOfOrderStrategy()).build()
        );
        ConnectionConfig connectionConfig = ConnectionConfig.custom().
                setConnectTimeout(Timeout.of(properties.getConnectTimeout())).
                build();
        connectionManager.setDefaultConnectionConfig(connectionConfig);
        connectionManager.setMaxTotal(properties.getMaxTotalConnections());
        connectionManager.setDefaultMaxPerRoute(properties.getMaxConnectionsPerRoute());
        return connectionManager;
    }

    private URI getUri(final String path) {
        return URI.create(properties.getBaseUrl() + path);
    }

    /**
     * Executes a get request on the rostore
     *
     * @param requestProperties request parameters
     * @param deserializer reads from server and transforms the data to object
     * @return object from the server
     * @param <T> type of the object
     */
    public <T> T get(final RequestProperties requestProperties, final Function<CloseableHttpResponse, T> deserializer) {
        final HttpGet get = new HttpGet(getUri(requestProperties.getPath()));
        return standardRequest(get, requestProperties, null, deserializer);
    }

    /**
     * Executes a delete command
     * @param requestProperties request parameters
     * @return {@code true} if delete request was successful
     */
    public boolean delete(final RequestProperties requestProperties) {
        final HttpDelete delete = new HttpDelete(getUri(requestProperties.getPath()));
        return standardRequest(delete, requestProperties, null, (response) -> {
            if (response.getCode() >= 200 && response.getCode() < 300) {
                return true;
            } else {
                throwClientException(delete.getMethod(), requestProperties, response);
                return false;
            }
        });
    }

    /**
     * Executes the request on rostore and return the response as byte array
     * @param requestProperties the request properties
     * @return the data
     */
    public byte[] getBytes(final RequestProperties requestProperties) {
        return get(requestProperties, (response) -> {
            try {
                return response.getEntity().getContent().readAllBytes();
            } catch (IOException e) {
                throw new ClientException("Call to \"" + requestProperties.getPath() + "\" has failed.", requestProperties, e);
            }
        });
    }

    /**
     * Executes the request on rostore and return the response as string (UTF-8)
     * @param requestProperties the request properties
     * @return the data as string
     */
    public String getString(final RequestProperties requestProperties) {
        return new String(getBytes(requestProperties), StandardCharsets.UTF_8);
    }

    /**
     * Executes a post request on the rostore
     *
     * @param requestProperties request parameters
     * @param data to post on the server
     * @param deserializer reads server's response and transforms the data to object
     * @return object from the server
     * @param <T> type of the object
     */
    public <T> T post(final RequestProperties requestProperties, final InputStream data, final Function<CloseableHttpResponse, T> deserializer) {
        HttpPost post = new HttpPost(getUri(requestProperties.getPath()));
        return standardRequest(post, requestProperties, data, deserializer);
    }

    /**
     * Executes a put request on the rostore
     *
     * @param requestProperties request parameters
     * @param data data to put on the server
     * @param deserializer reads server's response and transforms the data to object
     * @return object from the server
     * @param <T> type of the object
     */
    public <T> T put(final RequestProperties requestProperties, final InputStream data, final Function<CloseableHttpResponse, T> deserializer) {
        HttpPut put = new HttpPut(getUri(requestProperties.getPath()));
        return standardRequest(put, requestProperties, data, deserializer);
    }


    private <T> T standardRequest(final HttpUriRequestBase requestBase, final RequestProperties requestProperties, final InputStream data, final Function<CloseableHttpResponse, T> deserializer) {
        setHeaders(requestBase, requestProperties);
        if (data != null) {
            requestBase.setEntity(new InputStreamEntity(data, org.apache.hc.core5.http.ContentType.APPLICATION_OCTET_STREAM));
        }
        try (final CloseableHttpResponse response = getHttpClient().execute(requestBase)) {
            if (response.getCode() >= 200 && response.getCode() < 300) {
                return deserializer.apply(response);
            } else {
                if (response.getCode() == 409) {
                    throw new VersionConflictException("Wrong version", requestProperties);
                } else {
                    throwClientException(requestBase.getMethod(), requestProperties, response);
                    return null;
                }
            }
        } catch (final Exception e) {
            wrapException(requestBase.getMethod(), requestProperties, e);
            return null;
        }
    }

    @JsonIgnoreProperties(ignoreUnknown = true)
    private static class ErrorRepr {
        String message;

        public String getMessage() {
            return message;
        }

        public void setMessage(String message) {
            this.message = message;
        }
    }

    private static String getHeader(final CloseableHttpResponse response, final String headerName) {
        try {
            final Header header = response.getHeader(headerName);
            if (header != null) {
                return header.getValue();
            }
            return null;
        } catch (final ProtocolException e) {
            throw new WrappedClientException("Can't get header \"" + headerName + "\"", e);
        }
    }

    private void throwClientException(final String operation, final RequestProperties requestProperties, final CloseableHttpResponse response) {
        String contentType = getHeader(response, "Content-Type");
        String serverMessage = null;
        if (ContentType.JSON.getMediaType().equals(contentType)) {
            try {
                serverMessage = jsonMapper.toObject(response.getEntity().getContent(), ErrorRepr.class, "-").message;
            } catch (final IOException cl) {
                // swallow
            }
        }
        final String trackingId = getTrackingIdHeader(response);
        throw new ClientException("Can't execute \"" + operation + "\" on \"" + requestProperties.getPath() + "\". Server responded with status=\"" + response.getCode() + "\", error ref=\"" + trackingId + "\"", requestProperties, trackingId, serverMessage);
    }

    private void wrapException(final String operation, final RequestProperties requestProperties, final Exception exception) {
        if (exception instanceof ClientException) {
            throw (ClientException)exception;
        }
        if (exception instanceof VersionConflictException) {
            throw (VersionConflictException)exception;
        }
        if (exception instanceof WrappedClientException) {
            if (exception.getCause() != null) {
                throw new ClientException("Exception while executing \"" + operation + "\" on \"" + requestProperties.getPath() + "\": " + exception.getMessage(), requestProperties, exception.getCause());
            }
            throw new ClientException("Exception while executing \"" + operation + "\" on \"" + requestProperties.getPath() + "\": " + exception.getMessage(), requestProperties);
        }
        if (exception instanceof SSLHandshakeException) {
            throw new ClientException("SSL Exception while executing \"" + operation + "\" on \"" + requestProperties.getPath() + "\".", requestProperties, exception);
        }
        throw new ClientException("Exception while executing \"" + operation + "\" on \"" + requestProperties.getPath() + "\".", requestProperties, exception);
    }

    private static String getTrackingIdHeader(final CloseableHttpResponse response) {
        return getHeader(response, HEADER_TRACKING_ID);
    }

    /**
     * Extracts version header from the server response
     * @param response the server response
     * @return version
     */
    public static Long getVersionHeader(final CloseableHttpResponse response) {
        final String version = getHeader(response, HEADER_VERSION);
        if (version != null) {
            return Long.parseLong(version);
        }
        return null;
    }

    /**
     * Extracts eol header from the server response
     * @param response the server response
     * @return eol value
     */
    public Long getEOLHeader(final CloseableHttpResponse response) {
        final String eolHeader = getHeader(response, HEADER_EOL);
        if (eolHeader != null) {
            Long eol = Long.parseLong(eolHeader);
            if (eol != Utils.EOL_FOREVER) {
                return convertEOLFromHeader(eol);
            }
        }
        return null;
    }

    public static InputStream getInputStream(final CloseableHttpResponse response) {
        try {
            return response.getEntity().getContent();
        } catch (final IOException e) {
            throw new WrappedClientException("Can't read body of the response", e);
        }
    }

}
