package org.rostore.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.undertow.httpcore.StatusCodes;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.security.SecuritySchemes;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.openapi.annotations.tags.Tags;
import org.jboss.logging.MDC;
import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.entity.media.RecordOption;
import org.rostore.entity.apikey.ApiKeyDefinition;
import org.rostore.service.apikey.ApiKeyManager;
import org.rostore.entity.apikey.Permission;
import org.rostore.service.apikey.PermissionDeniedException;
import org.rostore.v2.container.async.AsyncContainer;
import org.rostore.v2.container.async.AsyncListener;
import org.rostore.v2.container.async.AsyncStatus;
import org.rostore.v2.container.async.AsyncStream;
import org.rostore.entity.StringKeyList;
import org.rostore.v2.keys.KeyList;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.ServletInputStream;
import jakarta.servlet.ServletOutputStream;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import jakarta.ws.rs.*;
import jakarta.ws.rs.container.AsyncResponse;
import jakarta.ws.rs.container.Suspended;
import jakarta.ws.rs.core.Context;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.stream.Collectors;

@Path("/")
@RequestScoped
@SecuritySchemes({
        @SecurityScheme(securitySchemeName = "apiKey",
                in = SecuritySchemeIn.HEADER,
                type= SecuritySchemeType.APIKEY,
                apiKeyName = Headers.APIKEY_HEADER)
})
@SecurityRequirement(name = "apiKey")
@Tags({
        @Tag(name="Operations - Key-Value Store")
})
public class KeyValueService {

    private final static Logger logger = Logger.getLogger(KeyValueService.class.getName());

    private static final int MAX_NUMBER_OF_KEYS = 20;
    private static final int MAX_SIZE_OF_KEYS = 100000;

    @Inject
    private RoStoreAccessor roStoreAccessor;

    @Inject
    private ApiKeyManager apiKeyManager;

    @Inject
    private ObjectMapper objectMapper;

    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes("*/*")
    @Path("/container/{container}/key/{key}")
    @APIResponses(
            value = {
                    @APIResponse(
                            responseCode = "403",
                            description = "No access",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "409",
                            description = "Version conflict",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))),
                    @APIResponse(
                            responseCode = "507",
                            description = "Quota exceeded",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))),
                    @APIResponse(
                            responseCode = "500",
                            description = "Internal Error",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "200",
                            description = "Key-value has been stored successfully.") })
    @Operation(summary="Posts a key-value pair to the container")
    public void put(@PathParam("container") final String containerName,
                        @PathParam("key") final String key,
                        @HeaderParam(Headers.OPTIONS_HEADER) final String options,
                        @HeaderParam(Headers.TTL_HEADER) final Long ttl,
                        @HeaderParam(Headers.EOL_HEADER) final Long unixEol,
                        @HeaderParam(Headers.VERSION_HEADER) final Long versionHeader,
                        @Context HttpServletResponse servletResponse,
                        @Context HttpServletRequest servletRequest) throws IOException {
        if (ApiKeyManager.APIKEY_CONTAINER_NAME.equals(containerName)) {
            throw new PermissionDeniedException("Can't update container " + ApiKeyManager.APIKEY_CONTAINER_NAME + ". Use admin service for this operation.");
        }
        apiKeyManager.checkContainerPermission(containerName, EnumSet.of(Permission.WRITE));
        assert servletRequest.isAsyncStarted();

        final ServletInputStream inputStream = servletRequest.getInputStream();
        final ServletOutputStream outputStream = servletResponse.getOutputStream();

        final AsyncContainer asyncContainer = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(containerName);
        if (asyncContainer == null) {
            final Object o = MDC.get(Headers.TRACKING_ID_HEADER);
            final String trackingId = o != null ? o.toString() : "-";
            servletResponse.setStatus(StatusCodes.NOT_FOUND);
            final ErrorRepresentation errorRepresentation = new ErrorRepresentation("No container \"" + containerName + "\" found.", trackingId);
            outputStream.write(objectMapper.writeValueAsBytes(errorRepresentation));
            outputStream.close();
            inputStream.close();
            return;
        }
        long version = versionHeader == null ? Utils.VERSION_UNDEFINED : versionHeader;
        final Record record = new Record().ttlOrUnitEol(ttl, unixEol).version(version).addOptions(RecordOption.parse(options));

        AsyncStream<InputStream> asyncStream = AsyncStream.wrapBlocking(servletRequest.getInputStream(), new AsyncListener() {
            @Override
            public void record(final Record record) {
                servletResponse.setStatus(Response.Status.OK.getStatusCode());
                Headers.toHeaders(servletResponse, record);
            }

            @Override
            public void error(final Exception e) {
            }

            @Override
            public void status(final AsyncStatus asyncStatus) {
            }
        });

        asyncContainer.putAsync(0, key.getBytes(), asyncStream, record);
        asyncStream.get();
    }

    @DELETE
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/container/{container}/key/{key}")
    @Operation(summary="Deletes permanently a key-value pair from the container")
    @APIResponses(
            value = {
                    @APIResponse(
                            responseCode = "403",
                            description = "No access",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "409",
                            description = "Version conflict",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))),
                    @APIResponse(
                            responseCode = "500",
                            description = "Internal Error",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "200",
                            description = "Key-value has been deleted successfully.") })
    public Response delete(@PathParam("container") final String containerName,
                           @PathParam("key") final String key,
                           @HeaderParam(Headers.OPTIONS_HEADER) final String options,
                           @HeaderParam(Headers.VERSION_HEADER) final Long versionHeader) {
        if (ApiKeyManager.APIKEY_CONTAINER_NAME.equals(containerName)) {
            throw new PermissionDeniedException("Can't delete api key in container " + ApiKeyManager.APIKEY_CONTAINER_NAME + ". Use admin service for this operation.");
        }
        apiKeyManager.checkContainerPermission(containerName, EnumSet.of(Permission.DELETE));
        final AsyncContainer asyncContainer = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(containerName);
        if (asyncContainer == null) {
            throw new NotFoundException("No container \"" + containerName + "\" found.");
        }
        final Record record = new Record().addOptions(RecordOption.parse(options));
        if (versionHeader != null) {
            record.version(versionHeader);
        }
        Response.ResponseBuilder rb = Response.ok();
        boolean result = asyncContainer.remove(0, key.getBytes(), record);
        if (result == false) {
            throw new NotFoundException("No key \"" + key + "\" found in container \"" + containerName + "\".");
        }
        return rb.build();
    }

    @GET
    @Produces({MediaType.APPLICATION_OCTET_STREAM})
    @Path("/container/{container}/key/{key}")
    @Operation(summary="Retrieves a value by the given key from the container")
    @APIResponses(
            value = {
                    @APIResponse(
                            responseCode = "403",
                            description = "No access",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "404",
                            description = "The key not found",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "500",
                            description = "Internal Error",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "200",
                            description = "Value for the key",
                            content = @Content(mediaType = MediaType.APPLICATION_OCTET_STREAM))
            })
    public void get(@PathParam("container") final String containerName,
                        @PathParam("key") final String key,
                        @Context HttpServletResponse servletResponse,
                        @Context HttpServletRequest servletRequest) throws IOException {

        if (ApiKeyManager.APIKEY_CONTAINER_NAME.equals(containerName)) {
            throw new PermissionDeniedException("Can't get api key in container " + ApiKeyManager.APIKEY_CONTAINER_NAME + ". Use admin service for this operation.");
        }
        apiKeyManager.checkContainerPermission(containerName, EnumSet.of(Permission.READ));
        final AsyncContainer asyncContainer = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(containerName);
        if (asyncContainer == null) {
            throw new NotFoundException("No container \"" + containerName + "\" found or expired.");
        }

        AsyncStream<OutputStream> as = AsyncStream.wrapBlocking(servletResponse.getOutputStream(), new AsyncListener() {
            @Override
            public void record(Record record) {
                servletResponse.setStatus(Response.Status.OK.getStatusCode());
                Headers.toHeaders(servletResponse, record);
            }

            @Override
            public void error(Exception e) {
            }

            @Override
            public void status(AsyncStatus asyncStatus) {
                if (asyncStatus.isFinished()) {
                    if (asyncStatus == AsyncStatus.CANCELED) {
                        servletResponse.setStatus(Response.Status.NOT_FOUND.getStatusCode());
                        servletResponse.addHeader("Content-Type", MediaType.APPLICATION_JSON);
                        final RestError restError = RestError.convert(new NotFoundException("No key \"" + key + "\" found in container \"" + containerName + "\"."));
                        try {
                            servletResponse.getOutputStream().write(objectMapper.writeValueAsBytes(restError.getErrorRepresentation()));
                        } catch (IOException e) {
                            //throw new RuntimeException(e);
                        }
                    }
                    try {
                        servletResponse.getOutputStream().close();
                    } catch (IOException e) {
                        //throw new RuntimeException(e);
                    }
                }
            }
        });

        asyncContainer.getAsync(0, key.getBytes(StandardCharsets.UTF_8), as);
        as.get();
    }

    @GET
    @Produces({MediaType.APPLICATION_JSON})
    @Path("/container/{container}/keys")
    @Operation(summary="Lists keys from the container", description = "The keys are not processed in the alphabetic order. The order depends on the number of shards. Internally, each shard is processed independently.")
    @APIResponses(
            value = {
                    @APIResponse(
                            responseCode = "403",
                            description = "No access",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "500",
                            description = "Internal Error",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),

                    @APIResponse(
                            responseCode = "200",
                            description = "List of keys",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON))
            })
    public Response getKeys(@PathParam("container") final String containerName, @QueryParam("start-with-key") final String startWithKey, @QueryParam("continuation-key") final String continuationKey) {
        apiKeyManager.checkContainerPermission(containerName, EnumSet.of(Permission.LIST));
        final AsyncContainer asyncSegmentPool = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(containerName);
        if (asyncSegmentPool == null) {
            throw new NotFoundException("No container " + containerName + " found or expired.");
        }
        final KeyList keyList = asyncSegmentPool.list(1, Utils.getBytes(startWithKey), Utils.getBytes(continuationKey), MAX_NUMBER_OF_KEYS, MAX_SIZE_OF_KEYS);
        if (keyList.getSize() == 0) {
            StringBuilder sb = new StringBuilder("No keys are found in container \"").
                    append(containerName).append("\"");
            if (startWithKey != null || continuationKey != null) {
                sb.append(". Parameters: ");
                if (startWithKey != null) {
                    sb.append("start-with-key=\"").
                            append(startWithKey).
                            append("\"");
                    if (continuationKey != null) {
                        sb.append(", ");
                    }
                }
                if (continuationKey != null) {
                    sb.append("continuation-key=\"").
                            append(continuationKey).
                            append("\"");
                }
                sb.append(".");
            }
            throw new NotFoundException(sb.toString());
        }
        final StringKeyList skl = new StringKeyList(keyList);
        return Response.ok(skl).build();
    }

    @GET
    @Produces(MediaType.APPLICATION_JSON)
    @Path("/container/list")
    @Operation(summary="Lists all the container the user has access to")
    @APIResponses(
            value = {
                    @APIResponse(
                            responseCode = "403",
                            description = "No access",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "500",
                            description = "Internal Error",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "200",
                            description = "List of containers",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON))
            })
    public Response containerList(@PathParam("key") final String key) {
        ApiKeyDefinition apiKeyDefinition = apiKeyManager.getAndCheckKey();
        List<String> containers;
        if (apiKeyDefinition.getApiKeyPermissions().getStorePermissions().contains(Permission.LIST) ||
                apiKeyDefinition.getApiKeyPermissions().getStorePermissions().contains(Permission.SUPER)) {
            containers = roStoreAccessor.listAllContainers();
        } else {
            Set<String> set = apiKeyDefinition.getApiKeyPermissions().getContainerPermissions().entrySet().stream().filter(e -> e.getValue().contains(Permission.LIST)).map(e -> e.getKey()).collect(Collectors.toSet());
            containers = new ArrayList<>(set);
            Collections.sort(containers);
        }
        return Response.ok(containers).build();
    }

}
