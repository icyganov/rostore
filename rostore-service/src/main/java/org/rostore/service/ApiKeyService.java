package org.rostore.service;

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
import org.rostore.Utils;
import org.rostore.entity.Record;
import org.rostore.entity.apikey.ApiKeyDefinition;
import org.rostore.entity.media.RecordOption;
import org.rostore.service.apikey.ApiKeyManager;
import org.rostore.entity.apikey.ApiKeyPermissions;
import org.rostore.entity.apikey.Permission;
import org.rostore.v2.container.DataWithRecord;
import org.rostore.entity.StringKeyList;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.EnumSet;
import java.util.logging.Logger;

@Path("/admin")
@Produces(MediaType.APPLICATION_JSON)
@SecuritySchemes({
        @SecurityScheme(securitySchemeName = "apiKey",
                in = SecuritySchemeIn.HEADER,
                type= SecuritySchemeType.APIKEY,
                apiKeyName = Headers.APIKEY_HEADER)
})
@Tags({
        @Tag(name="Administration - Api-Keys", description = "Collection of operations to manage the api-keys")
})
@SecurityRequirement(name = "apiKey")
@RequestScoped
public class ApiKeyService {

    private final static Logger logger = Logger.getLogger(ApiKeyService.class.getName());

    @Inject
    private ApiKeyManager apiKeyManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/api-key")
    @Operation(summary="Creates a new Api-Key with a set of given permissions", description = "Operation requires a store-wide grant permission")
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
                            description = "Api-Key created successfully.",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ApiKeyDefinition.class)))
            })
    public Response createApiKey(@HeaderParam(Headers.OPTIONS_HEADER) final String options,
                                 @HeaderParam(Headers.TTL_HEADER) final Long ttl,
                                 @HeaderParam(Headers.EOL_HEADER) final Long unixEOL,
                                 final ApiKeyPermissions permissions) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.GRANT));
        final Record record = new Record().ttlOrUnitEol(ttl, unixEOL).version(Utils.VERSION_START).addOptions(RecordOption.parse(options));
        final DataWithRecord<ApiKeyDefinition> apiKeyDefinitionData = apiKeyManager.create(permissions, record);
        final Response.ResponseBuilder rb = Response.ok(apiKeyDefinitionData.getData());
        Headers.toHeaders(rb, apiKeyDefinitionData.getRecord());
        return rb.build();
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/api-key/{request-api-key}")
    @Operation(summary="Updates an existing Api-Key with a set of given permissions", description = "Operation requires a store-wide grant permission")
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
                            description = "Api-Key has been updated successfully.",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ApiKeyDefinition.class)))
            })
    public Response updateApiKey(@PathParam("request-api-key") final String requestApiKey,
                                 @HeaderParam(Headers.OPTIONS_HEADER) final String options,
                                 @HeaderParam(Headers.TTL_HEADER) final Long ttl,
                                 @HeaderParam(Headers.EOL_HEADER) final Long unixEOL,
                                 @HeaderParam(Headers.VERSION_HEADER) final Long versionHeader,
                                 final ApiKeyPermissions permissions) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.GRANT));
        long version = versionHeader == null ? Utils.VERSION_UNDEFINED : versionHeader;
        final ApiKeyDefinition apiKeyDefinition = new ApiKeyDefinition(requestApiKey, permissions);
        final Record record = new Record().ttlOrUnitEol(ttl, unixEOL).version(version).addOptions(RecordOption.parse(options));
        apiKeyManager.update(apiKeyDefinition, record);
        final Response.ResponseBuilder rb = Response.ok(apiKeyDefinition);
        Headers.toHeaders(rb, record);
        return rb.build();
    }

    @GET
    @Path("/api-key/repair-root")
    @Operation(summary="Repairs a broken root Api-Key entry", description = "Operation requires a store-wide root permission")
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
                            description = "Root is repaired successfully.",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ApiKeyDefinition.class)))
            })
    public Response repairRoot() {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.SUPER));
        final DataWithRecord<ApiKeyDefinition> dataWithRecord = apiKeyManager.repairRootApiKeyEntry();
        final Response.ResponseBuilder rb = Response.ok(dataWithRecord.getData());
        Headers.toHeaders(rb, dataWithRecord.getRecord());
        return rb.build();
    }

    @GET
    @Path("/api-key/{request-api-key}")
    @Operation(summary="Returns a set of permissions associated with an Api-Key", description = "Operation requires a store-wide read permission")
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
                            responseCode = "404",
                            description = "The API-Key not found or not access",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ErrorRepresentation.class))
                    ),
                    @APIResponse(
                            responseCode = "200",
                            description = "The api-key definition",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                schema = @Schema(implementation = ApiKeyDefinition.class)))
            })
    public Response getApiKey(@PathParam("request-api-key") final String requestApiKey,
                              @HeaderParam(Headers.VERSION_HEADER) final Long versionHeader) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.READ));
        final DataWithRecord<ApiKeyDefinition> apiKeyDefinitionData = apiKeyManager.get(requestApiKey);
        if (apiKeyDefinitionData == null) {
            throw new NotFoundException("No api-key \"" + requestApiKey + "\" found or expired.");
        }
        final Response.ResponseBuilder rb = Response.ok(apiKeyDefinitionData.getData());
        Headers.toHeaders(rb, apiKeyDefinitionData.getRecord());
        return rb.build();
    }

    @GET
    @Path("/api-key/list")
    @Operation(summary="Returns a list of Api-Keys in the store", description = "Operation requires a store-wide list permission")
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
                            description = "The api-key list",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = StringKeyList.class)))
            })
    public Response listApiKey(@QueryParam("start-api-key") final String startApiKey) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.LIST));
        StringKeyList stringKeyList = apiKeyManager.list(startApiKey);
        return Response.ok().entity(stringKeyList).build();
    }
}
