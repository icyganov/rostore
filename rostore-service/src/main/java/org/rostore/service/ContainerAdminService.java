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
import org.rostore.entity.MemoryAllocationState;
import org.rostore.service.apikey.ApiKeyManager;
import org.rostore.entity.apikey.Permission;
import org.rostore.entity.media.ContainerMeta;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.EnumSet;
import java.util.logging.Logger;

@Path("/admin/container")
@Produces(MediaType.APPLICATION_JSON)
@SecuritySchemes({
        @SecurityScheme(securitySchemeName = "apiKey",
                in = SecuritySchemeIn.HEADER,
                type= SecuritySchemeType.APIKEY,
                apiKeyName = Headers.APIKEY_HEADER)
})
@Tags({
        @Tag(name="Administration - Container", description = "Collection of operations to manage containers")
})
@SecurityRequirement(name = "apiKey")
@RequestScoped
public class ContainerAdminService {

    private final static Logger logger = Logger.getLogger(ContainerAdminService.class.getName());

    @Inject
    private RoStoreAccessor roStoreAccessor;

    @Inject
    private ApiKeyManager apiKeyManager;

    @POST
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{container}")
    @Operation(summary="Creates a new container in the store", description = "Operation requires a store-wide create permission")
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
                            description = "Container has been created successfully.") })
    public Response createContainer(@PathParam("container") final String containerName, final ContainerMeta meta) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.CREATE));
        roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().create(containerName, meta);
        return Response.ok().build();
    }

    @DELETE
    @Consumes(MediaType.APPLICATION_JSON)
    @Path("/{container}")
    @Operation(summary="Delete an existing container in the store", description = "Operation requires a store-wide delete permission")
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
                            description = "Container removed successfully.") })
    public Response deleteContainer(@PathParam("container") final String containerName) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.DELETE));
        if (!roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().remove(containerName)) {
            throw new NotFoundException("Container \"" + containerName + "\" does not exist.");
        }
        return Response.ok().build();
    }


    @GET
    @Path("/{container}/meta")
    @Operation(summary="Retrieves a meta-information for a given container", description = "Operation requires a store-wide read permission")
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
                            description = "Meta data is returned",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = ContainerMeta.class))) })
    public Response getContainerMeta(@PathParam("container") final String containerName) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.READ));
        final ContainerMeta containerMeta = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(containerName).getContainer().getDescriptor().getContainerMeta();
        return Response.ok().entity(containerMeta).build();
    }

    @GET
    @Path("/{container}/space")
    @Operation(summary="Retrieves a space usage for a given container", description = "Operation requires a store-wide read permission")
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
                            description = "Container space usage info is returned",
                            content = @Content(mediaType = MediaType.APPLICATION_JSON,
                                    schema = @Schema(implementation = MemoryAllocationState.class))) })
    public Response getContainerSpace(@PathParam("container") final String containerName) {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.READ));
        final MemoryAllocationState memoryManagementState = roStoreAccessor.getAsyncContainerMedia().getAsyncContainers().get(containerName).getContainer().getMemoryAllocation();
        return Response.ok().entity(memoryManagementState).build();
    }
}
