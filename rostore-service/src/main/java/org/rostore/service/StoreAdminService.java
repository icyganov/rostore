package org.rostore.service;

import io.quarkus.runtime.Quarkus;
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeIn;
import org.eclipse.microprofile.openapi.annotations.enums.SecuritySchemeType;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
import org.eclipse.microprofile.openapi.annotations.security.SecurityScheme;
import org.eclipse.microprofile.openapi.annotations.security.SecuritySchemes;
import org.eclipse.microprofile.openapi.annotations.tags.Tag;
import org.eclipse.microprofile.openapi.annotations.tags.Tags;
import org.rostore.entity.MemoryAllocationState;
import org.rostore.entity.media.RoStoreProperties;
import org.rostore.entity.media.Version;
import org.rostore.service.apikey.ApiKeyManager;
import org.rostore.entity.apikey.Permission;
import org.rostore.service.apikey.PermissionDeniedException;
import org.rostore.v2.container.async.AsyncContainerMediaProperties;
import org.rostore.v2.media.MediaProperties;

import jakarta.enterprise.context.RequestScoped;
import jakarta.inject.Inject;
import jakarta.ws.rs.*;
import jakarta.ws.rs.core.MediaType;
import jakarta.ws.rs.core.Response;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Logger;

@Path("/admin/store")
@Produces(MediaType.APPLICATION_JSON)
@SecuritySchemes({
        @SecurityScheme(securitySchemeName = "apiKey",
                in = SecuritySchemeIn.HEADER,
                type= SecuritySchemeType.APIKEY,
                apiKeyName = Headers.APIKEY_HEADER)
})
@Tags({
        @Tag(name="Administration - Media", description = "Collection of operations to manage the underlying media")
})
@SecurityRequirement(name = "apiKey")
@RequestScoped
public class StoreAdminService {

    private final static Logger logger = Logger.getLogger(StoreAdminService.class.getName());

    private static final Version version = new Version("2.1");

    @Inject
    private RoStoreAccessor roStoreAccessor;

    @Inject
    private ApiKeyManager apiKeyManager;

    @GET
    @Path("/shutdown")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary="Shutdowns this store", description = "This operation can only start a root user")
    public Response shutdownStore() {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.SUPER));
        Quarkus.asyncExit();
        return Response.ok().build();
    }

    @GET
    @Path("/ping")
    @Produces(MediaType.TEXT_PLAIN)
    @Operation(summary="Check for availability", description = "This operation can only start a root user")
    public Response pingStore() {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.SUPER));
        return Response.ok("pong").build();
    }

    @GET
    @Path("/version")
    @Produces(MediaType.APPLICATION_JSON)
    @Operation(summary="Get version", description = "This operation is not protected")
    public Response version() {
        return Response.ok(version).build();
    }

    @POST
    @Path("/create")
    @Consumes(MediaType.APPLICATION_JSON)
    @Operation(summary="Initializes a new store", description = "This operation can only start a root user")
    public Response initStore(final RoStoreProperties roStoreProperties) {
        if (!apiKeyManager.isRootApiKey()) {
            throw new PermissionDeniedException("This operation can only be executed with root api key.");
        }
        final AsyncContainerMediaProperties asyncContainerMediaProperties = new AsyncContainerMediaProperties();
        asyncContainerMediaProperties.setMediaProperties(MediaProperties.from(roStoreProperties.getMediaProperties()));
        asyncContainerMediaProperties.setContainerListProperties(roStoreProperties.getContainerListProperties());
        roStoreAccessor.create(asyncContainerMediaProperties);
        return Response.ok().build();
    }

    @GET
    @Path("/space")
    @Operation(summary="Provides information on store space usage", description = "Operation requires a store-wide read permission")
    public Response storeSpace() {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.READ));
        final Map<String, Object> ret= new HashMap<>();
        final MemoryAllocationState mms = MemoryAllocationState.store(roStoreAccessor.getAsyncContainerMedia().getMedia().getMemoryManagement());
        ret.put("media", mms);
        ret.put("properties", roStoreAccessor.getAsyncContainerMedia().getMedia().getMediaProperties());
        return Response.ok(ret).build();
    }

    @GET
    @Path("/mapper-properties")
    @Operation(summary="Provides information on store space usage", description = "Operation requires a store-wide read permission")
    public Response mapperProperties() {
        apiKeyManager.checkStorePermission(EnumSet.of(Permission.READ));
        return Response.ok(roStoreAccessor.getAsyncContainerMedia().getMedia().getMediaProperties().getMapperProperties()).build();
    }
}
