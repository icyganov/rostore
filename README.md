# Rostore Parent Bundle

This is a rostore parent project, that contains several components allowing to start the Rostore server, get access to it over the Java Http Client and CLI.

# Modules

| Module                                       | Description                                                                                                                                                                          |
|----------------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------| 
| rostore-utils                                | Different utility classes that are used across other modules. |                                                                                                                        
| rostore-media                                | Core java classes to manage storage. Offers the mamory-mapping and high-speed access to the storage. Can be used to attach to Java project the RoStore capabilities in a native way. | 
| rostore-async                                | Add asynchroneous access capabilities to core rostore-media to allow highly parallel mode of operation with many concurrent threads accessing the core rostore.                      |
| [rostore-service](rostore-service/README.md) | Provides a stand-alone JAX-RS service on top of rostore-async. Service has batch files that allows start/stop the service.                                                           |
| rostore-cli                                  | Provides a command line interface to access the remotely running rostore-service                                                                                                     |
| rostore-client                               | Provides a simple Http Apache 5.x Client based interface to access the remote rostore-service.                                                                                       |
