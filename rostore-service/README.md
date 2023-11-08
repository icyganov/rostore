# ROSTORE 

RO-STORE is a simple highly efficient fail-tolerant key-value store with a direct persistence to a physical storage device. Key and value can be anything and are treated internally as a byte array. Every entry can have a special TTL (time to live) attribute that can specify when the key-value pair turns invalid. After invalidation the entry is removed from the underlying media.

More is here: https://ro-store.net/?page=home

# Install
To install the rostore the following should be done:

1. Unzip the archive to any convenient location
   ```bash
   unzip rostore.zip
   ```
2. Move to the rostore directory in the archive
   ```bash
    cd rostore
   ```
3. Open the rostore.properties and replace at least ROSTORE_ROOT_API_KEY and ROSTORE_PUBLIC_API_KEY with a specific uuid. You can use any common uuid generation tool. You can let ROSTORE_PUBLIC_API_KEY empty if you don't want an unauthorized access to the rostore.
   ```bash
    ROSTORE_ROOT_API_KEY=4e030824-08bf-4a0a-b6cb-bafa19406349
    ROSTORE_PUBLIC_API_KEY=2fbce488-b62d-41e1-9eb7-edb01e58e2b0
   ```
4. To start the rostore execute the following command:
   ```bash
   rostore start
   ```
After it all done the rostore swagger ui can be opened in the following location: http://localhost:8080/swagger-ui. 

Please, note that this connection is not secured by ssl and all information is transfered unencrypted through your network. To secure this connection you need to put the store under the apache server or configure it to use the ssl connection (see the respective lines in the rostore.properties).

The last thing that still should be done is a call to the POST /admin/store/create, which will initialize a store. The api-key provided to ROSTORE_ROOT_API_KEY should be provided in this call. Please, use Authorize button on swagger ui to enter the api-key.

The store can be used now.