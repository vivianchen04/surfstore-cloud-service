# surfstore-cloud-service
This is a cloud-based file storage service called SurfStore. SurfStore is a networked file storage application that is based on Dropbox, and lets you sync files to and from the “cloud”. You will implement the cloud service, and a client which interacts with your service via gRPC.

Multiple clients can concurrently connect to the SurfStore service to access a common, shared set of files. Clients accessing SurfStore “see” a consistent set of updates to files, but SurfStore does not offer any guarantees about operations across files, meaning that it does not support multi-file transactions (such as atomic move).
The SurfStore service is composed of the following two services:BlockStore(store content blocks and identifier) MetaStore (manages the metadata of files and the entire system)


--- update1: add consistent hashing algorithm to support scalable blockstore services. For reality, simulate having many block store servers, and implement a mapping algorithm that maps blocks to servers.
--- update2: modify metadata server to make it fault tolerant based on the RAFT protocol. 
