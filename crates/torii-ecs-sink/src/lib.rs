pub mod grpc_service;
pub mod sink;

pub mod proto {
    pub mod world {
        tonic::include_proto!("world");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("world_descriptor");
    }

    pub mod types {
        tonic::include_proto!("types");
    }
}

pub use grpc_service::EcsService;
pub use proto::world::FILE_DESCRIPTOR_SET;
pub use sink::EcsSink;
