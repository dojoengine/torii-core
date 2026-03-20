pub mod grpc_service;
pub mod sink;

pub mod proto {
    pub mod arcade {
        tonic::include_proto!("arcade.v1");

        pub const FILE_DESCRIPTOR_SET: &[u8] =
            tonic::include_file_descriptor_set!("arcade_descriptor");
    }
}

pub use grpc_service::ArcadeService;
pub use proto::arcade::FILE_DESCRIPTOR_SET;
pub use sink::ArcadeSink;
