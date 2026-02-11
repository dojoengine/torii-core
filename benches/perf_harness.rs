use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use starknet::core::types::{EmittedEvent, Felt, U256};
use starknet::macros::selector;
use std::collections::HashMap;
use std::sync::Arc;
use tempfile::TempDir;
use tokio::runtime::Runtime;

use torii::etl::envelope::{Envelope, TypedBody};
use torii::etl::Decoder;
use torii::grpc::{proto::TopicSubscription, SubscriptionManager};
use torii_common::{blob_to_felt, blob_to_u256, felt_to_blob, u256_to_blob};
use torii_erc1155::decoder::Erc1155Decoder;
use torii_erc20::{
    decoder::Erc20Decoder,
    storage::{Erc20Storage, TransferData, TransferDirection},
};
use torii_erc721::decoder::Erc721Decoder;

#[derive(Debug)]
struct BenchBody {
    value: u64,
}

impl TypedBody for BenchBody {
    fn envelope_type_id(&self) -> torii::etl::envelope::TypeId {
        torii::etl::envelope::TypeId::new("bench.body")
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

fn runtime() -> Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to create tokio runtime")
}

fn make_erc20_transfer_event(seed: u64) -> EmittedEvent {
    EmittedEvent {
        from_address: Felt::from(0x1000 + seed),
        keys: vec![
            selector!("Transfer"),
            Felt::from(0x2000 + seed),
            Felt::from(0x3000 + seed),
        ],
        data: vec![Felt::from(10_000 + seed), Felt::ZERO],
        block_hash: None,
        block_number: Some(1_000_000 + seed),
        transaction_hash: Felt::from(0x4000 + seed),
    }
}

fn make_erc721_transfer_event(seed: u64) -> EmittedEvent {
    EmittedEvent {
        from_address: Felt::from(0x1100 + seed),
        keys: vec![
            selector!("Transfer"),
            Felt::from(0x2200 + seed),
            Felt::from(0x3300 + seed),
            Felt::from(seed),
            Felt::ZERO,
        ],
        data: vec![],
        block_hash: None,
        block_number: Some(2_000_000 + seed),
        transaction_hash: Felt::from(0x4400 + seed),
    }
}

fn make_erc1155_transfer_single_event(seed: u64) -> EmittedEvent {
    EmittedEvent {
        from_address: Felt::from(0x1200 + seed),
        keys: vec![
            selector!("TransferSingle"),
            Felt::from(0x2300 + seed),
            Felt::from(0x3400 + seed),
            Felt::from(0x4500 + seed),
        ],
        data: vec![
            Felt::from(seed),
            Felt::ZERO,
            Felt::from(100 + seed),
            Felt::ZERO,
        ],
        block_hash: None,
        block_number: Some(3_000_000 + seed),
        transaction_hash: Felt::from(0x4600 + seed),
    }
}

fn make_transfer_batch(size: usize, offset: u64) -> Vec<TransferData> {
    (0..size)
        .map(|i| {
            let i = i as u64;
            TransferData {
                id: None,
                token: Felt::from(0x5000 + ((i + offset) % 4)),
                from: Felt::from(0x6000 + ((i + offset) % 1024)),
                to: Felt::from(0x7000 + ((i + offset + 7) % 1024)),
                amount: U256::from_words((i + offset) as u128, 0),
                block_number: 4_000_000 + i + offset,
                tx_hash: Felt::from(0x8000 + i + offset),
                timestamp: None,
            }
        })
        .collect()
}

fn benchmark_common_conversions(c: &mut Criterion) {
    let mut group = c.benchmark_group("common_conversions");

    let felt = Felt::from_hex_unchecked(
        "0x0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
    );
    group.bench_function("felt_roundtrip", |b| {
        b.iter(|| {
            let blob = felt_to_blob(black_box(felt));
            black_box(blob_to_felt(black_box(&blob)))
        });
    });

    let small = U256::from(42u64);
    group.bench_function("u256_roundtrip_small", |b| {
        b.iter(|| {
            let blob = u256_to_blob(black_box(small));
            black_box(blob_to_u256(black_box(&blob)))
        });
    });

    let large = U256::from_words(u128::MAX - 7, u128::MAX - 11);
    group.bench_function("u256_roundtrip_large", |b| {
        b.iter(|| {
            let blob = u256_to_blob(black_box(large));
            black_box(blob_to_u256(black_box(&blob)))
        });
    });

    group.finish();
}

fn benchmark_decoders(c: &mut Criterion) {
    let rt = runtime();
    let mut group = c.benchmark_group("decoder_throughput");

    let erc20_decoder = Erc20Decoder::new();
    let erc20_event = make_erc20_transfer_event(1);
    group.bench_function("erc20_transfer_decode", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc20_decoder
                    .decode_event(black_box(&erc20_event))
                    .await
                    .expect("erc20 decode failed"),
            )
        });
    });

    let erc721_decoder = Erc721Decoder::new();
    let erc721_event = make_erc721_transfer_event(1);
    group.bench_function("erc721_transfer_decode", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc721_decoder
                    .decode_event(black_box(&erc721_event))
                    .await
                    .expect("erc721 decode failed"),
            )
        });
    });

    let erc1155_decoder = Erc1155Decoder::new();
    let erc1155_event = make_erc1155_transfer_single_event(1);
    group.bench_function("erc1155_transfer_single_decode", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc1155_decoder
                    .decode_event(black_box(&erc1155_event))
                    .await
                    .expect("erc1155 decode failed"),
            )
        });
    });

    let batch_events: Vec<_> = (0..256).map(make_erc20_transfer_event).collect();
    group.throughput(Throughput::Elements(batch_events.len() as u64));
    group.bench_function("erc20_decode_batch_256", |b| {
        b.to_async(&rt).iter(|| async {
            black_box(
                erc20_decoder
                    .decode(black_box(&batch_events))
                    .await
                    .expect("erc20 batch decode failed"),
            )
        });
    });

    group.finish();
}

fn benchmark_envelope_core(c: &mut Criterion) {
    let mut group = c.benchmark_group("etl_envelope");

    group.bench_function("envelope_create", |b| {
        b.iter(|| {
            let envelope = Envelope::new(
                black_box("bench-envelope".to_string()),
                Box::new(BenchBody { value: 7 }),
                HashMap::new(),
            );
            black_box(envelope)
        });
    });

    let envelope = Envelope::new(
        "bench-envelope".to_string(),
        Box::new(BenchBody { value: 99 }),
        HashMap::new(),
    );
    group.bench_function("envelope_downcast", |b| {
        b.iter(|| {
            let body = envelope
                .downcast_ref::<BenchBody>()
                .expect("downcast failed");
            black_box(body.value)
        });
    });

    group.finish();
}

fn benchmark_subscription_manager(c: &mut Criterion) {
    let mut group = c.benchmark_group("grpc_subscription_manager");

    let manager = Arc::new(SubscriptionManager::new());
    let (tx, _rx) = tokio::sync::mpsc::channel(1024);
    manager.register_client("bench-client".to_string(), tx);

    let topics_1 = vec![TopicSubscription {
        topic: "erc20.transfers".to_string(),
        filters: HashMap::from([("token".to_string(), "0x123".to_string())]),
        filter_data: None,
    }];

    let topics_3 = vec![
        TopicSubscription {
            topic: "erc20.transfers".to_string(),
            filters: HashMap::new(),
            filter_data: None,
        },
        TopicSubscription {
            topic: "erc721.transfers".to_string(),
            filters: HashMap::new(),
            filter_data: None,
        },
        TopicSubscription {
            topic: "erc1155.transfers".to_string(),
            filters: HashMap::new(),
            filter_data: None,
        },
    ];

    group.bench_function("update_subscriptions_1_topic", |b| {
        b.iter(|| {
            manager.update_subscriptions(
                black_box("bench-client"),
                black_box(topics_1.clone()),
                black_box(Vec::new()),
            );
        });
    });

    group.bench_function("update_subscriptions_3_topics", |b| {
        b.iter(|| {
            manager.update_subscriptions(
                black_box("bench-client"),
                black_box(topics_3.clone()),
                black_box(vec!["erc20.transfers".to_string()]),
            );
        });
    });

    group.finish();
}

fn benchmark_erc20_storage(c: &mut Criterion) {
    let mut group = c.benchmark_group("erc20_storage");

    let dir = TempDir::new().expect("failed to create tempdir");
    let db_path = dir.path().join("erc20_bench.db");
    let storage = Erc20Storage::new(db_path.to_str().expect("invalid db path"))
        .expect("failed to create erc20 storage");

    for size in [100usize, 1_000usize, 5_000usize] {
        let mut offset = 0u64;
        group.throughput(Throughput::Elements(size as u64));
        group.bench_with_input(
            BenchmarkId::new("insert_transfers_batch", size),
            &size,
            |b, &s| {
                b.iter(|| {
                    let batch = make_transfer_batch(s, offset);
                    offset += s as u64;
                    black_box(
                        storage
                            .insert_transfers_batch(black_box(&batch))
                            .expect("insert batch failed"),
                    )
                });
            },
        );
    }

    let preload = make_transfer_batch(20_000, 100_000);
    storage
        .insert_transfers_batch(&preload)
        .expect("preload insert failed");

    let wallet = Felt::from(0x6000 + 7);
    group.bench_function("get_transfers_filtered_wallet", |b| {
        b.iter(|| {
            black_box(
                storage
                    .get_transfers_filtered(
                        Some(wallet),
                        None,
                        None,
                        &[],
                        TransferDirection::All,
                        Some(4_000_000),
                        None,
                        None,
                        100,
                    )
                    .expect("wallet query failed"),
            )
        });
    });

    let token = Felt::from(0x5000 + 1);
    group.bench_function("get_transfers_filtered_token", |b| {
        b.iter(|| {
            black_box(
                storage
                    .get_transfers_filtered(
                        None,
                        None,
                        None,
                        &[token],
                        TransferDirection::All,
                        Some(4_000_000),
                        None,
                        None,
                        100,
                    )
                    .expect("token query failed"),
            )
        });
    });

    group.finish();
}

criterion_group!(
    benches,
    benchmark_common_conversions,
    benchmark_decoders,
    benchmark_envelope_core,
    benchmark_subscription_manager,
    benchmark_erc20_storage,
);
criterion_main!(benches);
