# ExternalContractRegistered Implementation Plan

## Overview
We need to handle the Dojo world event `ExternalContractRegistered` in the current ETL architecture and begin indexing the registered contract without restarting the process. In the old Torii processor model, this event only registered contract metadata and seeded indexing state; in the new architecture, the same behavior spans decoder output, contract-to-decoder routing, extractor enrollment, and sink-side contract classification.

## Goals
- Detect and decode `ExternalContractRegistered` emitted by a Dojo world.
- Start extracting events for the registered contract from the registration block onward.
- Route the new contract to the correct decoder set without requiring a restart.
- Preserve correct contract typing in ECS/storage APIs once external contract events arrive.
- Keep the flow idempotent across retries and restarts.

## Non-Goals
- Backfill external contract history from before the registration block.
- Support arbitrary contract kinds beyond decoders/sinks that are actually registered in the process.
- Migrate the whole Dojo indexer to `GlobalEventExtractor` as part of this change.
- Recreate the full old processor framework.

## Assumptions and Constraints
- The event payload is still equivalent to the old processor input: namespace, contract name, instance name, contract selector, class hash, contract address, and block number.
- Old behavior used `block_number - 1` as stored head, which means new indexing effectively starts at the registration block. In the new extractor model, the equivalent is `from_block = registration_block`.
- The current Dojo path is built around a static `EventExtractor` contract list in [`bins/torii-introspect-bin/src/main.rs`](/Users/valentindosimont/www/c7e/torii-core/bins/torii-introspect-bin/src/main.rs), so “start indexing” now requires runtime extractor enrollment, not just metadata persistence.
- `EcsSink` currently records every raw event contract as `WORLD`, which will be wrong for dynamically indexed external contracts.
- External contract indexing should be gated somehow. The old code used `should_index_external_contract(namespace, instance_name)`; the current binary has no equivalent yet.

## Requirements

### Functional
- Decode `ExternalContractRegistered` from world events in `torii-dojo`.
- Produce a typed control signal that downstream code can act on.
- Map the registered contract to the appropriate decoder IDs based on `contract_name`.
- Persist the decoder mapping so restarts preserve the registration.
- Enroll the contract into the event extractor starting at the registration block.
- Avoid duplicate registration work if the same event is replayed.
- Record the registered contract with the right `ContractType` in ECS storage.

### Non-Functional
- No missing events from the registration block because of cursor advancement.
- No restart required after registration.
- Minimal changes to existing sink APIs.
- Safe behavior when the registered contract type is unsupported or no matching decoder is installed.

## Technical Design

### Data Model
- Reuse `engine.contract_decoders` for persisted contract-to-decoder routing.
- Reuse `engine.extractor_state` for per-contract event extractor progress, but add the ability for `EventExtractor` to discover dynamically added contract states.
- Add an in-memory contract-type registry shared with `EcsSink` so external contracts are not later overwritten as `WORLD`.

### API Design
- Add a new typed envelope/body in `torii-dojo`, for example `dojo.external_contract_registered`.
- Add a command, for example `RegisterExternalContractCommand`, carrying:
  - world address
  - contract address
  - contract name / resolved contract type
  - namespace
  - instance name
  - registration block
  - resolved decoder IDs
- Add a command handler that:
  - updates `engine.contract_decoders`
  - writes dynamic extractor state for the contract
  - updates the shared contract-type registry

### Architecture
```text
World event (ExternalContractRegistered)
  -> DojoDecoder
  -> control envelope: dojo.external_contract_registered
  -> sink/handler path
  -> RegisterExternalContractCommand
  -> command handler
     -> persist contract_decoders
     -> add dynamic event extractor state (from registration block)
     -> update shared contract type registry
  -> next ETL cycle
     -> EventExtractor sees new contract state
     -> fetches events from external contract
     -> DecoderContext routes events using persisted mapping
     -> token / other sinks process them normally
```

### Recommended Fit
- Handle event parsing in `torii-dojo`, not in `EcsSink`.
  - Reason: this is a Dojo-specific world event, and the decoder is already the component that understands Dojo event selectors.
- Trigger extractor enrollment via command bus, not direct sink-to-extractor mutation.
  - Reason: this keeps ETL control-plane work asynchronous and avoids coupling sinks to a concrete extractor instance.
- Extend `EventExtractor` to load dynamically added contract states from `EngineDb`.
  - Reason: this works with the existing extractor persistence model and survives restart.

### Alternative Considered
- Switch the Dojo indexer to `GlobalEventExtractor` and rely only on runtime decoder routing.
  - Pros: no dynamic extractor enrollment needed.
  - Cons: much larger behavior and cost change; not aligned with the current targeted-contract design of `torii-introspect-bin`.

---

## Implementation Plan

### Serial Dependencies (Must Complete First)

These tasks create foundations that other work depends on. Complete in order.

#### Phase 0: Control-Plane Foundations
**Prerequisite for:** All subsequent phases

| Task | Description | Output |
|------|-------------|--------|
| 0.1 | Add a typed `ExternalContractRegistered` control body in `torii-dojo` and teach `DojoDecoder` to emit it instead of treating the selector as unknown. | Decoder can parse the new world event into a control envelope. |
| 0.2 | Add a shared contract-type mapping helper (`contract_name` -> `ContractType` + decoder IDs). | Single source of truth for dynamic routing and ECS classification. |
| 0.3 | Add `EngineDb` support for discovering dynamic event extractor states, or another minimal persisted lookup used by `EventExtractor`. | Extractor can load runtime-added contracts on startup and during steady state. |

---

### Parallel Workstreams

These workstreams can be executed independently after Phase 0.

#### Workstream A: Dynamic Extractor Enrollment
**Dependencies:** Phase 0  
**Can parallelize with:** Workstreams B, C

| Task | Description | Output |
|------|-------------|--------|
| A.1 | Extend `EventExtractor` so it can hydrate unknown contracts from persisted state, not only its static config list. | Runtime-added contracts become fetch targets without restart. |
| A.2 | Ensure newly added contracts start from the registration block inclusive and follow head (`to_block = u64::MAX`). | No missed same-block follow-up events. |
| A.3 | Make extractor refresh dynamic contracts on each cycle or before each extraction attempt. | Newly registered contracts begin indexing promptly. |

#### Workstream B: Runtime Registration Command Path
**Dependencies:** Phase 0  
**Can parallelize with:** Workstreams A, C

| Task | Description | Output |
|------|-------------|--------|
| B.1 | Add `RegisterExternalContractCommand` and a dedicated `CommandHandler`. | Background registration work can be triggered from sinks. |
| B.2 | Persist `contract -> decoder_ids` into `engine.contract_decoders` and update the shared registry cache. | `DecoderContext` routes future external contract events correctly. |
| B.3 | Persist extractor state for the new contract starting at the registration block. | Extractor enrollment survives restarts and replay. |
| B.4 | Handle unsupported contract types or missing installed decoders by logging and skipping extractor enrollment. | Safe failure mode instead of partial broken indexing. |

#### Workstream C: Sink Classification and Wiring
**Dependencies:** Phase 0  
**Can parallelize with:** Workstreams A, B

| Task | Description | Output |
|------|-------------|--------|
| C.1 | Extend the Dojo/ECS sink path to consume the new control envelope and dispatch the registration command. | External registrations trigger runtime indexing. |
| C.2 | Replace `EcsSink`’s hardcoded `ContractType::World` classification with a lookup against shared contract-type state. | External contract events are stored as ERC20/ERC721/ERC1155 instead of WORLD. |
| C.3 | Seed that shared contract-type state from both static CLI targets and runtime registrations. | Static and dynamic contracts use the same classification path. |
| C.4 | Add config gating for external contract indexing, ideally restoring old whitelist semantics or a simpler opt-in flag. | Runtime registration is explicit and controlled. |

---

### Merge Phase

After parallel workstreams complete, these tasks integrate the work.

#### Phase N: Integration
**Dependencies:** Workstreams A, B, C

| Task | Description | Output |
|------|-------------|--------|
| N.1 | Wire the new command handler and shared registries into `bins/torii-introspect-bin`. | Binary boots with dynamic external contract registration enabled. |
| N.2 | Ensure decoder mappings, extractor state, and ECS contract typing all agree for dynamically added contracts. | End-to-end runtime registration flow works coherently. |
| N.3 | Verify logs and metrics for registration success, duplicate replay, unsupported contract types, and extractor enrollment. | Operable runtime behavior. |

---

## Testing and Validation

- Unit test `DojoDecoder` on an `ExternalContractRegistered` raw event.
- Unit test contract-name resolution to `(ContractType, DecoderId set)`.
- Unit test command handler idempotence when the same contract is registered twice.
- Unit test `EventExtractor` dynamic contract discovery from persisted state.
- Unit test `EcsSink` contract-type lookup so external contract events do not overwrite type to `WORLD`.
- Add an integration test with a synthetic sequence:
  - world emits `ExternalContractRegistered` at block `N`
  - command persists mapping and extractor state
  - extractor later fetches external contract events from block `N`
  - token sink processes those events
  - ECS contract record shows the external contract type, not `WORLD`

## Rollout and Migration

- Gate the feature behind explicit config at first.
- Preserve existing behavior for static contract lists when the feature is disabled.
- On restart, rely on persisted `contract_decoders` and extractor state so dynamically registered contracts resume normally.
- Rollback path: disable the feature flag and ignore control envelopes; existing static indexing remains unchanged.

## Verification Checklist

- `cargo test -p torii-dojo`
- `cargo test -p torii-ecs-sink`
- `cargo test -p torii -- extractor::event`
- Synthetic end-to-end test for dynamic external contract registration
- Manual verification in `torii-introspect-bin`:
  - start with only a world contract configured
  - emit `ExternalContractRegistered`
  - confirm extractor state appears for the external contract
  - confirm subsequent external contract events are fetched and decoded
  - confirm ECS contract query reports the expected `ContractType`

## Risk Assessment

| Risk | Likelihood | Impact | Mitigation |
|------|------------|--------|------------|
| Registered contract starts indexing too late and misses same-block events | Med | High | Start dynamic extractor state at the registration block, not the next block. |
| ECS contract type gets overwritten back to `WORLD` on raw-event ingest | High | High | Replace hardcoded classification with shared contract-type lookup before enabling the feature. |
| Unsupported contract names produce partial state | Med | Med | Resolve type before persistence; skip extractor enrollment if no installed decoder matches. |
| EventExtractor never sees dynamically added contracts | Med | High | Add explicit extractor refresh/load path plus tests around persisted state discovery. |
| Replay of the registration event creates duplicate work | High | Med | Keep handler idempotent via upsert semantics in `EngineDb` and in-memory maps. |

## Open Questions

- [ ] Do we want strict parity with the old `should_index_external_contract(namespace, instance_name)` whitelist, or is a coarse feature flag enough?
- [ ] Which `contract_name` strings should be considered supported initially: only `ERC20`/`ERC721`/`ERC1155`, or also `UDC`/others?
- [ ] Should unsupported registrations still appear in ECS contract listings as `OTHER`, even if no extractor enrollment happens?
- [ ] Do we want this feature only in `torii-introspect-bin`, or as reusable core plumbing for any Torii app using `EventExtractor`?

## Decision Log

| Decision | Rationale | Alternatives Considered |
|----------|-----------|------------------------|
| Decode `ExternalContractRegistered` in `torii-dojo` | The decoder already owns Dojo world event semantics. | Parse raw events directly in `EcsSink`. |
| Use a command-handler path for runtime registration | Keeps control-plane mutation out of sink hot paths and fits existing async background-work design. | Direct sink mutation of extractor/cache. |
| Keep `EventExtractor` and add dynamic enrollment | Smallest change that matches current targeted-contract architecture. | Replace the binary with `GlobalEventExtractor`. |
| Fix ECS contract typing as part of this feature | Dynamic external events will otherwise be misclassified immediately. | Postpone classification cleanup until later. |
