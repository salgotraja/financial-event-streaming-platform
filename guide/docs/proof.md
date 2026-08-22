# Code to proof map

The shortest path from a behaviour you want to trust to the test that demonstrates it. Read the
implementation, then its focused unit test, then the integration test that runs it against a real
broker: the three layers give you intent, local behaviour and the broker-shaped boundary in that
order.

## Delivery and durability

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| Every producer gets `acks=all` and idempotence whether it asks or not | `ProducerDurabilityConfiguration` | the profile map is `public static` and asserted by the services that use it |
| Auto-commit cannot be turned back on by a service | `ConsumerAcknowledgementConfiguration` | `should_disable_auto_commit_even_when_a_service_configured_it_on` |
| A weaker configured ack mode loses to `MANUAL_IMMEDIATE` | `ConsumerAcknowledgementConfiguration` | `should_force_manual_immediate_acknowledgement_over_a_weaker_configured_ack_mode` |
| `isolation.level` stays unset, because no producer is transactional | `ConsumerAcknowledgementConfiguration` | `should_not_set_isolation_level_because_no_producer_is_transactional` |
| The listener container really runs manual immediate acknowledgement | `AuditKafkaConfiguration` | `should_run_the_listener_container_with_manual_immediate_acknowledgement` |

## Keying and ordering

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| A ticker's trades stay on one partition | `TradeEventPublisher` | `should_key_the_record_on_ticker_so_a_tickers_trades_stay_on_one_partition`, and `should_key_the_delivered_record_on_ticker` against a real broker |
| Tick ordering per ticker survives to the broker | `MarketDataTickPublisher` | `should_key_the_record_on_ticker_so_the_cache_projection_stays_ordered_per_ticker` |
| Corporate-action revisions for one instrument stay ordered | `CorporateActionPublisher` | `should_key_the_record_on_ticker_so_revisions_for_one_instrument_stay_ordered` |
| A compacted topic keys on row identity, not on a routing hint | `InstrumentReferencePublisher` | `should_key_the_record_on_instrument_id_because_the_topic_is_compacted` |

## Idempotency and the archive

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| The same record delivered twice is archived once | `AuditArchiveService` | `should_write_once_when_the_same_record_is_delivered_twice` |
| The same offset on a different partition is a different record | `ArchivedRecord.idempotencyKey` | `should_treat_the_same_offset_on_a_different_partition_as_a_different_record` |
| The deduplication window is bounded, and that limit is real | `AuditArchiveService` | `should_archive_again_once_a_record_falls_out_of_the_bounded_window` |
| A failed sink write is retried rather than swallowed by the dedup mark | `AuditArchiveService` | `should_archive_on_retry_when_the_sink_rejected_the_first_attempt` |
| The archive stores the delivered bytes and the broker coordinates | `ArchivedRecord` | `should_archive_the_delivered_bytes_and_the_broker_coordinates` |
| An undecodable payload never reaches the archive | `AuditArchiveService` | `should_not_write_an_undecodable_payload_to_the_archive` |
| Event type comes from the writer schema, with no compile-time dependency | `AuditEventDecoder` | `should_name_the_event_type_from_the_writer_schema_without_a_compile_time_dependency_on_it` |
| A tombstone on the compacted topic is classified, not treated as a failure | `AuditEventDecoder` | `should_classify_a_tombstone_rather_than_fail_on_it` |

## Poison records and the dead-letter path

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| One poison record is quarantined and the partition keeps moving | `AuditKafkaConfiguration`, `DeadLetterPublisher` | `should_quarantine_a_poison_record_and_keep_archiving_the_records_behind_it` |
| A transient failure gets three attempts before quarantine | `AuditKafkaConfiguration` | `should_retry_a_failing_sink_three_times_before_quarantining_the_record` |
| The dead-letter topic is the lowercase `.dlq` of the source topic | `DeadLetterPublisher` | `should_quarantine_to_the_lowercase_dlq_topic_of_the_source_topic` |
| A replay preserves per-key order | `DeadLetterPublisher` | `should_keep_the_original_key_so_a_replay_preserves_per_key_order` |
| The quarantined event carries the delivered bytes, not a re-encoding | `DeadLetterPublisher` | `should_carry_the_delivered_bytes_rather_than_a_re_encoding_of_them` |
| `retryCount` and `firstFailureAt` are measured, not invented | `FailureTracker` | `should_report_the_attempts_actually_made_and_when_the_record_first_failed` |
| The failure names the root cause, not the container's wrapper | `DeadLetterPublisher` | `should_name_the_root_cause_not_the_wrapper_the_container_reported` |
| A tombstone that fails is quarantined rather than crashing the publisher | `DeadLetterPublisher` | `should_quarantine_an_empty_payload_rather_than_fail_when_the_record_was_a_tombstone` |

## Contracts and evolution

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| A breaking schema change fails the build | `SchemaCompatibilityTest` | `should_stay_fully_compatible_with_the_accepted_baseline` |
| Deleting a schema fails the build | `SchemaCompatibilityTest` | `should_not_silently_drop_a_schema_from_the_baseline` |
| Every schema stays in one namespace | `SchemaCompatibilityTest` | `should_keep_every_schema_in_the_dev_engnotes_fes_events_namespace` |
| A native trade leaves CDC provenance null | `TradeEvent.avsc` | `should_omit_cdc_provenance_when_built_by_a_native_live_producer` |
| A migrated trade can carry provenance in the same schema | `TradeEvent.avsc` | `should_carry_cdc_provenance_when_built_by_the_migration_normalizer` |
| `timestamp-millis` survives the round trip as `Instant` | `TradeEvent.avsc` | `should_map_timestamp_millis_fields_to_instant`, `should_preserve_millisecond_timestamps_through_the_round_trip` |

## Authorization

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| An identity with no grant is denied, and the denial is distinguishable from a broken broker | `SecureKafkaStack` | `should_deny_an_authenticated_identity_that_holds_no_grant_with_an_authorization_error` |
| Each producer can write its own topic | committed `kafka-acls.yml` | `should_allow_writing_the_topic_this_identity_owns`, four subclasses |
| A write-only identity cannot read what it writes | committed `kafka-acls.yml` | `should_deny_reading_the_topic_this_identity_writes` |
| A producer cannot write another service's topic | committed `kafka-acls.yml` | `should_deny_writing_another_services_topic` |
| The archive can read the evidence it exists to keep | `audit-service` policy | `should_allow_reading_an_archived_topic_through_its_own_consumer_group` |
| The archive cannot manufacture the evidence it archives | `audit-service` policy | `should_deny_writing_a_financial_topic` |
| One workload cannot advance another's offsets | `audit-service` policy | `should_deny_joining_a_consumer_group_other_than_its_own` |

## Service identity

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| A running service authenticates as its own principal, not the administrator's | `KafkaSecurityConfiguration` | `should_be_denied_while_ungranted_then_work_once_its_own_principal_is_granted`, once per service |
| A service cannot name its own SASL identity | `KafkaSecurityConfiguration` | `should_override_a_jaas_config_a_service_tried_to_set_for_itself` |
| No service module mentions a JAAS config at all | the six service modules | `should_not_let_a_module_name_a_sasl_username_of_its_own` |
| The module name, the application name and the policy principal agree | `ServiceIdentityNamesTest` reading `settings.gradle`, `application.yml` and `kafka-acls.yml` | `should_name_one_module_one_application_name_and_one_policy_principal_alike` |
| The derivation reaches consumers, not only producers | `KafkaSecurityConfiguration` | `should_apply_to_consumers_as_well_as_producers` |
| A plaintext developer stack is not forced to authenticate | `KafkaSecurityConfiguration` | `should_stay_inactive_when_the_secure_profile_is_not_on` |
| The in-network listener demands credentials | `SecureKafkaStack` | `should_refuse_an_in_network_client_that_presents_no_credentials`, `should_admit_an_in_network_client_that_presents_credentials` |
| The inter-broker listener, where clients are super users, is unreachable from the network | `SecureKafkaStack` | `should_not_expose_the_inter_broker_listener_to_a_client_on_the_network` |
| The registry serves subjects it stored through the authenticated broker | `SecureKafkaStack` | `should_serve_a_subject_registered_against_the_authenticated_broker` |

## Producer-specific behaviour

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| Load simulation is a mode, not boot behaviour | `TickGenerationConfiguration` | `should_not_start_tick_generation_unless_load_simulation_mode_is_enabled` |
| The price walk stays positive and the spread is symmetric | `TickGenerator` | `should_keep_the_price_positive_across_a_long_walk`, `should_apply_the_configured_spread_symmetrically_around_the_mid` |
| Volume really is heavy-tailed | `TickGenerator` | `should_produce_a_heavy_volume_tail_that_a_thin_tailed_draw_would_not` |
| The generator is reproducible for a seed | `TickGenerator` | `should_be_reproducible_for_a_given_seed` |
| The batch size follows from the rate and the wake interval | `TickGenerationDriver` | `should_size_the_batch_from_the_rate_and_the_wake_interval` |
| An action missing a required attribute is rejected before the broker | `CorporateActionValidator` | `should_reject_a_stock_split_carrying_no_split_ratio`, `should_reject_an_invalid_action_before_touching_the_broker` |
| Every missing attribute is reported, not just the first | `CorporateActionValidator` | `should_report_every_missing_attribute_rather_than_only_the_first` |
| An action cannot take effect before it was announced | `CorporateActionValidator` | `should_reject_an_action_that_takes_effect_before_it_was_announced` |
| A null instrument never tombstones a compacted key | `InstrumentReferencePublisher` | `should_reject_a_null_instrument_rather_than_tombstoning_the_key` |
| Producer identity is stamped, never accepted from the caller | `InstrumentReferencePublisher` | `should_overwrite_the_caller_supplied_producer_identity_with_the_configured_one` |
| A reference version that does not advance is rejected | `InstrumentReferencePublisher` | `should_reject_a_reference_version_that_does_not_advance`, `should_track_versions_per_instrument_rather_than_globally` |
| Trace context comes from the active span when the schema has no field for it | `InstrumentReferencePublisher` | `should_deliver_w3c_trace_context_from_the_active_span_rather_than_the_payload` |
| Synthetic trade generation is a mode, not boot behaviour | `TradeGenerationConfiguration` | `should_not_wire_the_driver_when_generation_is_not_configured` |
| Trades are emitted in the whole batch due for each wake, not one at a time | `TradeGenerationDriver` | `should_publish_the_batch_due_for_each_wake` |
| The batch size follows from the rate and the wake interval | `TradeGenerationDriver` | `should_derive_the_batch_size_from_the_rate_and_the_wake_interval` |
| A generated trade satisfies the schema's required fields | `TradeGenerator` | `should_generate_a_trade_that_satisfies_the_schemas_required_fields` |
| Corporate action seeding is a mode, not boot behaviour | `CorporateActionSeedConfiguration` | `should_not_wire_the_seeder_when_the_seed_is_not_configured` |
| Every configured action is published at startup | `CorporateActionSeeder` | `should_publish_every_configured_action_at_startup` |
| An action the validator rejects is skipped rather than failing startup | `CorporateActionSeeder` | `should_skip_an_action_the_validator_rejects_rather_than_failing_startup` |

## The market cache projection

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| A newer tick overwrites, an older one never does | `MarketStateProjection` | `should_apply_a_newer_tick_over_an_older_one`, `should_skip_a_tick_older_than_the_stored_one` |
| Redelivery of the same record has one effect | `MarketStateProjection` | `should_skip_a_tick_whose_timestamp_equals_the_stored_one`, and `should_have_one_effect_when_the_same_tick_is_delivered_twice` against a real broker |
| An absent entry is written rather than skipped | `MarketStateProjection` | `should_write_the_entry_when_no_state_exists_for_the_ticker` |
| A tick reaches Redis through the listener | `MarketDataTickConsumer` | `should_project_a_healthy_tick_into_redis` |
| A malformed record is quarantined with its bytes intact and the partition keeps moving | `ProjectorKafkaConfiguration` | `should_quarantine_a_malformed_record_with_its_original_bytes_and_keep_the_partition_moving` |
| An unreachable Redis pauses the listener instead of dead-lettering good records | `ProjectorKafkaConfiguration` | `should_not_quarantine_a_valid_tick_when_redis_is_unreachable` |
| The specified metric series names really appear in a scrape | `MarketCacheMetrics` | `should_publish_the_series_names_the_specification_requires` |
| Projection lag is measured from the source event | `MarketCacheMetrics` | `should_report_the_lag_between_the_source_event_and_the_write` |
| A per-ticker gauge survives garbage collection | `MarketCacheMetrics` | `should_keep_reporting_an_entry_age_after_the_recording_call_has_returned` |
| A rejected tick does not make the cache look fresher than it is | `MarketCacheMetrics` | `should_not_advance_the_entry_age_from_a_tick_that_was_not_applied` |
| A duplicate and an out-of-order tick are counted apart | `MarketCacheMetrics` | `should_count_a_duplicate_and_an_older_tick_under_different_reasons` |
| The projector cannot read the trade stream or write the topic it projects | `security/kafka-acls.yml` | `should_deny_reading_the_trade_stream`, `should_deny_writing_the_topic_it_projects`, `should_deny_joining_a_consumer_group_other_than_its_own` |

## Structure

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| No deterministic-plane module depends on the agent plane | `build.gradle` | `./gradlew checkPlaneIsolation`, verified to fail on both a project edge and a Neo4j dependency |
| Every service commits a renderable ACL policy | `KafkaAclScriptRenderer` | `./gradlew renderKafkaAcls`, wired into `check` |
| Source formatting is enforced rather than reviewed | `build.gradle` | `./gradlew spotlessCheck`, wired into `check` |
| The audit evidence path has a working local AWS endpoint | `LocalStackFixture` | `should_accept_a_bucket_on_the_emulated_s3_endpoint`, `should_expose_kms_which_the_manifest_signature_will_depend_on`, `should_report_the_endpoint_and_region_a_client_would_be_configured_with` |

## What has no proof yet

Anything that would need a service that does not exist: enrichment latency, risk evaluation,
read-model rebuild, the agent tool boundary, sustained throughput, and evidence integrity end to end.
Dependency failure has one proof now, on the projector's Redis connection, and none on PostgreSQL. Those rows appear in `.claude/rules/testing.md` as required
categories and are waiting on their subjects.
