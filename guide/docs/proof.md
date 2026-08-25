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
| A tick lands in the bucket its own event timestamp selects, not the current one | `project-tick.lua` | `should_place_a_tick_in_the_bucket_its_own_event_timestamp_selects` |
| A redelivered offset does not double-count the window | `project-tick.lua` | `should_not_double_count_when_the_same_offset_is_replayed` |
| Two distinct ticks sharing a millisecond both count toward volume | `project-tick.lua` | `should_count_both_ticks_when_two_distinct_ticks_share_a_millisecond` |
| A bucket older than the window is pruned | `project-tick.lua` | `should_prune_a_bucket_older_than_the_window_from_the_incoming_tick` |
| The window expires while the latest-price entry does not | `MarketStateProjection` | `should_give_the_window_a_ttl_so_an_idle_ticker_does_not_linger_forever` |
| Both keys are written in one call so they cannot disagree | `MarketStateProjection` | `should_write_both_keys_in_one_call_so_they_cannot_disagree` |
| Replaying the same ticks rebuilds identical window state | `MarketDataTickConsumer` | `should_rebuild_identical_window_state_when_the_same_ticks_are_consumed_again` |
| The window bucket gauge survives garbage collection | `MarketCacheMetrics` | `should_keep_reporting_a_bucket_count_after_the_recording_call_has_returned` |
| A tick the offset guard rejected is counted apart | `MarketCacheMetrics` | `should_count_a_tick_the_offset_guard_rejected` |
| A tick carrying a non-finite price is rejected and Redis is left untouched | `MarketStateProjection` | `should_reject_a_tick_carrying_a_non_finite_price_and_leave_redis_untouched` |
| A negative volume is rejected before the write | `MarketStateProjection` | `should_reject_a_tick_carrying_a_negative_volume` |
| A wildly future timestamp cannot prune the window | `MarketStateProjection` | `should_reject_a_tick_whose_event_timestamp_is_more_than_an_hour_ahead_of_the_clock` |
| A telemetry failure does not quarantine a record already projected | `MarketDataTickConsumer` | `should_acknowledge_the_record_when_metrics_recording_throws` |
| The projector's Redis grant runs the projection script and nothing wider | `users.acl.template` | `should_run_the_projection_script_against_a_market_key_as_the_granted_user`, `should_deny_a_granted_command_outside_the_market_key_space`, `should_refuse_an_unauthenticated_connection` |
| The projector cannot read the trade stream or write the topic it projects | `security/kafka-acls.yml` | `should_deny_reading_the_trade_stream`, `should_deny_writing_the_topic_it_projects`, `should_deny_joining_a_consumer_group_other_than_its_own` |

## Trade enrichment

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| A cached tick older than the freshness limit is not used | `TradeEnricher` | `should_accept_an_age_exactly_on_the_maximum`, `should_reject_an_age_one_millisecond_over_the_maximum_as_stale` |
| A cached tick that postdates the trade is not used either, so a replay cannot enrich from the future | `TradeEnricher` | `should_reject_a_cache_entry_newer_than_the_trade_rather_than_enriching_from_the_future`, `should_accept_an_age_of_exactly_zero` |
| The VWAP fold ignores buckets newer than the trade, so a replay reproduces the live value | `MarketStateReader` | `should_discard_a_bucket_newer_than_the_trade_so_replay_reproduces_the_live_value`, `should_keep_the_bucket_exactly_on_the_upper_bound_which_is_the_trades_own` |
| The fold keeps the five-minute horizon and ignores the window's `lastOffset` field | `MarketStateReader` | `should_keep_the_bucket_exactly_on_the_lower_bound`, `should_discard_a_bucket_older_than_the_trades_five_minute_horizon`, `should_ignore_last_offset_and_any_field_that_is_not_a_bucket` |
| Market capitalisation comes from the last traded price, not the mid-price | `TradeEnricher` | `should_report_market_capitalisation_in_crores_from_the_last_traded_price`, whose fixture makes mid and last differ so the swap is detectable |
| A quote that would make `priceDeviation` infinite is rejected before the event is built | `TradeEnricher` | `should_reject_a_zero_mid_price_before_it_makes_price_deviation_infinite` |
| An empty window is refused rather than divided by | `TradeEnricher` | `should_reject_a_window_with_no_volume_rather_than_dividing_by_zero` |
| Each reason a trade cannot be enriched reaches the DLQ separately | `EnrichmentKafkaConfiguration` | `should_quarantine_a_trade_whose_ticker_has_no_projected_market_state`, `..._is_older_than_the_freshness_limit`, `..._postdates_it`, `..._carries_no_volume`, `..._the_instrument_master_does_not_carry` |
| A Redis outage pauses the container instead of dead-lettering a good trade | `EnrichmentKafkaConfiguration` | `should_pause_the_container_during_a_redis_outage_rather_than_dead_letter_a_good_trade` |
| No trade is delivered before the instrument master has been folded | `EnrichmentKafkaConfiguration` | `should_not_deliver_any_trade_before_the_instrument_master_has_been_folded`, watched failing with the gate bean removed |
| The loader waits for every partition, and an empty master starts rather than hangs | `InstrumentCacheLoader` | `should_not_report_loaded_until_every_partition_has_been_read_to_its_captured_end`, `should_report_loaded_immediately_when_the_master_is_empty` |
| A failed load fails startup instead of serving a partial map | `InstrumentCacheLoader` | `should_fail_startup_rather_than_release_a_partial_map_when_the_wait_times_out`, `should_fail_loading_and_report_not_loaded_when_the_onLoaded_callback_throws` |
| Closing a loaded loader raises nothing and a second close is a no-op, and a load against a topic with no partitions throws naming the missing partitions | `InstrumentCacheLoader` | `should_close_cleanly_once_loaded_and_treat_a_second_close_as_a_noop`, `should_close_its_own_consumer_when_setup_fails_before_the_catch_up_loop_is_reached` |
| The listener joins the configured group, not one named after its listener id | `RawTradeConsumer` | `should_join_trade_enrichment_service_not_a_group_named_after_the_listener_id`, asserting the group Spring Kafka actually computes |
| Enrichment cannot read the tick stream, write its own input, or join another group | `security/kafka-acls.yml` | `should_deny_reading_the_market_data_tick_stream`, `should_deny_writing_the_topic_it_consumes`, `should_deny_joining_a_consumer_group_other_than_its_own` |
| The enrichment Redis grant is read-only and confined to the market key space | `users.acl.template` | `should_deny_a_write_inside_eval_because_the_grant_holds_no_write_command` for command denial, `should_deny_reading_outside_market_star_using_a_command_the_identity_does_hold` for key-space denial |

## Risk alerting

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| A trade is evaluated against the rule version in force at its own event time, not the latest one | `RiskRuleEngine` | `a_trade_is_evaluated_against_the_rule_in_force_at_its_own_event_time` |
| The timeline resolves the version in force at an instant, inclusive at the effective edge | `RuleTimeline` | `a_version_is_not_in_force_before_its_effective_instant`, `the_highest_version_effective_at_the_instant_wins`, `two_transitions_sharing_an_instant_are_ordered_by_version` |
| Retirement closes the interval from its own instant, and a later approval reopens it | `RuleTimeline` | `a_retirement_turns_the_rule_off_from_its_own_instant_onward`, `a_later_activation_reinstates_a_retired_rule` |
| A draft or a rejected proposal never takes a live version out of force | `RuleTimeline` | `a_draft_alongside_a_live_version_does_not_take_the_rule_out_of_force`, `a_rejected_proposal_does_not_retire_the_live_version` |
| Applying the same transition twice changes nothing, so a replayed fold is idempotent | `RuleTimeline` | `applying_the_same_transition_twice_changes_nothing` |
| A governed version of a rule type suppresses the bootstrap set for that type, evaluated at the queried instant | `RiskRuleRegistry` | `a_governed_version_of_the_same_rule_type_suppresses_the_bootstrap`, `the_bootstrap_returns_for_instants_before_the_governed_version_took_effect`, `a_retired_governed_rule_does_not_hand_the_type_back_to_the_bootstrap` |
| Several governed rules of one type are all in force and each can alert | `RiskRuleRegistry`, `RiskRuleEngine` | `two_governed_rules_of_one_type_are_both_in_force`, `every_in_force_rule_of_the_type_is_evaluated_and_each_can_alert` |
| One malformed governed version degrades only itself, not the whole trade | `RiskRuleEngine` | `a_malformed_governed_rule_version_is_skipped_and_does_not_abort_the_other_rules` |
| The full version history is folded, not just the latest record per rule | `RuleTimelineLoader` | `the_full_history_of_a_rule_is_folded_not_just_its_latest_record` |
| An undecodable governance record is skipped rather than failing startup | `RuleTimelineLoader` | `an_undecodable_record_is_skipped_and_the_gate_still_opens`, `a_governed_version_with_invalid_parameters_is_skipped_and_the_gate_still_opens` |
| A rejected version leaves the previously in-force version alone | `RuleTimelineLoader` | `a_rejected_version_leaves_the_previously_in_force_version_alone` |
| An empty rule topic opens the gate, and a fold that cannot finish fails startup | `RuleTimelineLoader` | `the_gate_opens_on_an_empty_rule_topic`, `the_load_fails_startup_when_it_cannot_reach_the_end_offsets_in_time` |
| No trade is evaluated before the fold completes | `RiskAlertKafkaConfiguration` | `should_call_load_initial_snapshot_on_the_loader_during_context_refresh`, and `a_breaching_trade_produces_an_alert_through_a_real_broker_and_registry` watched failing with the gate bean removed |
| Severity is banded, both edges inclusive, and the critical band wins | `PriceDeviationRule` | `a_deviation_at_the_warning_band_produces_a_warning`, `a_deviation_at_the_critical_band_produces_a_critical`, `a_deviation_below_the_warning_band_produces_no_alert` |
| A downward breach alerts identically to an upward one of the same magnitude | `PriceDeviationRule` | `a_negative_deviation_of_the_same_magnitude_alerts_identically` |
| A non-finite deviation is rejected rather than silently passing every comparison | `PriceDeviationRule` | `a_non_finite_deviation_is_rejected_rather_than_evaluated` |
| The specification's single-threshold parameter name is refused rather than guessed at | `PriceDeviationParameters` | `the_specifications_single_threshold_name_is_not_silently_accepted`, `a_critical_band_below_the_warning_band_is_rejected`, `equal_bands_are_rejected` |
| Redelivery of the same trade under the same rule version produces the same alert id | `PriceDeviationRule`, `IdempotencyKeys` | `redelivering_the_same_trade_produces_the_same_alert_id`, `a_different_rule_version_produces_a_different_alert_id` |
| The alert timestamp is the trade's event time, so a replay reproduces it | `PriceDeviationRule` | `the_alert_timestamp_is_the_trades_event_time_not_the_wall_clock` |
| Every alert is published before the offset is acknowledged | `EnrichedTradeConsumer` | `the_alert_is_published_before_the_offset_is_acknowledged`, `two_alerts_from_one_trade_are_both_published_before_the_acknowledgement`, `a_trade_that_breaches_nothing_is_acknowledged_without_publishing` |
| A metrics failure after a successful publish does not re-deliver the trade | `EnrichedTradeConsumer` | `a_metrics_failure_after_a_successful_publish_does_not_prevent_the_acknowledgement` |
| The listener joins the configured group, not one named after its listener id | `EnrichedTradeConsumer` | `the_listener_id_does_not_override_the_configured_consumer_group` |
| A poison record is quarantined per record and the partition keeps moving | `RiskAlertKafkaConfiguration` | `a_malformed_record_is_quarantined_and_the_record_behind_it_is_still_evaluated`, `the_recovered_records_offset_is_acknowledged_so_the_partition_keeps_moving`, `the_quarantined_payload_comes_from_the_exception_not_the_null_record_value` |
| A decode failure and an invalid argument are never retried | `RiskAlertKafkaConfiguration` | `a_deserialization_failure_is_not_retried`, `an_invalid_argument_is_not_retried` |
| The meters scrape through a real Prometheus registry | `RiskAlertMetrics` | `every_meter_in_this_class_scrapes_through_a_real_prometheus_registry_without_throwing` |
| Risk alerting cannot write the governance topic, its own input, or join another group | `security/kafka-acls.yml` | `should_deny_writing_the_governed_rule_topic`, `should_deny_writing_the_topic_it_consumes`, `should_deny_joining_a_consumer_group_other_than_its_own`, the first watched failing with a WRITE grant added |

## Structure

| Behaviour | Implementation | Proof |
| --- | --- | --- |
| No deterministic-plane module depends on the agent plane | `build.gradle` | `./gradlew checkPlaneIsolation`, verified to fail on both a project edge and a Neo4j dependency |
| Every service commits a renderable ACL policy | `KafkaAclScriptRenderer` | `./gradlew renderKafkaAcls`, wired into `check` |
| Source formatting is enforced rather than reviewed | `build.gradle` | `./gradlew spotlessCheck`, wired into `check` |
| The audit evidence path has a working local AWS endpoint | `LocalStackFixture` | `should_accept_a_bucket_on_the_emulated_s3_endpoint`, `should_expose_kms_which_the_manifest_signature_will_depend_on`, `should_report_the_endpoint_and_region_a_client_would_be_configured_with` |

## What has no proof yet

Anything that would need a service that does not exist: read-model rebuild, the agent tool boundary,
sustained throughput, and evidence integrity end to end. Enrichment and risk evaluation now have
behavioural proof but no latency proof: no run has measured either against its budget.
Dependency failure has one proof now, on the projector's Redis connection, and none on PostgreSQL. Those rows appear in `.claude/rules/testing.md` as required
categories and are waiting on their subjects.
