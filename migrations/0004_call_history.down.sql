ALTER TABLE user_event_outbox DROP CONSTRAINT IF EXISTS user_event_outbox_aggregate_type_chk;

ALTER TABLE user_event_outbox
    ADD CONSTRAINT user_event_outbox_aggregate_type_chk
        CHECK (aggregate_type IN ('user_profile', 'user_privacy', 'user_relationship', 'user_block', 'user_projection'));

DROP INDEX IF EXISTS user_call_history_owner_started_idx;
DROP INDEX IF EXISTS user_call_history_owner_call_uidx;

DROP TABLE IF EXISTS user_call_history;
