ALTER TABLE user_event_outbox
    DROP CONSTRAINT IF EXISTS user_event_outbox_aggregate_type_chk;

ALTER TABLE user_event_outbox
    ADD CONSTRAINT user_event_outbox_aggregate_type_chk
        CHECK (aggregate_type IN ('user_profile', 'user_privacy', 'user_relationship', 'user_block', 'user_projection', 'user_call_history'));

ALTER TABLE user_chat_pins
    DROP CONSTRAINT IF EXISTS user_chat_pins_chat_id_len_chk,
    DROP CONSTRAINT IF EXISTS user_chat_pins_chat_type_chk;

DROP INDEX IF EXISTS user_chat_pins_user_order_idx;

DROP TABLE IF EXISTS user_chat_pins;

ALTER TABLE user_profiles
    DROP COLUMN IF EXISTS avatar_full_object_id,
    DROP COLUMN IF EXISTS avatar_preview_object_id;
