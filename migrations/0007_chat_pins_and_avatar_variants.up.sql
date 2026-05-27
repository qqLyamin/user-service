ALTER TABLE user_profiles
    ADD COLUMN IF NOT EXISTS avatar_preview_object_id text NULL,
    ADD COLUMN IF NOT EXISTS avatar_full_object_id text NULL;

CREATE TABLE IF NOT EXISTS user_chat_pins (
    user_id uuid NOT NULL REFERENCES user_profiles(user_id) ON DELETE CASCADE,
    chat_type text NOT NULL,
    chat_id text NOT NULL,
    sort_order integer NULL,
    pinned_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (user_id, chat_type, chat_id)
);

CREATE INDEX IF NOT EXISTS user_chat_pins_user_order_idx
    ON user_chat_pins (user_id, sort_order, pinned_at DESC);

ALTER TABLE user_chat_pins
    ADD CONSTRAINT user_chat_pins_chat_type_chk
        CHECK (chat_type IN ('direct', 'group')),
    ADD CONSTRAINT user_chat_pins_chat_id_len_chk
        CHECK (length(btrim(chat_id)) > 0 AND char_length(chat_id) <= 256);

ALTER TABLE user_event_outbox
    DROP CONSTRAINT IF EXISTS user_event_outbox_aggregate_type_chk;

ALTER TABLE user_event_outbox
    ADD CONSTRAINT user_event_outbox_aggregate_type_chk
        CHECK (aggregate_type IN ('user_profile', 'user_privacy', 'user_relationship', 'user_block', 'user_projection', 'user_chat_pin'));
