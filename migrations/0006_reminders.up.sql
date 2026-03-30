CREATE TABLE IF NOT EXISTS user_reminders (
    reminder_id uuid PRIMARY KEY,
    user_id text NOT NULL,
    source_type text NOT NULL,
    message_id uuid NOT NULL,
    conversation_id text NOT NULL,
    conversation_type text NULL,
    room_id text NULL,
    call_id text NULL,
    message_preview_text text NULL,
    message_author_user_id text NULL,
    message_author_display_name text NULL,
    message_ts_ms bigint NULL,
    note text NULL,
    remind_at_ms bigint NOT NULL,
    state text NOT NULL,
    fired_at_ms bigint NULL,
    dismissed_at_ms bigint NULL,
    created_at_ms bigint NOT NULL,
    updated_at_ms bigint NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_user_reminders_user_state_remind_at
    ON user_reminders (user_id, state, remind_at_ms);

CREATE INDEX IF NOT EXISTS idx_user_reminders_due
    ON user_reminders (state, remind_at_ms);

CREATE UNIQUE INDEX IF NOT EXISTS ux_user_reminders_dedupe
    ON user_reminders (user_id, message_id, remind_at_ms, state)
    WHERE state = 'scheduled';

ALTER TABLE user_reminders
    ADD CONSTRAINT user_reminders_source_type_chk
        CHECK (source_type IN ('chat_message')),
    ADD CONSTRAINT user_reminders_state_chk
        CHECK (state IN ('scheduled', 'fired', 'dismissed', 'cancelled')),
    ADD CONSTRAINT user_reminders_message_preview_len_chk
        CHECK (message_preview_text IS NULL OR char_length(message_preview_text) <= 512),
    ADD CONSTRAINT user_reminders_note_len_chk
        CHECK (note IS NULL OR char_length(note) <= 1000);
