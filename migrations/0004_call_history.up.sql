CREATE TABLE IF NOT EXISTS user_call_history (
    history_id uuid PRIMARY KEY,
    call_id text NOT NULL,
    owner_user_id uuid NOT NULL REFERENCES user_profiles(user_id) ON DELETE CASCADE,
    initiator_user_id uuid NOT NULL REFERENCES user_profiles(user_id) ON DELETE CASCADE,
    call_type text NOT NULL,
    direction text NOT NULL,
    call_status text NOT NULL,
    participant_user_ids text NOT NULL,
    participant_count integer NOT NULL,
    started_at timestamptz NOT NULL,
    ended_at timestamptz NULL,
    duration_seconds integer NOT NULL,
    room_id text NULL,
    conversation_id text NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE UNIQUE INDEX IF NOT EXISTS user_call_history_owner_call_uidx
    ON user_call_history (owner_user_id, call_id);

CREATE INDEX IF NOT EXISTS user_call_history_owner_started_idx
    ON user_call_history (owner_user_id, started_at DESC, updated_at DESC);

ALTER TABLE user_call_history
    ADD CONSTRAINT user_call_history_call_type_chk
        CHECK (call_type IN ('audio', 'video')),
    ADD CONSTRAINT user_call_history_direction_chk
        CHECK (direction IN ('incoming', 'outgoing')),
    ADD CONSTRAINT user_call_history_status_chk
        CHECK (call_status IN ('completed', 'missed', 'cancelled', 'declined', 'ongoing')),
    ADD CONSTRAINT user_call_history_participant_count_chk
        CHECK (participant_count > 0),
    ADD CONSTRAINT user_call_history_duration_chk
        CHECK (duration_seconds >= 0);

ALTER TABLE user_event_outbox DROP CONSTRAINT IF EXISTS user_event_outbox_aggregate_type_chk;

ALTER TABLE user_event_outbox
    ADD CONSTRAINT user_event_outbox_aggregate_type_chk
        CHECK (aggregate_type IN ('user_profile', 'user_privacy', 'user_relationship', 'user_block', 'user_projection', 'user_call_history'));
