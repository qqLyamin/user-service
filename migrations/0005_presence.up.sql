CREATE TABLE IF NOT EXISTS user_presence_sessions (
    user_id uuid NOT NULL REFERENCES user_profiles(user_id) ON DELETE CASCADE,
    session_id text NOT NULL,
    device_id text NULL,
    platform text NOT NULL,
    state text NOT NULL,
    last_pulse_at timestamptz NOT NULL,
    last_disconnect_at timestamptz NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    PRIMARY KEY (user_id, session_id)
);

CREATE INDEX IF NOT EXISTS user_presence_sessions_user_state_idx
    ON user_presence_sessions (user_id, state, last_pulse_at DESC);

ALTER TABLE user_presence_sessions
    ADD CONSTRAINT user_presence_sessions_state_chk
        CHECK (state IN ('connected', 'disconnected'));
