CREATE TABLE IF NOT EXISTS user_profiles (
    user_id uuid PRIMARY KEY,
    display_name text NOT NULL,
    username text NULL,
    avatar_object_id text NULL,
    bio text NULL,
    locale text NULL,
    time_zone text NULL,
    profile_status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL,
    deleted_at timestamptz NULL
);

CREATE TABLE IF NOT EXISTS user_privacy_settings (
    user_id uuid PRIMARY KEY REFERENCES user_profiles(user_id) ON DELETE CASCADE,
    profile_visibility text NOT NULL,
    dm_policy text NOT NULL,
    friend_request_policy text NOT NULL,
    last_seen_visibility text NOT NULL,
    avatar_visibility text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS user_relationships (
    relation_id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    target_user_id uuid NOT NULL,
    relation_type text NOT NULL,
    status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS user_blocks (
    block_id uuid PRIMARY KEY,
    user_id uuid NOT NULL,
    target_user_id uuid NOT NULL,
    reason text NULL,
    created_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS user_entity_projection (
    user_id uuid NOT NULL,
    entity_type text NOT NULL,
    entity_id text NOT NULL,
    relation_role text NULL,
    visibility_status text NOT NULL,
    created_at timestamptz NOT NULL,
    updated_at timestamptz NOT NULL
);

CREATE TABLE IF NOT EXISTS user_event_outbox (
    event_id uuid PRIMARY KEY,
    aggregate_type text NOT NULL,
    aggregate_id uuid NOT NULL,
    event_type text NOT NULL,
    payload jsonb NOT NULL,
    created_at timestamptz NOT NULL,
    published_at timestamptz NULL
);
