CREATE UNIQUE INDEX IF NOT EXISTS user_profiles_username_uidx
    ON user_profiles (username)
    WHERE username IS NOT NULL;

CREATE INDEX IF NOT EXISTS user_profiles_profile_status_idx
    ON user_profiles (profile_status);

CREATE UNIQUE INDEX IF NOT EXISTS user_relationships_unique_idx
    ON user_relationships (user_id, target_user_id, relation_type);

CREATE INDEX IF NOT EXISTS user_relationships_user_status_idx
    ON user_relationships (user_id, status);

CREATE INDEX IF NOT EXISTS user_relationships_target_status_idx
    ON user_relationships (target_user_id, status);

CREATE UNIQUE INDEX IF NOT EXISTS user_blocks_unique_idx
    ON user_blocks (user_id, target_user_id);

CREATE INDEX IF NOT EXISTS user_blocks_target_idx
    ON user_blocks (target_user_id);

CREATE UNIQUE INDEX IF NOT EXISTS user_entity_projection_unique_idx
    ON user_entity_projection (user_id, entity_type, entity_id);

CREATE INDEX IF NOT EXISTS user_entity_projection_user_type_idx
    ON user_entity_projection (user_id, entity_type);

CREATE INDEX IF NOT EXISTS user_event_outbox_published_created_idx
    ON user_event_outbox (published_at, created_at);

CREATE INDEX IF NOT EXISTS user_event_outbox_aggregate_idx
    ON user_event_outbox (aggregate_type, aggregate_id);
