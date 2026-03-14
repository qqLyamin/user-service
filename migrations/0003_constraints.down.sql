ALTER TABLE user_event_outbox DROP CONSTRAINT IF EXISTS user_event_outbox_aggregate_type_chk;

ALTER TABLE user_entity_projection DROP CONSTRAINT IF EXISTS user_entity_projection_entity_type_chk;

ALTER TABLE user_blocks DROP CONSTRAINT IF EXISTS user_blocks_user_target_chk;

ALTER TABLE user_relationships
    DROP CONSTRAINT IF EXISTS user_relationships_status_chk,
    DROP CONSTRAINT IF EXISTS user_relationships_relation_type_chk,
    DROP CONSTRAINT IF EXISTS user_relationships_user_target_chk;

ALTER TABLE user_privacy_settings
    DROP CONSTRAINT IF EXISTS user_privacy_settings_avatar_visibility_chk,
    DROP CONSTRAINT IF EXISTS user_privacy_settings_last_seen_visibility_chk,
    DROP CONSTRAINT IF EXISTS user_privacy_settings_friend_request_policy_chk,
    DROP CONSTRAINT IF EXISTS user_privacy_settings_dm_policy_chk,
    DROP CONSTRAINT IF EXISTS user_privacy_settings_profile_visibility_chk;

ALTER TABLE user_profiles
    DROP CONSTRAINT IF EXISTS user_profiles_profile_status_chk,
    DROP CONSTRAINT IF EXISTS user_profiles_username_len_chk,
    DROP CONSTRAINT IF EXISTS user_profiles_display_name_len_chk,
    DROP CONSTRAINT IF EXISTS user_profiles_display_name_chk;
