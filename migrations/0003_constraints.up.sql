ALTER TABLE user_profiles
    ADD CONSTRAINT user_profiles_display_name_chk
        CHECK (length(btrim(display_name)) > 0),
    ADD CONSTRAINT user_profiles_display_name_len_chk
        CHECK (char_length(display_name) <= 64),
    ADD CONSTRAINT user_profiles_username_len_chk
        CHECK (username IS NULL OR char_length(username) <= 32),
    ADD CONSTRAINT user_profiles_profile_status_chk
        CHECK (profile_status IN ('active', 'disabled', 'deleted'));

ALTER TABLE user_privacy_settings
    ADD CONSTRAINT user_privacy_settings_profile_visibility_chk
        CHECK (profile_visibility IN ('public', 'friends_only', 'private')),
    ADD CONSTRAINT user_privacy_settings_dm_policy_chk
        CHECK (dm_policy IN ('everyone', 'friends_only', 'nobody')),
    ADD CONSTRAINT user_privacy_settings_friend_request_policy_chk
        CHECK (friend_request_policy IN ('everyone', 'mutuals_only', 'nobody')),
    ADD CONSTRAINT user_privacy_settings_last_seen_visibility_chk
        CHECK (last_seen_visibility IN ('public', 'friends_only', 'private')),
    ADD CONSTRAINT user_privacy_settings_avatar_visibility_chk
        CHECK (avatar_visibility IN ('public', 'friends_only', 'private'));

ALTER TABLE user_relationships
    ADD CONSTRAINT user_relationships_user_target_chk
        CHECK (user_id <> target_user_id),
    ADD CONSTRAINT user_relationships_relation_type_chk
        CHECK (relation_type IN ('friend', 'contact')),
    ADD CONSTRAINT user_relationships_status_chk
        CHECK (status IN ('pending_outgoing', 'pending_incoming', 'accepted', 'declined', 'removed'));

ALTER TABLE user_blocks
    ADD CONSTRAINT user_blocks_user_target_chk
        CHECK (user_id <> target_user_id);

ALTER TABLE user_entity_projection
    ADD CONSTRAINT user_entity_projection_entity_type_chk
        CHECK (entity_type IN ('room', 'conversation', 'dm', 'group', 'contact', 'media_object'));

ALTER TABLE user_event_outbox
    ADD CONSTRAINT user_event_outbox_aggregate_type_chk
        CHECK (aggregate_type IN ('user_profile', 'user_privacy', 'user_relationship', 'user_block', 'user_projection'));
