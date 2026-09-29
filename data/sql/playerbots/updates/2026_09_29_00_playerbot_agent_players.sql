-- Tracks which real-player characters have already published a
-- player_first_entry event so the external controller's player-cohort
-- workflow fires at most once per character.

CREATE TABLE IF NOT EXISTS `playerbots_agent_players` (
  `guid` INT UNSIGNED NOT NULL,
  `seen_at` BIGINT UNSIGNED NOT NULL DEFAULT 0,
  PRIMARY KEY (`guid`)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 COLLATE = utf8mb4_unicode_ci;