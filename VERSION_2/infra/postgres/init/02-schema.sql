CREATE TABLE IF NOT EXISTS heroes (
    hero_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    localized_name TEXT NOT NULL,
    primary_attr TEXT,
    attack_type TEXT,
    roles TEXT[]
);

CREATE TABLE IF NOT EXISTS npcs (
    npc_id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    status_health REAL,
    status_mana REAL,
    movement_speed REAL,
    attack_range REAL,
    attack_damage_min REAL,
    attack_damage_max REAL,
    attack_rate REAL,
    projectile_speed REAL,
    team_name TEXT,
    is_neutral_unit BOOLEAN,
    is_ancient BOOLEAN,
    can_be_dominated BOOLEAN,
    has_inventory BOOLEAN,
    level REAL
);

CREATE TABLE IF NOT EXISTS players (
    account_id BIGINT PRIMARY KEY,
    personaname TEXT,
    loaded_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS matches (
    match_id BIGINT PRIMARY KEY,
    match_seq_num BIGINT,
    start_time INTEGER,
    duration INTEGER,
    game_mode INTEGER,
    lobby_type INTEGER,
    radiant_win BOOLEAN,
    radiant_score INTEGER,
    dire_score INTEGER,
    tower_status_radiant INTEGER,
    tower_status_dire INTEGER,
    barracks_status_radiant INTEGER,
    barracks_status_dire INTEGER,
    cluster INTEGER,
    first_blood_time INTEGER,
    human_players INTEGER,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    stratz_parsed_at TIMESTAMPTZ,
    discover_run_id INTEGER,
    enrich_run_id INTEGER,
    enrichment_attempts INTEGER
);

CREATE TABLE IF NOT EXISTS match_players (
    match_id BIGINT,
    player_slot INTEGER,
    account_id BIGINT,
    hero_id INTEGER,
    item_0 INTEGER,
    item_1 INTEGER,
    item_2 INTEGER,
    item_3 INTEGER,
    item_4 INTEGER,
    item_5 INTEGER,
    backpack_0 INTEGER,
    backpack_1 INTEGER,
    backpack_2 INTEGER,
    item_neutral INTEGER,
    kills INTEGER,
    deaths INTEGER,
    assists INTEGER,
    kda REAL,
    gold INTEGER,
    gold_per_min INTEGER,
    xp_per_min INTEGER,
    gold_spent INTEGER,
    net_worth INTEGER,
    hero_damage INTEGER,
    tower_damage INTEGER,
    hero_healing INTEGER,
    level INTEGER,
    last_hits INTEGER,
    denies INTEGER,
    is_radiant BOOLEAN,
    win BOOLEAN,
    aghanims_scepter BOOLEAN,
    aghanims_shard BOOLEAN,
    rank INTEGER,
    loaded_at TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (match_id, player_slot)
);

CREATE INDEX IF NOT EXISTS idx_matches_start_time ON matches(start_time);
CREATE INDEX IF NOT EXISTS idx_match_players_account_id ON match_players(account_id);
CREATE INDEX IF NOT EXISTS idx_match_players_hero_id ON match_players(hero_id);

COMMENT ON TABLE heroes IS 'Справочник героев Dota 2';
COMMENT ON TABLE npcs IS 'Справочник NPC (включая Рошана, npc_id=133)';
COMMENT ON TABLE players IS 'Справочник имен игроков (PII)';
COMMENT ON TABLE matches IS 'Основная таблица матчей';
COMMENT ON TABLE match_players IS 'Статистика игроков в разрезе матчей';