CREATE TABLE IF NOT EXISTS public.curated_model_ready (
    collision_index       TEXT PRIMARY KEY,
    collision_year        INTEGER NOT NULL,
    collision_severity    INTEGER NOT NULL CHECK (collision_severity IN (1, 2, 3)),
    is_fatal              INTEGER NOT NULL CHECK (is_fatal IN (0, 1)),
    collision_datetime    TIMESTAMP NOT NULL,
    longitude             DOUBLE PRECISION,
    latitude              DOUBLE PRECISION,
    police_force          INTEGER,
    number_of_vehicles    INTEGER CHECK (number_of_vehicles IS NULL OR number_of_vehicles >= 1),
    number_of_casualties  INTEGER CHECK (number_of_casualties IS NULL OR number_of_casualties >= 0),
    speed_limit           INTEGER,
    road_type             INTEGER,
    urban_or_rural_area   INTEGER,
    light_conditions      INTEGER,
    weather_conditions    INTEGER,
    road_surface_conditions INTEGER,
    loaded_at             TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_curated_year
    ON public.curated_model_ready (collision_year);

CREATE INDEX IF NOT EXISTS idx_curated_is_fatal
    ON public.curated_model_ready (is_fatal);

CREATE INDEX IF NOT EXISTS idx_curated_datetime
    ON public.curated_model_ready (collision_datetime);


CREATE TABLE IF NOT EXISTS public.dim_source (
    source_id    TEXT PRIMARY KEY,
    display_name TEXT NOT NULL,
    provider     TEXT,
    license      TEXT,
    created_at   TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

INSERT INTO public.dim_source (source_id, display_name, provider, license)
VALUES
    ('uk_stats19', 'UK DfT STATS19 Collision Data', 'Department for Transport', 'Open Government Licence v3.0')
ON CONFLICT (source_id) DO NOTHING;


CREATE TABLE IF NOT EXISTS public.dim_date (
    date_key  DATE PRIMARY KEY,
    year      INTEGER NOT NULL,
    month     INTEGER NOT NULL,
    day       INTEGER NOT NULL,
    dow       INTEGER NOT NULL
);

INSERT INTO public.dim_date (date_key, year, month, day, dow)
SELECT DISTINCT
    (collision_datetime AT TIME ZONE 'UTC')::date AS date_key,
    EXTRACT(YEAR  FROM collision_datetime AT TIME ZONE 'UTC')::int,
    EXTRACT(MONTH FROM collision_datetime AT TIME ZONE 'UTC')::int,
    EXTRACT(DAY   FROM collision_datetime AT TIME ZONE 'UTC')::int,
    EXTRACT(DOW   FROM collision_datetime AT TIME ZONE 'UTC')::int
FROM public.curated_model_ready
ON CONFLICT (date_key) DO NOTHING;


CREATE TABLE IF NOT EXISTS public.dq_run_log (
    id              BIGSERIAL PRIMARY KEY,
    pipeline_run_id TEXT NOT NULL,
    layer           TEXT NOT NULL,
    check_name      TEXT NOT NULL,
    severity        TEXT NOT NULL CHECK (severity IN ('fail', 'warn')),
    passed          BOOLEAN NOT NULL,
    detail          TEXT,
    logged_at       TIMESTAMP NOT NULL DEFAULT (NOW() AT TIME ZONE 'UTC')
);

CREATE INDEX IF NOT EXISTS idx_dq_run_log_run_id
    ON public.dq_run_log (pipeline_run_id);


CREATE OR REPLACE VIEW public.v_yearly_severity AS
SELECT
    collision_year,
    COUNT(*)                                        AS total_collisions,
    SUM(is_fatal)                                   AS fatal_collisions,
    ROUND(100.0 * SUM(is_fatal) / COUNT(*), 3)      AS fatal_pct
FROM public.curated_model_ready
GROUP BY collision_year
ORDER BY collision_year;