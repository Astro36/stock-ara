CREATE TABLE companies (
    id                 serial                PRIMARY KEY,
    name               character varying(20) NOT NULL,
    business_summary   text,
    business_raw       text,
    business_embedding vector(1536)
);
CREATE INDEX ix_company_name ON companies (name);

CREATE TABLE assets (
    id       serial                PRIMARY KEY,
    name     character varying(20) NOT NULL,
    symbol   character varying(10) NOT NULL,
    exchange character varying(10) NOT NULL,
    currency character(3)          NOT NULL
);
CREATE INDEX ix_asset_name ON assets (name);
CREATE UNIQUE INDEX ix_asset_symbol ON assets (symbol);
CREATE INDEX ix_asset_exchange ON assets (exchange);

CREATE TABLE asset_stocks (
    asset_id           integer REFERENCES assets,
    company_id         integer REFERENCES companies,
    outstanding_shares bigint,
    PRIMARY KEY(company_id, asset_id)
);

CREATE TABLE asset_prices (
    date     date           NOT NULL,
    asset_id integer        NOT NULL REFERENCES assets,
    open     numeric(10, 2) NOT NULL,
    high     numeric(10, 2) NOT NULL,
    low      numeric(10, 2) NOT NULL,
    close    numeric(10, 2) NOT NULL,
    volume   integer        NOT NULL
);
SELECT create_hypertable('asset_prices', by_range('date'));
CREATE UNIQUE INDEX ix_asset_price ON asset_prices (asset_id, date);

CREATE MATERIALIZED VIEW asset_weekly_close_prices WITH (timescaledb.continuous) AS (
    SELECT
        time_bucket('1 week'::interval, date) AS week,
        asset_id,
        last(close, date) AS close
    FROM asset_prices
    GROUP BY week, asset_id
);
