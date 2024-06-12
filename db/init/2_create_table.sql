CREATE TABLE companies (
    id                 serial                PRIMARY KEY,
    name               character varying(20) NOT NULL,
    business_summary   text,
    business_raw       text,
    business_embedding vector(1536)
);
CREATE INDEX company_name_idx ON companies (name);

CREATE TABLE assets (
    id       serial                PRIMARY KEY,
    name     character varying(20) NOT NULL,
    symbol   character varying(10) NOT NULL,
    exchange character varying(10) NOT NULL,
    currency character(3)          NOT NULL
);
CREATE INDEX asset_name_idx ON assets (name);
CREATE UNIQUE INDEX asset_symbol_idx ON assets (symbol);
CREATE INDEX asset_exchange_idx ON assets (exchange);

CREATE TABLE asset_stocks (
    asset_id           integer REFERENCES assets,
    company_id         integer REFERENCES companies,
    outstanding_shares bigint,
    PRIMARY KEY(asset_id, company_id)
);
CREATE INDEX asset_stocks_company_id_idx ON asset_stocks (company_id);

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
CREATE UNIQUE INDEX asset_price_idx ON asset_prices (asset_id, date);

CREATE MATERIALIZED VIEW asset_weekly_close_prices WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 week'::interval, date) AS week,
    asset_id,
    last(close, date) AS close
FROM asset_prices
GROUP BY week, asset_id;

CREATE VIEW asset_last_prices AS
SELECT
    asset_id,
    last(close, week) AS close
FROM asset_weekly_close_prices
GROUP BY asset_id;

CREATE MATERIALIZED VIEW stock_market_caps AS
SELECT
    s.asset_id AS asset_id,
    close * outstanding_shares AS market_cap
FROM asset_stocks s
    JOIN asset_last_prices p on p.asset_id = s.asset_id;
CREATE UNIQUE INDEX stock_market_caps_asset_id_idx ON stock_market_caps (asset_id);
