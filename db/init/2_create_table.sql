CREATE TABLE assets (
    id       serial                PRIMARY KEY,
    symbol   character varying(10) NOT NULL,
    name     text                  NOT NULL,
    currency character(3)          NOT NULL,
    type     character varying(20) NOT NULL,
    UNIQUE(symbol)
);
CREATE INDEX ix_asset_symbol ON assets (symbol);

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

CREATE TABLE companies (
    id                 serial  PRIMARY KEY,
    name               text    NOT NULL,
    listed_asset_id    integer REFERENCES assets,
    outstanding_shares bigint,
    business_summary   text,
    business_raw       text,
    business_embedding vector(1536)
);
CREATE INDEX ix_company_asset_id ON companies (listed_asset_id);
