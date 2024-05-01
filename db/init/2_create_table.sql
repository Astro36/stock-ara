CREATE TABLE companies (
    id serial PRIMARY KEY,
    name text NOT NULL
);

CREATE TABLE company_filings (
    company_id integer NOT NULL REFERENCES companies,
    date date NOT NULL,
    business_summary text,
    business_detail text,
    business_embedding vector(1536),
    PRIMARY KEY(company_id, date)
);

CREATE TABLE securities (
    id serial PRIMARY KEY,
    symbol char(6) NOT NULL,
    exchange char(3) NOT NULL,
    currency char(3) NOT NULL,
    company_id integer NOT NULL REFERENCES companies,
    UNIQUE(symbol, exchange)
);
CREATE INDEX ix_security_symbol ON securities (symbol, exchange);

CREATE TABLE security_prices (
    date date NOT NULL,
    security_id integer NOT NULL REFERENCES securities,
    open numeric(10, 2) NOT NULL,
    high numeric(10, 2) NOT NULL,
    low numeric(10, 2) NOT NULL,
    close numeric(10, 2) NOT NULL,
    volume integer NOT NULL
);
SELECT create_hypertable('security_prices', by_range('date'));
CREATE UNIQUE INDEX ix_security_price ON security_prices (security_id, date);
