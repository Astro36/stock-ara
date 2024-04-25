CREATE TABLE companies (
    id serial PRIMARY KEY,
    name text NOT NULL,
);

CREATE TABLE company_stocks (
    company_id integer NOT NULL REFERENCES companies,
    symbol char(6) NOT NULL,
    exchange char(3) NOT NULL,
    UNIQUE(symbol, exchange),
);
CREATE INDEX ix_symbol ON company_stocks (symbol);

CREATE TABLE company_stock_prices (
    date date NOT NULL,
    company_id integer NOT NULL REFERENCES companies,
    open numeric(8, 2) NOT NULL,
    high numeric(8, 2) NOT NULL,
    low numeric(8, 2) NOT NULL,
    close numeric(8, 2) NOT NULL,
    volume integer NOT NULL,
);
SELECT create_hypertable('company_stock_prices', by_range('date'));
CREATE INDEX ix_cid_date ON company_stock_prices (company_id, date DESC);

CREATE TABLE company_filings (
    company_id integer NOT NULL REFERENCES companies,
    date date NOT NULL,
    content text NOT NULL,
    embedding vector(3),
    PRIMARY KEY(company_id, date),
);
