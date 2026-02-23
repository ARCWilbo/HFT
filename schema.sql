CREATE TABLE IF NOT EXISTS option_snapshots (
  id            BIGSERIAL PRIMARY KEY,
  ticker        TEXT NOT NULL,
  option_symbol TEXT NOT NULL,      -- e.g., OCC symbol or your internal option id
  strike        NUMERIC(12, 4) NOT NULL,
  exp           DATE NOT NULL,
  m1            DOUBLE PRECISION,
  m2            DOUBLE PRECISION,
  m3            DOUBLE PRECISION,
  m4            DOUBLE PRECISION,

  updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
  -- prevent duplicates for the same contract identity
CREATE INDEX IF NOT EXISTS idx_option_snapshots_symbol_time
  ON option_snapshots (option_symbol, updated_at DESC);

CREATE INDEX IF NOT EXISTS idx_option_snapshots_ticker_exp_strike
  ON option_snapshots (ticker, exp, strike);