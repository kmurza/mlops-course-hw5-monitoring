CREATE TABLE IF NOT EXISTS scores (
    transaction_id VARCHAR(255) PRIMARY KEY,
    score FLOAT NOT NULL,
    fraud_flag INTEGER NOT NULL,
    model_version VARCHAR(50),
    us_state VARCHAR(10),
    merch VARCHAR(255),
    cat_id VARCHAR(50),
    amount FLOAT,
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_flag ON scores(fraud_flag);
CREATE INDEX IF NOT EXISTS idx_created_at ON scores(created_at DESC);
CREATE INDEX IF NOT EXISTS idx_model_version ON scores(model_version);
CREATE INDEX IF NOT EXISTS idx_us_state ON scores(us_state);
CREATE INDEX IF NOT EXISTS idx_merch ON scores(merch);
CREATE INDEX IF NOT EXISTS idx_cat_id ON scores(cat_id);