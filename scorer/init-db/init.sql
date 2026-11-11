CREATE TABLE IF NOT EXISTS scores (
    transaction_id VARCHAR(255) PRIMARY KEY,
    score FLOAT NOT NULL,
    fraud_flag INTEGER NOT NULL,
    model_version VARCHAR(50),
    created_at TIMESTAMP DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_fraud_flag ON scores(fraud_flag);
CREATE INDEX IF NOT EXISTS idx_created_at ON scores(created_at DESC);