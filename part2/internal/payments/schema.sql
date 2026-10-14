CREATE TABLE IF NOT EXISTS partners (
    id text PRIMARY KEY,
    api_key_hash text NOT NULL UNIQUE,
    fee_bps integer NOT NULL CHECK (fee_bps BETWEEN 0 AND 10000),
    fixed_fee bigint NOT NULL CHECK (fixed_fee >= 0)
);
CREATE TABLE IF NOT EXISTS invoices (
    id text PRIMARY KEY,
    partner_id text NOT NULL REFERENCES partners,
    idempotency_key text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    address text NOT NULL UNIQUE,
    fee_bps integer NOT NULL CHECK (fee_bps BETWEEN 0 AND 10000),
    fixed_fee bigint NOT NULL CHECK (fixed_fee >= 0),
    settled boolean NOT NULL DEFAULT false,
    created_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (partner_id, idempotency_key)
);
CREATE TABLE IF NOT EXISTS chain_state (
    id integer PRIMARY KEY CHECK (id = 1),
    head bigint NOT NULL CHECK (head >= 0),
    aml_down boolean NOT NULL DEFAULT false
);
INSERT INTO chain_state(id,head) VALUES (1,0) ON CONFLICT DO NOTHING;
CREATE TABLE IF NOT EXISTS transfers (
    id text PRIMARY KEY,
    invoice_id text NOT NULL REFERENCES invoices,
    tx_hash text NOT NULL,
    log_index integer NOT NULL CHECK (log_index >= 0),
    sender text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    block_height bigint NOT NULL CHECK (block_height > 0),
    generation integer NOT NULL DEFAULT 1,
    canonical boolean NOT NULL DEFAULT true,
    status text NOT NULL DEFAULT 'confirming'
      CHECK (status IN ('confirming','aml_pending','aml_cleared','credited','refund_ready','orphaned')),
    aml_attempts integer NOT NULL DEFAULT 0,
    last_error text NOT NULL DEFAULT '',
    next_attempt_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (tx_hash, log_index)
);
CREATE INDEX IF NOT EXISTS transfers_pending ON transfers(next_attempt_at)
    WHERE status IN ('confirming','aml_pending');
CREATE TABLE IF NOT EXISTS refunds (
    id text PRIMARY KEY,
    transfer_id text NOT NULL UNIQUE REFERENCES transfers,
    destination text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    reason text NOT NULL CHECK (reason IN ('high_risk','overpayment')),
    status text NOT NULL CHECK (status IN ('created','ready_to_send')),
    created_at timestamptz NOT NULL DEFAULT now()
);
-- A posting is a balanced pair by construction: debit and credit have the
-- SAME positive amount. No independently writable account balances exist.
CREATE TABLE IF NOT EXISTS ledger_postings (
    id bigserial PRIMARY KEY,
    operation_key text NOT NULL UNIQUE,
    transfer_id text NOT NULL REFERENCES transfers,
    debit_account text NOT NULL,
    credit_account text NOT NULL,
    amount bigint NOT NULL CHECK (amount > 0),
    created_at timestamptz NOT NULL DEFAULT now(),
    CHECK (debit_account <> credit_account)
);
CREATE OR REPLACE VIEW ledger_entries AS
    SELECT id AS posting_id, debit_account AS account, -amount AS delta FROM ledger_postings
    UNION ALL
    SELECT id AS posting_id, credit_account AS account, amount AS delta FROM ledger_postings;
