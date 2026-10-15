package payments

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"
	"time"
)

//go:embed schema.sql
var schema string

var (
	ErrNotFound = errors.New("not found")
	ErrConflict = errors.New("idempotency key or transfer conflicts with existing data")
	ErrInvalid  = errors.New("invalid input")
)

type Partner struct {
	ID       string `json:"id"`
	APIKey   string `json:"api_key"`
	FeeBPS   int    `json:"fee_bps"`
	FixedFee Amount `json:"fixed_fee"`
}
type Invoice struct {
	ID        string     `json:"id"`
	PartnerID string     `json:"partner_id"`
	Amount    Amount     `json:"amount"`
	Address   string     `json:"address"`
	FeeBPS    int        `json:"fee_bps"`
	FixedFee  Amount     `json:"fixed_fee"`
	Status    string     `json:"status"`
	Transfers []Transfer `json:"transfers"`
}
type Transfer struct {
	ID            string `json:"id"`
	InvoiceID     string `json:"invoice_id"`
	TxHash        string `json:"tx_hash"`
	LogIndex      int    `json:"log_index"`
	Sender        string `json:"sender"`
	Amount        Amount `json:"amount"`
	BlockHeight   int64  `json:"block_height"`
	Confirmations int64  `json:"confirmations"`
	Status        string `json:"status"`
	AMLAttempts   int    `json:"aml_attempts"`
	LastError     string `json:"last_error,omitempty"`
}
type Refund struct {
	ID          string `json:"id"`
	TransferID  string `json:"transfer_id"`
	Destination string `json:"destination"`
	Amount      Amount `json:"amount"`
	Reason      string `json:"reason"`
	Status      string `json:"status"`
}
type Service struct {
	DB            *sql.DB
	Chain         Blockchain
	AML           AML
	Confirmations int64
	RetryDelay    time.Duration
}

func New(db *sql.DB, n int64, blacklist []string) *Service {
	return &Service{DB: db, Chain: &Simulator{DB: db}, AML: NewMockAML(db, blacklist), Confirmations: n, RetryDelay: 2 * time.Second}
}
func ID() string {
	var b [20]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b[:])
}
func hashKey(s string) string { b := sha256.Sum256([]byte(s)); return hex.EncodeToString(b[:]) }

func Migrate(ctx context.Context, db *sql.DB) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, `SELECT pg_advisory_xact_lock(8237189)`); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, schema); err != nil {
		return err
	}
	return tx.Commit()
}
func Seed(ctx context.Context, db *sql.DB, partners []Partner) error {
	if len(partners) == 0 {
		return errors.New("at least one partner is required")
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for _, p := range partners {
		if p.ID == "" || p.APIKey == "" || p.FeeBPS < 0 || p.FeeBPS > 10000 || p.FixedFee < 0 || p.FixedFee > MaxAmount {
			return ErrInvalid
		}
		_, err = tx.ExecContext(ctx, `INSERT INTO partners(id,api_key_hash,fee_bps,fixed_fee) VALUES($1,$2,$3,$4)
   ON CONFLICT(id) DO UPDATE SET api_key_hash=excluded.api_key_hash,fee_bps=excluded.fee_bps,fixed_fee=excluded.fixed_fee`, p.ID, hashKey(p.APIKey), p.FeeBPS, p.FixedFee)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}
func (s *Service) Authenticate(ctx context.Context, key string) (string, error) {
	var id string
	err := s.DB.QueryRowContext(ctx, `SELECT id FROM partners WHERE api_key_hash=$1`, hashKey(key)).Scan(&id)
	return id, err
}
func (s *Service) CreateInvoice(ctx context.Context, partner, key string, amount Amount) (Invoice, error) {
	if key == "" || len(key) > 200 || amount <= 0 || amount > MaxAmount {
		return Invoice{}, ErrInvalid
	}
	id := ID()
	_, err := s.DB.ExecContext(ctx, `INSERT INTO invoices(id,partner_id,idempotency_key,amount,address,fee_bps,fixed_fee)
  SELECT $1,id,$2,$3,$4,fee_bps,fixed_fee FROM partners WHERE id=$5
  ON CONFLICT(partner_id,idempotency_key) DO NOTHING`, id, key, amount, "0x"+ID(), partner)
	if err != nil {
		return Invoice{}, err
	}
	var stored Amount
	err = s.DB.QueryRowContext(ctx, `SELECT id,amount FROM invoices WHERE partner_id=$1 AND idempotency_key=$2`, partner, key).Scan(&id, &stored)
	if err != nil {
		return Invoice{}, err
	}
	if stored != amount {
		return Invoice{}, ErrConflict
	}
	return s.Invoice(ctx, partner, id)
}
func (s *Service) Invoice(ctx context.Context, partner, id string) (Invoice, error) {
	// Repeatable read keeps invoice status and transfer list on one snapshot.
	tx, err := s.DB.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true})
	if err != nil {
		return Invoice{}, err
	}
	defer tx.Rollback()
	var v Invoice
	var settled bool
	err = tx.QueryRowContext(ctx, `SELECT id,partner_id,amount,address,fee_bps,fixed_fee,settled FROM invoices WHERE id=$1 AND partner_id=$2`, id, partner).Scan(&v.ID, &v.PartnerID, &v.Amount, &v.Address, &v.FeeBPS, &v.FixedFee, &settled)
	if errors.Is(err, sql.ErrNoRows) {
		return v, ErrNotFound
	}
	if err != nil {
		return v, err
	}
	rows, err := tx.QueryContext(ctx, `SELECT t.id,t.invoice_id,t.tx_hash,t.log_index,t.sender,t.amount,t.block_height,
  CASE WHEN t.canonical THEN greatest(0,c.head-t.block_height+1) ELSE 0 END,t.status,t.aml_attempts,t.last_error
  FROM transfers t CROSS JOIN chain_state c WHERE t.invoice_id=$1 ORDER BY t.id`, id)
	if err != nil {
		return v, err
	}
	v.Transfers = []Transfer{}
	v.Status = "awaiting_payment"
	pending, confirming, refund, cleared := false, false, false, false
	for rows.Next() {
		var t Transfer
		err = rows.Scan(&t.ID, &t.InvoiceID, &t.TxHash, &t.LogIndex, &t.Sender, &t.Amount, &t.BlockHeight, &t.Confirmations, &t.Status, &t.AMLAttempts, &t.LastError)
		if err != nil {
			rows.Close()
			return v, err
		}
		v.Transfers = append(v.Transfers, t)
		switch t.Status {
		case "aml_cleared":
			cleared = true
		case "aml_pending":
			pending = true
		case "confirming":
			confirming = true
		case "refund_ready":
			refund = true
		}
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return v, err
	}
	switch {
	case settled:
		v.Status = "completed"
	case pending:
		v.Status = "aml_pending"
	case confirming:
		v.Status = "confirming"
	case cleared:
		v.Status = "underpaid"
	case refund:
		v.Status = "refund_required"
	}
	return v, tx.Commit()
}

// SQL SUM(bigint) returns numeric: totals can exceed int64 safely.
func formatUnits(raw string) string {
	n, ok := new(big.Int).SetString(raw, 10)
	if !ok {
		panic("non-integer ledger sum")
	}
	sign := ""
	if n.Sign() < 0 {
		sign = "-"
		n.Abs(n)
	}
	q, r := new(big.Int), new(big.Int)
	q.QuoRem(n, big.NewInt(1_000_000), r)
	return fmt.Sprintf("%s%s.%06d", sign, q.String(), r.Int64())
}
func (s *Service) Balance(ctx context.Context, partner string) (map[string]string, error) {
	var available, hold, refund string
	err := s.DB.QueryRowContext(ctx, `SELECT
  COALESCE(sum(delta) FILTER(WHERE account=$1),0)::text,
  COALESCE(sum(delta) FILTER(WHERE account=$2),0)::text,
  COALESCE(sum(delta) FILTER(WHERE account=$3),0)::text FROM ledger_entries`, "partner:"+partner, "hold:"+partner, "refund:"+partner).Scan(&available, &hold, &refund)
	if err != nil {
		return nil, err
	}
	return map[string]string{"available": formatUnits(available), "hold": formatUnits(hold), "refund_reserved": formatUnits(refund)}, nil
}
func (s *Service) Refunds(ctx context.Context, partner string) ([]Refund, error) {
	rows, err := s.DB.QueryContext(ctx, `SELECT r.id,r.transfer_id,r.destination,r.amount,r.reason,r.status FROM refunds r JOIN transfers t ON t.id=r.transfer_id JOIN invoices i ON i.id=t.invoice_id WHERE i.partner_id=$1 ORDER BY r.id`, partner)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := []Refund{}
	for rows.Next() {
		var r Refund
		if err = rows.Scan(&r.ID, &r.TransferID, &r.Destination, &r.Amount, &r.Reason, &r.Status); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}
func post(ctx context.Context, tx *sql.Tx, key, transfer, from, to string, a Amount) error {
	if a == 0 {
		return nil
	}
	_, err := tx.ExecContext(ctx, `INSERT INTO ledger_postings(operation_key,transfer_id,debit_account,credit_account,amount) VALUES($1,$2,$3,$4,$5)`, key, transfer, from, to, a)
	return err
}
