package payments

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"strings"
)

type ChainSnapshot struct {
	Canonical     bool
	Confirmations int64
}

// Snapshot must protect the returned canonicality until tx commits.
type Blockchain interface {
	Snapshot(context.Context, *sql.Tx, string) (ChainSnapshot, error)
}
type Simulator struct{ DB *sql.DB }

var addressPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)
var hashPattern = regexp.MustCompile(`^0x[0-9a-fA-F]{64}$`)

type Incoming struct {
	Address  string `json:"address"`
	Sender   string `json:"sender"`
	Amount   Amount `json:"amount"`
	TxHash   string `json:"tx_hash"`
	LogIndex int    `json:"log_index"`
}

func lockChain(ctx context.Context, tx *sql.Tx) (int64, error) {
	var head int64
	err := tx.QueryRowContext(ctx, `SELECT head FROM chain_state WHERE id=1 FOR UPDATE`).Scan(&head)
	return head, err
}
func (s *Simulator) Snapshot(ctx context.Context, tx *sql.Tx, id string) (ChainSnapshot, error) {
	head, err := lockChain(ctx, tx)
	if err != nil {
		return ChainSnapshot{}, err
	}
	var v ChainSnapshot
	var block int64
	err = tx.QueryRowContext(ctx, `SELECT canonical,block_height FROM transfers WHERE id=$1`, id).Scan(&v.Canonical, &block)
	if v.Canonical && head >= block {
		v.Confirmations = head - block + 1
	}
	return v, err
}

// Receive mines a block containing one successful, genuine USDT log.
// Re-delivery of a canonical log does not mine another block.
func (s *Simulator) Receive(ctx context.Context, in Incoming) (string, error) {
	if !addressPattern.MatchString(in.Address) || !addressPattern.MatchString(in.Sender) || !hashPattern.MatchString(in.TxHash) || in.Amount <= 0 || in.Amount > MaxAmount || in.LogIndex < 0 {
		return "", ErrInvalid
	}
	in.Address = strings.ToLower(in.Address)
	in.Sender = strings.ToLower(in.Sender)
	in.TxHash = strings.ToLower(in.TxHash)
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer tx.Rollback()
	head, err := lockChain(ctx, tx)
	if err != nil {
		return "", err
	}
	var invoice string
	err = tx.QueryRowContext(ctx, `SELECT id FROM invoices WHERE address=$1`, in.Address).Scan(&invoice)
	if errors.Is(err, sql.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", err
	}
	var id, oldInvoice, sender string
	var amount Amount
	var canonical bool
	err = tx.QueryRowContext(ctx, `SELECT id,invoice_id,sender,amount,canonical FROM transfers WHERE tx_hash=$1 AND log_index=$2`, in.TxHash, in.LogIndex).Scan(&id, &oldInvoice, &sender, &amount, &canonical)
	switch {
	case err == nil:
		if oldInvoice != invoice || sender != in.Sender || amount != in.Amount {
			return "", ErrConflict
		}
		if canonical {
			return id, tx.Commit()
		}
		_, err = tx.ExecContext(ctx, `UPDATE transfers SET canonical=true,status='confirming',block_height=$2,generation=generation+1,next_attempt_at=now(),last_error='' WHERE id=$1`, id, head+1)
	case errors.Is(err, sql.ErrNoRows):
		id = ID()
		_, err = tx.ExecContext(ctx, `INSERT INTO transfers(id,invoice_id,tx_hash,log_index,sender,amount,block_height) VALUES($1,$2,$3,$4,$5,$6,$7)`, id, invoice, in.TxHash, in.LogIndex, in.Sender, in.Amount, head+1)
	}
	if err != nil {
		return "", err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE chain_state SET head=head+1 WHERE id=1`); err != nil {
		return "", err
	}
	return id, tx.Commit()
}
func (s *Simulator) Mine(ctx context.Context, blocks int64) error {
	if blocks <= 0 || blocks > 1000000 {
		return ErrInvalid
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = lockChain(ctx, tx); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE chain_state SET head=head+$1 WHERE id=1`, blocks); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Simulator) Reorg(ctx context.Context, blocks int64) error {
	if blocks <= 0 || blocks > 1000000 {
		return ErrInvalid
	}
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	head, err := lockChain(ctx, tx)
	if err != nil {
		return err
	}
	if blocks > head {
		return ErrInvalid
	}
	head -= blocks
	var terminal bool
	err = tx.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM transfers WHERE canonical AND block_height>$1 AND status IN ('credited','refund_ready'))`, head).Scan(&terminal)
	if err != nil {
		return err
	}
	if terminal {
		return fmt.Errorf("%w: deep reorg of settled funds requires manual reconciliation", ErrConflict)
	}
	rows, err := tx.QueryContext(ctx, `SELECT t.id,t.amount,t.generation,i.partner_id FROM transfers t JOIN invoices i ON i.id=t.invoice_id WHERE t.canonical AND t.block_height>$1 AND t.status IN ('aml_pending','aml_cleared')`, head)
	if err != nil {
		return err
	}
	type reversal struct {
		id, partner string
		amount      Amount
		generation  int
	}
	var reversals []reversal
	for rows.Next() {
		var r reversal
		if err = rows.Scan(&r.id, &r.amount, &r.generation, &r.partner); err != nil {
			rows.Close()
			return err
		}
		reversals = append(reversals, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, r := range reversals {
		if err = post(ctx, tx, fmt.Sprintf("%s:%d:reorg", r.id, r.generation), r.id, "hold:"+r.partner, "custody", r.amount); err != nil {
			return err
		}
	}
	if _, err = tx.ExecContext(ctx, `UPDATE transfers SET canonical=false,status='orphaned',last_error='' WHERE canonical AND block_height>$1`, head); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, `UPDATE chain_state SET head=$1 WHERE id=1`, head); err != nil {
		return err
	}
	return tx.Commit()
}
