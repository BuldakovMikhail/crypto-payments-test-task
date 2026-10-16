package payments

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"time"
)

// RunOnce can safely be invoked by multiple processes; PostgreSQL owns the locks.
func (s *Service) RunOnce(ctx context.Context) error {
	rows, err := s.DB.QueryContext(ctx, `SELECT t.id FROM transfers t CROSS JOIN chain_state c
 WHERE (t.status IN ('confirming','aml_pending') OR
   (t.status='aml_cleared' AND EXISTS (
     SELECT 1 FROM invoices i WHERE i.id=t.invoice_id AND
       (i.settled OR i.amount <= (SELECT COALESCE(sum(p.amount),0) FROM transfers p
         WHERE p.invoice_id=i.id AND p.status='aml_cleared' AND p.canonical
           AND c.head-p.block_height+1 >= $1)))))
 AND t.canonical AND t.next_attempt_at<=now()
 AND c.head-t.block_height+1 >= $1 ORDER BY t.next_attempt_at,t.id LIMIT 100`, s.Confirmations)
	if err != nil {
		return err
	}
	var ids []string
	for rows.Next() {
		var id string
		if err = rows.Scan(&id); err != nil {
			rows.Close()
			return err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	var failures []error
	for _, id := range ids {
		if err = s.process(ctx, id); err != nil {
			failures = append(failures, fmt.Errorf("process transfer %s: %w", id, err))
			if ctx.Err() != nil {
				break
			}
		}
	}
	return errors.Join(failures...)
}
func (s *Service) process(ctx context.Context, id string) error {
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	snapshot, err := s.Chain.Snapshot(ctx, tx, id)
	if err != nil {
		return err
	}
	var status, partner, sender, invoice string
	var amount Amount
	var generation int
	var next time.Time
	err = tx.QueryRowContext(ctx, `SELECT t.status,i.partner_id,t.sender,t.amount,t.generation,t.next_attempt_at,i.id
 FROM transfers t JOIN invoices i ON i.id=t.invoice_id WHERE t.id=$1 FOR UPDATE OF t`, id).Scan(&status, &partner, &sender, &amount, &generation, &next, &invoice)
	if err != nil {
		return err
	}
	if status != "confirming" && status != "aml_pending" && status != "aml_cleared" {
		return tx.Commit()
	}
	if !snapshot.Canonical || snapshot.Confirmations < s.Confirmations {
		return tx.Commit()
	}
	if next.After(time.Now()) {
		return tx.Commit()
	}
	// A reorg can remove confirmations without removing the transfer itself.
	// Resume settlement when they return, without repeating a successful AML check.
	if status == "aml_cleared" {
		if err = s.settleInvoice(ctx, tx, invoice); err != nil {
			return err
		}
		return tx.Commit()
	}
	prefix := fmt.Sprintf("%s:%d:", id, generation)
	if status == "confirming" {
		if err = post(ctx, tx, prefix+"receive", id, "custody", "hold:"+partner, amount); err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, `UPDATE transfers SET status='aml_pending' WHERE id=$1`, id); err != nil {
			return err
		}
	}
	// Persist the hold before calling AML. No database locks or connections are
	// held during the provider call. Concurrent AML calls are safe to repeat.
	if err = tx.Commit(); err != nil {
		return err
	}
	checkedGeneration := generation
	amlCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	high, amlErr := s.AML.Check(amlCtx, sender)
	cancel()
	tx, err = s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	snapshot, err = s.Chain.Snapshot(ctx, tx, id)
	if err != nil {
		return err
	}
	err = tx.QueryRowContext(ctx, `SELECT status,generation FROM transfers WHERE id=$1 FOR UPDATE`, id).Scan(&status, &generation)
	if err != nil {
		return err
	}
	// Discard a response made stale by reorg/reinclusion or another worker.
	if status != "aml_pending" || generation != checkedGeneration || !snapshot.Canonical || snapshot.Confirmations < s.Confirmations {
		return tx.Commit()
	}
	if amlErr != nil {
		_, err = tx.ExecContext(ctx, `UPDATE transfers SET aml_attempts=aml_attempts+1,last_error=$2,next_attempt_at=now()+($3*interval '1 millisecond') WHERE id=$1`, id, amlErr.Error(), s.RetryDelay.Milliseconds())
		if err != nil {
			return err
		}
		return tx.Commit()
	}
	status = "aml_cleared"
	if high {
		status = "refund_ready"
		if err = reserveRefund(ctx, tx, prefix, id, partner, sender, amount, "high_risk"); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE transfers SET status=$2,aml_attempts=aml_attempts+1,last_error='' WHERE id=$1`, id, status)
	if err != nil {
		return err
	}
	if !high {
		if err = s.settleInvoice(ctx, tx, invoice); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func (s *Service) Run(ctx context.Context) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			tickCtx, cancel := context.WithTimeout(ctx, 15*time.Second)
			if err := s.RunOnce(tickCtx); err != nil && ctx.Err() == nil {
				slog.Error("worker iteration failed", "error", err)
			}
			cancel()
		}
	}
}
