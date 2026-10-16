package payments

import (
	"context"
	"database/sql"
	"fmt"
)

func reserveRefund(ctx context.Context, tx *sql.Tx, prefix, id, partner, sender string, amount Amount, reason string) error {
	if _, err := tx.ExecContext(ctx, `INSERT INTO refunds(id,transfer_id,destination,amount,reason,status) VALUES($1,$2,$3,$4,$5,'created')`, ID(), id, sender, amount, reason); err != nil {
		return err
	}
	if err := post(ctx, tx, prefix+"reserve_refund", id, "hold:"+partner, "refund:"+partner, amount); err != nil {
		return err
	}
	_, err := tx.ExecContext(ctx, `UPDATE refunds SET status='ready_to_send' WHERE transfer_id=$1`, id)
	return err
}

// Strict underpayment policy: cleared partial payments remain in hold until
// the invoice is fully covered. The fixed fee is charged once per invoice.
func (s *Service) settleInvoice(ctx context.Context, tx *sql.Tx, invoice string) error {
	var expected, fixed Amount
	var bps int
	var partner string
	var settled bool
	err := tx.QueryRowContext(ctx, `SELECT amount,fixed_fee,fee_bps,partner_id,settled FROM invoices WHERE id=$1 FOR UPDATE`, invoice).Scan(&expected, &fixed, &bps, &partner, &settled)
	if err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT id,sender,amount,generation FROM transfers WHERE invoice_id=$1 AND status='aml_cleared' AND canonical ORDER BY block_height,log_index,id`, invoice)
	if err != nil {
		return err
	}
	type clearedTransfer struct {
		id, sender string
		amount     Amount
		generation int
	}
	var items []clearedTransfer
	for rows.Next() {
		var t clearedTransfer
		if err = rows.Scan(&t.id, &t.sender, &t.amount, &t.generation); err != nil {
			rows.Close()
			return err
		}
		items = append(items, t)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	// AML clearance alone is insufficient: every contribution must still have
	// N confirmations. Keep canonicality protected until settlement commits.
	eligible := items[:0]
	var total Amount
	for _, t := range items {
		snapshot, err := s.Chain.Snapshot(ctx, tx, t.id)
		if err != nil {
			return err
		}
		if !snapshot.Canonical || snapshot.Confirmations < s.Confirmations {
			continue
		}
		eligible = append(eligible, t)
		// Only coverage matters; saturating at expected avoids sum overflow.
		if t.amount >= expected-total {
			total = expected
		} else {
			total += t.amount
		}
	}
	if !settled && total < expected {
		return nil
	}
	remaining, feeRemaining := expected, Fee(expected, bps, fixed)
	if settled {
		remaining = 0
		feeRemaining = 0
	}
	for _, t := range eligible {
		applied := t.amount
		if applied > remaining {
			applied = remaining
		}
		remaining -= applied
		fee := feeRemaining
		if fee > applied {
			fee = applied
		}
		feeRemaining -= fee
		prefix := fmt.Sprintf("%s:%d:", t.id, t.generation)
		if err = post(ctx, tx, prefix+"settle", t.id, "hold:"+partner, "partner:"+partner, applied-fee); err != nil {
			return err
		}
		if err = post(ctx, tx, prefix+"fee", t.id, "hold:"+partner, "fees", fee); err != nil {
			return err
		}
		if excess := t.amount - applied; excess > 0 {
			if err = reserveRefund(ctx, tx, prefix, t.id, partner, t.sender, excess, "overpayment"); err != nil {
				return err
			}
		}
		status := "credited"
		if applied == 0 {
			status = "refund_ready"
		}
		if _, err = tx.ExecContext(ctx, `UPDATE transfers SET status=$2 WHERE id=$1`, t.id, status); err != nil {
			return err
		}
	}
	_, err = tx.ExecContext(ctx, `UPDATE invoices SET settled=true WHERE id=$1`, invoice)
	return err
}
