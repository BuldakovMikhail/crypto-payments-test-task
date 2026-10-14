package payments

import (
 "context"
 "database/sql"
 "fmt"
 "log/slog"
 "time"
)

// RunOnce can safely be invoked by multiple processes; PostgreSQL owns the locks.
func (s *Service) RunOnce(ctx context.Context) error {
 rows,err:=s.DB.QueryContext(ctx,`SELECT id FROM transfers WHERE status IN ('confirming','aml_pending') AND next_attempt_at<=now() ORDER BY next_attempt_at,id LIMIT 100`)
 if err!=nil{return err};var ids []string
 for rows.Next(){var id string;if err=rows.Scan(&id);err!=nil{rows.Close();return err};ids=append(ids,id)};err=rows.Err();rows.Close();if err!=nil{return err}
 for _,id:=range ids {if err=s.process(ctx,id);err!=nil{return err}};return nil
}
func (s *Service) process(ctx context.Context,id string) error {
 tx,err:=s.DB.BeginTx(ctx,nil);if err!=nil{return err};defer tx.Rollback()
 snapshot,err:=s.Chain.Snapshot(ctx,tx,id);if err!=nil{return err}
 var status,partner,sender,invoice string;var amount Amount;var generation int;var next time.Time
 err=tx.QueryRowContext(ctx,`SELECT t.status,i.partner_id,t.sender,t.amount,t.generation,t.next_attempt_at,i.id
 FROM transfers t JOIN invoices i ON i.id=t.invoice_id WHERE t.id=$1 FOR UPDATE OF t`,id).Scan(&status,&partner,&sender,&amount,&generation,&next,&invoice)
 if err!=nil{return err}
 if status!="confirming"&&status!="aml_pending" {return tx.Commit()}
 if !snapshot.Canonical||snapshot.Confirmations<s.Confirmations{return tx.Commit()}
 if next.After(time.Now()){return tx.Commit()}
 prefix:=fmt.Sprintf("%s:%d:",id,generation)
 if status=="confirming" {
  if err=post(ctx,tx,prefix+"receive",id,"custody","hold:"+partner,amount);err!=nil{return err}
  if _,err=tx.ExecContext(ctx,`UPDATE transfers SET status='aml_pending' WHERE id=$1`,id);err!=nil{return err}
 }
 // Only the bounded local mock is called under the chain lock. A production
 // HTTP AML adapter needs a durable job/lease and a separate finalize transaction.
 amlCtx,cancel:=context.WithTimeout(ctx,2*time.Second)
 high,amlErr:=s.AML.Check(amlCtx,sender);cancel()
 if amlErr!=nil {
  _,err=tx.ExecContext(ctx,`UPDATE transfers SET aml_attempts=aml_attempts+1,last_error=$2,next_attempt_at=now()+($3*interval '1 millisecond') WHERE id=$1`,id,amlErr.Error(),s.RetryDelay.Milliseconds())
  if err!=nil{return err};return tx.Commit()
 }
 status="aml_cleared"
 if high {
  status="refund_ready"
  if err=reserveRefund(ctx,tx,prefix,id,partner,sender,amount,"high_risk");err!=nil{return err}
 }
 _,err=tx.ExecContext(ctx,`UPDATE transfers SET status=$2,aml_attempts=aml_attempts+1,last_error='' WHERE id=$1`,id,status)
 if err!=nil{return err}
 if !high {if err=s.settleInvoice(ctx,tx,invoice);err!=nil{return err}}
 return tx.Commit()
}

func reserveRefund(ctx context.Context,tx *sql.Tx,prefix,id,partner,sender string,amount Amount,reason string)error{
 if _,err:=tx.ExecContext(ctx,`INSERT INTO refunds(id,transfer_id,destination,amount,reason,status) VALUES($1,$2,$3,$4,$5,'created')`,ID(),id,sender,amount,reason);err!=nil{return err}
 if err:=post(ctx,tx,prefix+"reserve_refund",id,"hold:"+partner,"refund:"+partner,amount);err!=nil{return err}
 _,err:=tx.ExecContext(ctx,`UPDATE refunds SET status='ready_to_send' WHERE transfer_id=$1`,id);return err
}

// Strict underpayment policy: cleared partial payments remain in hold until
// the invoice is fully covered. The fixed fee is charged once per invoice.
func (s *Service) settleInvoice(ctx context.Context,tx *sql.Tx,invoice string)error{
 var expected,fixed Amount;var bps int;var partner string;var settled bool
 err:=tx.QueryRowContext(ctx,`SELECT amount,fixed_fee,fee_bps,partner_id,settled FROM invoices WHERE id=$1 FOR UPDATE`,invoice).Scan(&expected,&fixed,&bps,&partner,&settled)
 if err!=nil{return err}
 rows,err:=tx.QueryContext(ctx,`SELECT id,sender,amount,generation FROM transfers WHERE invoice_id=$1 AND status='aml_cleared' AND canonical ORDER BY block_height,log_index,id`,invoice)
 if err!=nil{return err}
 type clearedTransfer struct{id,sender string;amount Amount;generation int};var items []clearedTransfer;var total Amount
 for rows.Next(){var t clearedTransfer;if err=rows.Scan(&t.id,&t.sender,&t.amount,&t.generation);err!=nil{rows.Close();return err};items=append(items,t)
  if total<expected{if t.amount>=expected-total{total=expected}else{total+=t.amount}}
 };err=rows.Err();rows.Close();if err!=nil{return err}
 if !settled&&total<expected{return nil}
 remaining,feeRemaining:=expected,Fee(expected,bps,fixed)
 if settled{remaining=0;feeRemaining=0}
 for _,t:=range items{
  applied:=t.amount;if applied>remaining{applied=remaining};remaining-=applied
  fee:=feeRemaining;if fee>applied{fee=applied};feeRemaining-=fee
  prefix:=fmt.Sprintf("%s:%d:",t.id,t.generation)
  if err=post(ctx,tx,prefix+"settle",t.id,"hold:"+partner,"partner:"+partner,applied-fee);err!=nil{return err}
  if err=post(ctx,tx,prefix+"fee",t.id,"hold:"+partner,"fees",fee);err!=nil{return err}
  if excess:=t.amount-applied;excess>0{if err=reserveRefund(ctx,tx,prefix,t.id,partner,t.sender,excess,"overpayment");err!=nil{return err}}
  status:="credited";if applied==0{status="refund_ready"}
  if _,err=tx.ExecContext(ctx,`UPDATE transfers SET status=$2 WHERE id=$1`,t.id,status);err!=nil{return err}
 }
 _,err=tx.ExecContext(ctx,`UPDATE invoices SET settled=true WHERE id=$1`,invoice);return err
}
func (s *Service) Run(ctx context.Context) {
 ticker:=time.NewTicker(time.Second);defer ticker.Stop()
 for {select {case <-ctx.Done():return;case <-ticker.C:
  tickCtx,cancel:=context.WithTimeout(ctx,15*time.Second)
  if err:=s.RunOnce(tickCtx);err!=nil&&ctx.Err()==nil{slog.Error("worker iteration failed","error",err)};cancel()
 }}
}
