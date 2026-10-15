package payments

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/http/httptest"
	"net/url"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

const badSender = "0x0000000000000000000000000000000000000bad"
const goodSender = "0x0000000000000000000000000000000000000001"
const adminKey = "test-admin"

type fixture struct {
	t   *testing.T
	ctx context.Context
	s   *Service
	dsn string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("set TEST_DATABASE_URL to run PostgreSQL integration tests")
	}
	root, err := sql.Open("postgres", dsn)
	must(t, err)
	schemaName := "test_" + ID()
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	if _, err = root.ExecContext(ctx, `CREATE SCHEMA `+schemaName); err != nil {
		root.Close()
		cancel()
		t.Fatal(err)
	}
	u, err := url.Parse(dsn)
	must(t, err)
	q := u.Query()
	q.Set("search_path", schemaName)
	u.RawQuery = q.Encode()
	f := &fixture{t: t, ctx: ctx, dsn: u.String()}
	f.restart()
	t.Cleanup(func() {
		f.s.DB.Close()
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cleanupCancel()
		_, err := root.ExecContext(cleanupCtx, `DROP SCHEMA `+schemaName+` CASCADE`)
		if err != nil {
			t.Error(err)
		}
		root.Close()
		cancel()
	})
	must(t, Migrate(ctx, f.s.DB))
	must(t, Seed(ctx, f.s.DB, []Partner{{ID: "alpha", APIKey: "alpha-secret", FeeBPS: 150, FixedFee: 100_000}, {ID: "beta", APIKey: "beta-secret", FeeBPS: 200}}))
	return f
}
func (f *fixture) restart() {
	f.t.Helper()
	if f.s != nil {
		must(f.t, f.s.DB.Close())
	}
	db, err := sql.Open("postgres", f.dsn)
	must(f.t, err)
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(4)
	f.s = New(db, 3, []string{badSender})
}
func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}
func (f *fixture) request(method, path, key, idem string, body any, code int) []byte {
	f.t.Helper()
	var payload []byte
	if body != nil {
		var err error
		payload, err = json.Marshal(body)
		must(f.t, err)
	}
	r := httptest.NewRequest(method, path, bytes.NewReader(payload)).WithContext(f.ctx)
	r.Header.Set("X-API-Key", key)
	r.Header.Set("X-Admin-Key", key)
	r.Header.Set("Idempotency-Key", idem)
	w := httptest.NewRecorder()
	f.s.Handler(adminKey).ServeHTTP(w, r)
	if w.Code != code {
		f.t.Fatalf("%s %s: %d, want %d: %s", method, path, w.Code, code, w.Body.String())
	}
	return w.Body.Bytes()
}
func (f *fixture) invoice(key, amount string) Invoice {
	f.t.Helper()
	b := f.request("POST", "/v1/invoices", "alpha-secret", key, map[string]string{"amount": amount}, 200)
	var v Invoice
	must(f.t, json.Unmarshal(b, &v))
	return v
}
func (f *fixture) incoming(v Invoice, amount Amount, bad bool, nonce int) Incoming {
	sender := goodSender
	if bad {
		sender = badSender
	}
	return Incoming{Address: v.Address, Sender: sender, Amount: amount, TxHash: fmt.Sprintf("0x%064x", nonce)}
}
func (f *fixture) receive(in Incoming) string {
	f.t.Helper()
	b := f.request("POST", "/sim/transfers", adminKey, "", in, 200)
	var out map[string]string
	must(f.t, json.Unmarshal(b, &out))
	return out["transfer_id"]
}
func (f *fixture) mine(n int) {
	f.t.Helper()
	f.request("POST", "/sim/mine", adminKey, "", map[string]int{"blocks": n}, 200)
}
func (f *fixture) process() { f.t.Helper(); f.request("POST", "/sim/process", adminKey, "", nil, 200) }
func (f *fixture) amlDown(down bool) {
	f.t.Helper()
	f.request("POST", "/sim/aml", adminKey, "", map[string]bool{"down": down}, 200)
}
func (f *fixture) balance(wantAvailable, wantHold, wantRefund string) {
	f.t.Helper()
	b := f.request("GET", "/v1/balance", "alpha-secret", "", nil, 200)
	var v map[string]string
	must(f.t, json.Unmarshal(b, &v))
	if v["available"] != wantAvailable || v["hold"] != wantHold || v["refund_reserved"] != wantRefund {
		f.t.Fatalf("balance: %v, want %s/%s/%s", v, wantAvailable, wantHold, wantRefund)
	}
}
func (f *fixture) state(id, want string) {
	f.t.Helper()
	b := f.request("GET", "/v1/invoices/"+id, "alpha-secret", "", nil, 200)
	var v Invoice
	must(f.t, json.Unmarshal(b, &v))
	if v.Status != want {
		f.t.Fatalf("invoice status %s, want %s", v.Status, want)
	}
}
func (f *fixture) parallelProcess(n int) {
	f.t.Helper()
	var wg sync.WaitGroup
	errs := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); other := New(f.s.DB, 3, []string{badSender}); errs <- other.RunOnce(f.ctx) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		must(f.t, err)
	}
}
func (f *fixture) ledgerBalanced() {
	f.t.Helper()
	var total string
	must(f.t, f.s.DB.QueryRowContext(f.ctx, `SELECT COALESCE(sum(delta),0)::text FROM ledger_entries`).Scan(&total))
	if total != "0" {
		f.t.Fatal("unbalanced ledger", total)
	}
	var negative bool
	must(f.t, f.s.DB.QueryRowContext(f.ctx, `SELECT EXISTS(SELECT 1 FROM ledger_entries WHERE account <> 'custody' GROUP BY account HAVING sum(delta)<0)`).Scan(&negative))
	if negative {
		f.t.Fatal("negative liability account")
	}
}

func TestIntegrationEndToEndRetryRestartAndConcurrency(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("order-1", "100")
	in := f.incoming(v, 100_000_000, false, 1)
	id := f.receive(in)
	f.process()
	f.state(v.ID, "confirming")
	f.balance("0.000000", "0.000000", "0.000000")
	f.mine(2)
	f.amlDown(true)
	f.process()
	f.state(v.ID, "aml_pending")
	f.balance("0.000000", "100.000000", "0.000000")
	var attempts int
	must(t, f.s.DB.QueryRowContext(f.ctx, `SELECT aml_attempts FROM transfers WHERE id=$1`, id).Scan(&attempts))
	if attempts != 1 {
		t.Fatal(attempts)
	}
	f.restart()
	f.process()
	f.balance("0.000000", "100.000000", "0.000000")
	f.amlDown(false)
	// Move the durable retry deadline instead of sleeping in the test.
	_, err := f.s.DB.ExecContext(f.ctx, `UPDATE transfers SET next_attempt_at=now() WHERE id=$1`, id)
	must(t, err)
	f.parallelProcess(24)
	f.state(v.ID, "completed")
	f.balance("98.400000", "0.000000", "0.000000")
	if duplicate := f.receive(in); duplicate != id {
		t.Fatal("different transfer ID")
	}
	f.restart()
	f.parallelProcess(8)
	f.balance("98.400000", "0.000000", "0.000000")
	var posts int
	must(t, f.s.DB.QueryRowContext(f.ctx, `SELECT count(*) FROM ledger_postings`).Scan(&posts))
	if posts != 3 {
		t.Fatalf("duplicate postings: %d", posts)
	}
	var fees string
	must(t, f.s.DB.QueryRowContext(f.ctx, `SELECT sum(delta)::text FROM ledger_entries WHERE account='fees'`).Scan(&fees))
	if fees != "1600000" {
		t.Fatal(fees)
	}
	f.ledgerBalanced()
}

func TestIntegrationHighRiskSingleRefund(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("risk", "10")
	f.receive(f.incoming(v, 10_000_000, true, 2))
	f.mine(2)
	f.parallelProcess(16)
	f.balance("0.000000", "0.000000", "10.000000")
	f.state(v.ID, "refund_required")
	f.restart()
	f.parallelProcess(8)
	var out []Refund
	must(t, json.Unmarshal(f.request("GET", "/v1/refunds", "alpha-secret", "", nil, 200), &out))
	if len(out) != 1 || out[0].Status != "ready_to_send" || out[0].Amount != 10_000_000 || out[0].Destination != badSender || out[0].Reason != "high_risk" {
		t.Fatalf("refunds: %+v", out)
	}
	if b := strings.TrimSpace(string(f.request("GET", "/v1/refunds", "beta-secret", "", nil, 200))); b != "[]" {
		t.Fatal(b)
	}
	f.ledgerBalanced()
}

func TestIntegrationReorgBeforeConfirmationsAndReinclusion(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("reorg", "100")
	in := f.incoming(v, 100_000_000, false, 3)
	id := f.receive(in)
	f.mine(1)
	f.process()
	f.request("POST", "/sim/reorg", adminKey, "", map[string]int{"blocks": 2}, 200)
	f.process()
	f.state(v.ID, "awaiting_payment")
	f.balance("0.000000", "0.000000", "0.000000")
	var count int
	must(t, f.s.DB.QueryRowContext(f.ctx, `SELECT count(*) FROM ledger_postings`).Scan(&count))
	if count != 0 {
		t.Fatal("postings before confirmations")
	}
	f.restart()
	if f.receive(in) != id {
		t.Fatal("reinclusion lost identity")
	}
	f.mine(2)
	f.parallelProcess(8)
	f.balance("98.400000", "0.000000", "0.000000")
	f.ledgerBalanced()
	// A deep reorg after settlement is explicitly refused as a manual incident.
	f.request("POST", "/sim/reorg", adminKey, "", map[string]int{"blocks": 3}, 409)
}

func TestIntegrationReorgReversesAMLHold(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("pending-reorg", "100")
	in := f.incoming(v, 100_000_000, false, 4)
	f.receive(in)
	f.mine(2)
	f.amlDown(true)
	f.process()
	f.balance("0.000000", "100.000000", "0.000000")
	f.request("POST", "/sim/reorg", adminKey, "", map[string]int{"blocks": 3}, 200)
	f.balance("0.000000", "0.000000", "0.000000")
	f.ledgerBalanced()
	f.restart()
	f.amlDown(false)
	f.receive(in)
	f.mine(2)
	f.process()
	f.balance("98.400000", "0.000000", "0.000000")
	f.ledgerBalanced()
}

type blockingAML struct {
	started chan struct{}
	release chan struct{}
}

func (m *blockingAML) Check(ctx context.Context, _ string) (bool, error) {
	close(m.started)
	select {
	case <-m.release:
		return false, nil
	case <-ctx.Done():
		return false, ctx.Err()
	}
}
func TestIntegrationReorgWhileAMLIsRunning(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("racing-aml", "100")
	in := f.incoming(v, 100_000_000, false, 5)
	f.receive(in)
	f.mine(2)
	m := &blockingAML{started: make(chan struct{}), release: make(chan struct{})}
	f.s.AML = m
	done := make(chan error, 1)
	go func() { done <- f.s.RunOnce(f.ctx) }()
	select {
	case <-m.started:
	case <-f.ctx.Done():
		t.Fatal("AML never started")
	}
	// This must not block on the provider call, and must invalidate its result.
	f.balance("0.000000", "100.000000", "0.000000")
	must(t, (&Simulator{DB: f.s.DB}).Reorg(f.ctx, 3))
	close(m.release)
	must(t, <-done)
	f.balance("0.000000", "0.000000", "0.000000")
	f.state(v.ID, "awaiting_payment")
	f.ledgerBalanced()
}

func TestIntegrationIdempotencyAndTenantIsolation(t *testing.T) {
	f := newFixture(t)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	ids := make(chan string, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := f.s.CreateInvoice(f.ctx, "alpha", "same-key", 100_000_000)
			errs <- err
			ids <- v.ID
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		must(t, err)
	}
	id := ""
	for next := range ids {
		if id != "" && id != next {
			t.Fatal("duplicate invoices")
		}
		id = next
	}
	f.restart()
	v := f.invoice("same-key", "100.000000")
	if v.ID != id {
		t.Fatal("idempotency lost on restart")
	}
	f.request("POST", "/v1/invoices", "alpha-secret", "same-key", map[string]string{"amount": "99"}, 409)
	f.request("GET", "/v1/invoices/"+id, "beta-secret", "", nil, 404)
	f.request("GET", "/v1/invoices/"+id, "wrong", "", nil, 401)
	f.request("POST", "/sim/mine", "alpha-secret", "", map[string]int{"blocks": 1}, 401)
	f.request("GET", "/v1/balance/beta", "alpha-secret", "", nil, 404)
	var beta Invoice
	must(t, json.Unmarshal(f.request("POST", "/v1/invoices", "beta-secret", "same-key", map[string]string{"amount": "100"}, 200), &beta))
	if beta.ID == id {
		t.Fatal("idempotency keys must be tenant scoped")
	}
	f.receive(f.incoming(beta, 100_000_000, false, 6))
	f.mine(2)
	f.process()
	var balance map[string]string
	must(t, json.Unmarshal(f.request("GET", "/v1/balance", "beta-secret", "", nil, 200), &balance))
	if balance["available"] != "98.000000" {
		t.Fatal(balance)
	}
	f.balance("0.000000", "0.000000", "0.000000")
	f.ledgerBalanced()
}

func TestIntegrationUnderpaymentTopUpAndOverpayment(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("partial", "100")
	f.receive(f.incoming(v, 97_000_000, false, 7))
	f.mine(2)
	f.process()
	f.state(v.ID, "underpaid")
	f.balance("0.000000", "97.000000", "0.000000")
	f.restart()
	f.receive(f.incoming(v, 8_000_000, false, 8))
	f.mine(2)
	f.parallelProcess(8)
	f.state(v.ID, "completed")
	f.balance("98.400000", "0.000000", "5.000000")
	refunds, err := f.s.Refunds(f.ctx, "alpha")
	must(t, err)
	if len(refunds) != 1 || refunds[0].Amount != 5_000_000 || refunds[0].Reason != "overpayment" {
		t.Fatal(refunds)
	}
	// A later transfer cannot settle the invoice again; it is entirely refunded.
	f.receive(f.incoming(v, 1_000_000, false, 9))
	f.mine(2)
	f.process()
	f.balance("98.400000", "0.000000", "6.000000")
	f.ledgerBalanced()
}

func TestIntegrationFeeSnapshotAndInvalidRequests(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("snapshot", "1.000001")
	_, err := f.s.DB.ExecContext(f.ctx, `UPDATE partners SET fee_bps=9000,fixed_fee=0 WHERE id='alpha'`)
	must(t, err)
	f.receive(f.incoming(v, 1_000_001, false, 10))
	f.mine(2)
	f.process()
	f.balance("0.885001", "0.000000", "0.000000")
	for _, body := range []any{map[string]any{"amount": 1}, map[string]string{"amount": "0"}, map[string]string{"amount": "1.0000001"}, map[string]string{"amount": "-1"}, map[string]string{"amount": "1", "unexpected": "x"}} {
		f.request("POST", "/v1/invoices", "alpha-secret", ID(), body, 400)
	}
	f.request("POST", "/v1/invoices", "alpha-secret", "", map[string]string{"amount": "1"}, 400)
	sim := &Simulator{DB: f.s.DB}
	in := f.incoming(v, 1_000_001, false, 10)
	in.Amount++
	if _, err = sim.Receive(f.ctx, in); !errors.Is(err, ErrConflict) {
		t.Fatalf("expected conflict: %v", err)
	}
	f.ledgerBalanced()
}

func TestIntegrationStaleAMLAfterReinclusion(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("generation", "100")
	in := f.incoming(v, 100_000_000, false, 11)
	id := f.receive(in)
	f.mine(2)
	m := &blockingAML{started: make(chan struct{}), release: make(chan struct{})}
	f.s.AML = m
	done := make(chan error, 1)
	go func() { done <- f.s.RunOnce(f.ctx) }()
	select {
	case <-m.started:
	case <-f.ctx.Done():
		t.Fatal("AML never started")
	}
	sim := &Simulator{DB: f.s.DB}
	must(t, sim.Reorg(f.ctx, 3))
	f.receive(in)
	f.mine(2)
	// The reincarnated transfer is also pending, but belongs to another generation.
	f.amlDown(true)
	other := New(f.s.DB, 3, []string{badSender})
	must(t, other.RunOnce(f.ctx))
	close(m.release)
	must(t, <-done)
	f.balance("0.000000", "100.000000", "0.000000")
	f.state(v.ID, "aml_pending")
	f.s = other
	f.amlDown(false)
	_, err := f.s.DB.ExecContext(f.ctx, `UPDATE transfers SET next_attempt_at=now() WHERE id=$1`, id)
	must(t, err)
	f.process()
	f.balance("98.400000", "0.000000", "0.000000")
	f.ledgerBalanced()
}

func TestIntegrationRefundTransactionRollbackAndRecovery(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("rollback", "100")
	f.receive(f.incoming(v, 100_000_000, true, 12))
	f.mine(2)
	// Fail between refund creation and completion to verify atomic rollback.
	_, err := f.s.DB.ExecContext(f.ctx, `CREATE FUNCTION fail_refund_posting() RETURNS trigger LANGUAGE plpgsql AS $$
 BEGIN IF NEW.credit_account LIKE 'refund:%' THEN RAISE EXCEPTION 'injected write failure'; END IF; RETURN NEW; END; $$;
 CREATE TRIGGER injected_failure BEFORE INSERT ON ledger_postings FOR EACH ROW EXECUTE FUNCTION fail_refund_posting()`)
	must(t, err)
	if err = f.s.RunOnce(f.ctx); err == nil {
		t.Fatal("expected injected failure")
	}
	refunds, err := f.s.Refunds(f.ctx, "alpha")
	must(t, err)
	if len(refunds) != 0 {
		t.Fatal("refund survived failed transaction")
	}
	f.balance("0.000000", "100.000000", "0.000000")
	f.state(v.ID, "aml_pending")
	f.ledgerBalanced()
	_, err = f.s.DB.ExecContext(f.ctx, `DROP TRIGGER injected_failure ON ledger_postings`)
	must(t, err)
	f.restart()
	f.process()
	f.balance("0.000000", "0.000000", "100.000000")
	refunds, err = f.s.Refunds(f.ctx, "alpha")
	must(t, err)
	if len(refunds) != 1 {
		t.Fatal(refunds)
	}
	if _, err = f.s.DB.ExecContext(f.ctx, `UPDATE ledger_postings SET amount=1`); err == nil {
		t.Fatal("ledger allowed mutation")
	}
	f.ledgerBalanced()
}

func TestIntegrationConcurrentTransferDelivery(t *testing.T) {
	f := newFixture(t)
	v := f.invoice("parallel-receive", "100")
	in := f.incoming(v, 100_000_000, false, 13)
	var wg sync.WaitGroup
	errs := make(chan error, 16)
	ids := make(chan string, 16)
	for i := 0; i < 16; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			id, err := (&Simulator{DB: f.s.DB}).Receive(f.ctx, in)
			errs <- err
			ids <- id
		}()
	}
	wg.Wait()
	close(errs)
	close(ids)
	for err := range errs {
		must(t, err)
	}
	id := ""
	for next := range ids {
		if id != "" && id != next {
			t.Fatal("duplicate transfers")
		}
		id = next
	}
	var head int64
	must(t, f.s.DB.QueryRowContext(f.ctx, `SELECT head FROM chain_state WHERE id=1`).Scan(&head))
	if head != 1 {
		t.Fatal("duplicate delivery mined blocks", head)
	}
	f.mine(2)
	f.parallelProcess(16)
	f.balance("98.400000", "0.000000", "0.000000")
	f.ledgerBalanced()
}
