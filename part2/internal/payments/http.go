package payments

import (
	"context"
	"crypto/subtle"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"time"
)

func (s *Service) Handler(adminKey string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		if err := s.DB.PingContext(r.Context()); err != nil {
			http.Error(w, "database unavailable", 503)
			return
		}
		writeJSON(w, 200, map[string]string{"status": "ok"})
	})
	partner := func(fn func(http.ResponseWriter, *http.Request, string)) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				writeJSON(w, 401, map[string]string{"error": "invalid API key"})
				return
			}
			id, err := s.Authenticate(r.Context(), key)
			if err != nil {
				writeJSON(w, 401, map[string]string{"error": "invalid API key"})
				return
			}
			fn(w, r, id)
		}
	}
	mux.HandleFunc("POST /v1/invoices", partner(func(w http.ResponseWriter, r *http.Request, p string) {
		var body struct {
			Amount Amount `json:"amount"`
		}
		if !decode(w, r, &body) {
			return
		}
		v, err := s.CreateInvoice(r.Context(), p, r.Header.Get("Idempotency-Key"), body.Amount)
		respond(w, v, err)
	}))
	mux.HandleFunc("GET /v1/invoices/{id}", partner(func(w http.ResponseWriter, r *http.Request, p string) {
		v, err := s.Invoice(r.Context(), p, r.PathValue("id"))
		respond(w, v, err)
	}))
	mux.HandleFunc("GET /v1/balance", partner(func(w http.ResponseWriter, r *http.Request, p string) {
		v, err := s.Balance(r.Context(), p)
		respond(w, v, err)
	}))
	mux.HandleFunc("GET /v1/refunds", partner(func(w http.ResponseWriter, r *http.Request, p string) {
		v, err := s.Refunds(r.Context(), p)
		respond(w, v, err)
	}))
	admin := func(fn http.HandlerFunc) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			if adminKey == "" || subtle.ConstantTimeCompare([]byte(r.Header.Get("X-Admin-Key")), []byte(adminKey)) != 1 {
				writeJSON(w, 401, map[string]string{"error": "invalid admin key"})
				return
			}
			fn(w, r)
		}
	}
	// Simulator administration is separate from partner API and privileges.
	sim := &Simulator{DB: s.DB}
	mux.HandleFunc("POST /sim/transfers", admin(func(w http.ResponseWriter, r *http.Request) {
		var in Incoming
		if !decode(w, r, &in) {
			return
		}
		id, err := sim.Receive(r.Context(), in)
		respond(w, map[string]string{"transfer_id": id}, err)
	}))
	mux.HandleFunc("POST /sim/mine", admin(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Blocks int64 `json:"blocks"`
		}
		if !decode(w, r, &in) {
			return
		}
		err := sim.Mine(r.Context(), in.Blocks)
		respond(w, map[string]bool{"ok": err == nil}, err)
	}))
	mux.HandleFunc("POST /sim/reorg", admin(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Blocks int64 `json:"blocks"`
		}
		if !decode(w, r, &in) {
			return
		}
		err := sim.Reorg(r.Context(), in.Blocks)
		respond(w, map[string]bool{"ok": err == nil}, err)
	}))
	mux.HandleFunc("POST /sim/aml", admin(func(w http.ResponseWriter, r *http.Request) {
		var in struct {
			Down *bool `json:"down"`
		}
		if !decode(w, r, &in) {
			return
		}
		if in.Down == nil {
			respond(w, nil, ErrInvalid)
			return
		}
		_, err := s.DB.ExecContext(r.Context(), `UPDATE chain_state SET aml_down=$1 WHERE id=1`, *in.Down)
		respond(w, map[string]bool{"ok": err == nil}, err)
	}))
	mux.HandleFunc("POST /sim/process", admin(func(w http.ResponseWriter, r *http.Request) {
		err := s.RunOnce(r.Context())
		respond(w, map[string]bool{"ok": err == nil}, err)
	}))
	mux.HandleFunc("GET /sim/ledger", admin(func(w http.ResponseWriter, r *http.Request) {
		rows, err := s.DB.QueryContext(r.Context(), `SELECT account,sum(delta)::text FROM ledger_entries GROUP BY account ORDER BY account`)
		if err != nil {
			respond(w, nil, err)
			return
		}
		defer rows.Close()
		out := map[string]string{}
		for rows.Next() {
			var account, units string
			if err = rows.Scan(&account, &units); err != nil {
				respond(w, nil, err)
				return
			}
			out[account] = formatUnits(units)
		}
		respond(w, out, rows.Err())
	}))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ctx, cancel := context.WithTimeout(r.Context(), 15*time.Second)
		defer cancel()
		mux.ServeHTTP(w, r.WithContext(ctx))
	})
}
func decode(w http.ResponseWriter, r *http.Request, v any) bool {
	d := json.NewDecoder(http.MaxBytesReader(w, r.Body, 8192))
	d.DisallowUnknownFields()
	if err := d.Decode(v); err != nil {
		writeJSON(w, 400, map[string]string{"error": "invalid JSON: " + err.Error()})
		return false
	}
	if err := d.Decode(new(any)); err != io.EOF {
		writeJSON(w, 400, map[string]string{"error": "expected exactly one JSON object"})
		return false
	}
	return true
}
func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(v); err != nil {
		slog.Error("write response", "error", err)
	}
}
func respond(w http.ResponseWriter, v any, err error) {
	if err == nil {
		writeJSON(w, 200, v)
		return
	}
	code := 500
	message := "internal error"
	switch {
	case errors.Is(err, ErrNotFound):
		code = 404
		message = err.Error()
	case errors.Is(err, ErrConflict):
		code = 409
		message = err.Error()
	case errors.Is(err, ErrInvalid):
		code = 400
		message = err.Error()
	default:
		slog.Error("request failed", "error", err)
	}
	writeJSON(w, code, map[string]string{"error": message})
}
