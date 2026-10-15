package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"cryptopayments/internal/payments"

	_ "github.com/lib/pq"
)

func main() {
	if err := run(); err != nil {
		slog.Error("server stopped", "error", err)
		os.Exit(1)
	}
}
func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	dsn := os.Getenv("DATABASE_URL")
	admin := os.Getenv("ADMIN_KEY")
	if dsn == "" || admin == "" {
		return errors.New("DATABASE_URL and ADMIN_KEY are required")
	}
	n := int64(12)
	if raw := os.Getenv("CONFIRMATIONS"); raw != "" {
		v, err := strconv.ParseInt(raw, 10, 64)
		if err != nil || v < 1 || v > 1000000 {
			return errors.New("invalid CONFIRMATIONS")
		}
		n = v
	}
	file := os.Getenv("PARTNERS_FILE")
	if file == "" {
		file = "config/partners.json"
	}
	data, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	var partners []payments.Partner
	if err = json.Unmarshal(data, &partners); err != nil {
		return err
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	db.SetMaxOpenConns(20)
	db.SetMaxIdleConns(10)
	db.SetConnMaxLifetime(30 * time.Minute)
	initCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	if err = db.PingContext(initCtx); err != nil {
		return err
	}
	if err = payments.Migrate(initCtx, db); err != nil {
		return err
	}
	if err = payments.Seed(initCtx, db, partners); err != nil {
		return err
	}
	s := payments.New(db, n, strings.Split(os.Getenv("AML_BLACKLIST"), ","))
	workerDone := make(chan struct{})
	go func() { defer close(workerDone); s.Run(ctx) }()
	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	server := &http.Server{Addr: addr, Handler: s.Handler(admin), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 20 * time.Second, IdleTimeout: 60 * time.Second}
	serverErr := make(chan error, 1)
	go func() {
		slog.Info("listening", "address", addr, "confirmations", n)
		serverErr <- server.ListenAndServe()
	}()
	select {
	case <-ctx.Done():
	case err = <-serverErr:
		if !errors.Is(err, http.ErrServerClosed) {
			stop()
			<-workerDone
			return err
		}
	}
	stop()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer shutdownCancel()
	err = server.Shutdown(shutdownCtx)
	<-workerDone
	return err
}
