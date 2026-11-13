// Command go-wallet is a deliberately racy wallet service for trying PgLoom.
//
// POST /withdraw {"wallet_id": 1, "amount": 100} debits a wallet. The handler
// is selected with WALLET_FIX:
//
//	(unset)  read the balance, check it in Go, then debit: a double spend
//	lock     the same, but the read takes FOR NO KEY UPDATE
//	atomic   one guarded UPDATE ... WHERE balance >= $1
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type withdrawal struct {
	WalletID int64 `json:"wallet_id"`
	Amount   int64 `json:"amount"`
}

var errInsufficientFunds = errors.New("insufficient funds")

func withdrawCheckThenAct(ctx context.Context, pool *pgxpool.Pool, w withdrawal, lock bool) error {
	query := "SELECT balance FROM wallets WHERE id = $1"
	if lock {
		query += " FOR NO KEY UPDATE"
	}
	return pgx.BeginFunc(ctx, pool, func(tx pgx.Tx) error {
		var balance int64
		if err := tx.QueryRow(ctx, query, w.WalletID).Scan(&balance); err != nil {
			return err
		}
		if balance < w.Amount {
			return errInsufficientFunds
		}
		_, err := tx.Exec(ctx, "UPDATE wallets SET balance = balance - $1 WHERE id = $2", w.Amount, w.WalletID)
		return err
	})
}

func withdrawAtomic(ctx context.Context, pool *pgxpool.Pool, w withdrawal) error {
	tag, err := pool.Exec(ctx,
		"UPDATE wallets SET balance = balance - $1 WHERE id = $2 AND balance >= $1",
		w.Amount, w.WalletID)
	if err != nil {
		return err
	}
	if tag.RowsAffected() != 1 {
		return errInsufficientFunds
	}
	return nil
}

func main() {
	ctx := context.Background()
	pool, err := pgxpool.New(ctx, os.Getenv("DATABASE_URL"))
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()
	fix := os.Getenv("WALLET_FIX")

	http.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	})
	http.HandleFunc("POST /withdraw", func(w http.ResponseWriter, r *http.Request) {
		var req withdrawal
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		switch fix {
		case "atomic":
			err = withdrawAtomic(r.Context(), pool, req)
		default:
			err = withdrawCheckThenAct(r.Context(), pool, req, fix == "lock")
		}
		switch {
		case errors.Is(err, errInsufficientFunds):
			http.Error(w, err.Error(), http.StatusConflict)
		case err != nil:
			http.Error(w, err.Error(), http.StatusInternalServerError)
		default:
			w.Write([]byte("ok"))
		}
	})

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = "127.0.0.1:8080"
	}
	log.Printf("go-wallet listening on %s (fix=%q)", addr, fix)
	log.Fatal(http.ListenAndServe(addr, nil))
}
