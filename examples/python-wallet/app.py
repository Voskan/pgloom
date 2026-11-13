"""A deliberately racy wallet service for trying PgLoom.

POST /withdraw {"wallet_id": 1, "amount": 100} debits a wallet and answers
{"status": "withdrawn", "balance": <new balance>}. The handler is selected
with WALLET_FIX:

    (unset)  read the balance, compute the new one in Python, write it back:
             a lost update, so two withdrawals both report balance 0
    lock     the same, but the read takes FOR NO KEY UPDATE
    atomic   one guarded UPDATE ... WHERE balance >= %s RETURNING balance
"""

import json
import os
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

from psycopg_pool import ConnectionPool

FIX = os.environ.get("WALLET_FIX", "")
pool = ConnectionPool(
    os.environ["DATABASE_URL"], min_size=2, kwargs={"autocommit": True}, open=False
)


MIGRATION = "CREATE TABLE IF NOT EXISTS wallets (id BIGINT PRIMARY KEY, balance BIGINT NOT NULL)"


class InsufficientFunds(Exception):
    pass


class NoSuchWallet(Exception):
    pass


def withdraw_read_modify_write(wallet_id, amount, lock):
    query = "SELECT balance FROM wallets WHERE id = %s" + (" FOR NO KEY UPDATE" if lock else "")
    with pool.connection() as conn, conn.transaction():
        row = conn.execute(query, (wallet_id,)).fetchone()
        if row is None:
            raise NoSuchWallet
        if row[0] < amount:
            raise InsufficientFunds
        new_balance = row[0] - amount
        conn.execute("UPDATE wallets SET balance = %s WHERE id = %s", (new_balance, wallet_id))
        return new_balance


def withdraw_atomic(wallet_id, amount):
    with pool.connection() as conn:
        row = conn.execute(
            "UPDATE wallets SET balance = balance - %s "
            "WHERE id = %s AND balance >= %s RETURNING balance",
            (amount, wallet_id, amount),
        ).fetchone()
        if row is None:
            raise InsufficientFunds
        return row[0]


class Handler(BaseHTTPRequestHandler):
    def reply(self, status, body):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_GET(self):
        if self.path == "/healthz":
            self.reply(HTTPStatus.OK, {"status": "ok"})
        else:
            self.reply(HTTPStatus.NOT_FOUND, {"error": "not found"})

    def do_POST(self):
        if self.path != "/withdraw":
            self.reply(HTTPStatus.NOT_FOUND, {"error": "not found"})
            return
        try:
            req = json.loads(self.rfile.read(int(self.headers.get("Content-Length", 0))))
            wallet_id, amount = int(req["wallet_id"]), int(req["amount"])
        except (ValueError, KeyError, TypeError) as e:
            self.reply(HTTPStatus.BAD_REQUEST, {"error": str(e)})
            return
        try:
            if FIX == "atomic":
                balance = withdraw_atomic(wallet_id, amount)
            else:
                balance = withdraw_read_modify_write(wallet_id, amount, FIX == "lock")
        except InsufficientFunds:
            self.reply(HTTPStatus.CONFLICT, {"error": "insufficient funds"})
        except NoSuchWallet:
            self.reply(HTTPStatus.NOT_FOUND, {"error": "no such wallet"})
        except Exception as e:  # noqa: BLE001
            self.reply(HTTPStatus.INTERNAL_SERVER_ERROR, {"error": str(e)})
        else:
            self.reply(HTTPStatus.OK, {"status": "withdrawn", "balance": balance})

    def log_message(self, format, *args):
        pass


def main():
    host, _, port = os.environ.get("LISTEN_ADDR", "127.0.0.1:8080").rpartition(":")
    pool.open(wait=True)
    with pool.connection() as conn:
        conn.execute(MIGRATION)
    server = ThreadingHTTPServer((host, int(port)), Handler)
    print(f"python-wallet listening on {host}:{port} (fix={FIX!r})", flush=True)
    try:
        server.serve_forever()
    finally:
        pool.close()


if __name__ == "__main__":
    main()
