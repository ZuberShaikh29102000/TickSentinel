# Deploying TickSentinel live (Render free + Supabase free)

Total cost: $0. Time: about 30 minutes. You create the accounts and paste one
secret; nothing secret ever goes into the code or the GitHub repo.

```
cron-job.org ──every 10 min──▶ Render (Docker, /health) ──▶ Supabase Postgres
                                   │
                    Binance, Coinbase, Kraken websockets
```

## 1. Push the code to GitHub

Render deploys from a GitHub repository.

1. Create an empty repository on GitHub (public is fine for a portfolio).
2. In the project folder:
   ```bash
   git status            # .env must NOT be listed (it is gitignored)
   git add -A
   git commit -m "TickSentinel: live ingestion"
   git remote add origin https://github.com/<you>/ticksentinel.git
   git push -u origin main
   ```

## 2. Create the Supabase database

1. Sign up at supabase.com and create a **new project**.
   - **Region: Asia-Pacific** (or, if a specific list is offered, Southeast Asia
     (Singapore)), close to the Render service in Singapore. The exact region
     Supabase chose appears in the pooler host name, e.g. `ap-southeast-1`.
   - Choose a strong database password and save it in a password manager.
2. Get the connection string: open the project, click **Connect** (top of the
   dashboard), and copy the **Session pooler** URI. It looks like:
   ```
   postgresql://postgres.<project-ref>:[YOUR-PASSWORD]@aws-<N>-ap-southeast-1.pooler.supabase.com:5432/postgres
   ```
   Copy the host exactly from the dialog: the number `<N>` is a pooler cluster
   index that differs between projects.
   Use the **session pooler**, not "Direct connection" (IPv6-only on the free
   plan) and not "Transaction pooler" (port 6543; breaks prepared statements).
3. Replace `[YOUR-PASSWORD]` with your password and add `?sslmode=require` to
   the end. If the password contains special characters such as `@ : / ? # %`,
   URL-encode them (for example `@` becomes `%40`).

You do not need to create any tables: the app creates them on first connect.

## 3. Create the Render service

1. Sign up at render.com with your GitHub account.
2. **New > Blueprint**, pick your repository. Render reads `render.yaml`:
   one free Docker web service named `ticksentinel` in Singapore.
3. When asked for `DATABASE_URL`, paste the Supabase URL from step 2.
4. Deploy. The first build takes a few minutes. In the **Logs** tab you should
   see `binance connected`, `coinbase connected`, `kraken connected` and
   `database connected`.
5. Open `https://<your-service>.onrender.com/health`: `"status": "ok"` and
   all three feeds `"ok": true`.

## 4. Keep it awake with cron-job.org

Render's free plan stops a web service after 15 minutes without incoming
requests. The app's websocket connections are outgoing, so they don't count.

1. Sign up at cron-job.org and **Create cronjob**.
2. URL: `https://<your-service>.onrender.com/health`
3. Schedule: **every 10 minutes**. Save, then use **Test run** to check you get HTTP 200.

## 5. Check it's working

- `/health` shows uptime, per-feed message counts, rows written and dropped,
  and the latest maintenance result, including `db_bytes` (database size).
- From your PC you can inspect the production database by putting the same URL
  in your local `.env` as `DATABASE_URL=...` and running
  `uv run python -m ticksentinel.storage.check`. Remove it again afterwards so
  local runs go back to your local database.

## Limits to know

| Limit | What the app does about it |
|---|---|
| Supabase 500 MB, read-only when exceeded | Retention keeps ~300 MB; guard trims at 400 MB |
| Supabase pauses projects inactive for a week | The app writes every second, so it stays active |
| Render 750 free hours/month | One always-on service uses ~744 h; don't add a second free service |
| Render restarts/redeploys | SIGTERM is handled: buffered rows are written before exit |
