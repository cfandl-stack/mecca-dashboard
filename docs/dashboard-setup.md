# Dashboard Setup

## Backup

Vor den Aenderungen wurde ein Rueckfall-Archiv des damaligen `HEAD` erstellt:

`C:\Users\c.fandl\Documents\AI_home\Codex\Ausschreibung Automatisierung\Dashboard_mecca_backup_pre_upgrade_2026-04-20.zip`

## Supabase

1. Neues Supabase-Projekt anlegen.
2. Unter SQL Editor den Inhalt aus `supabase/dashboard_hidden_records.sql` ausfuehren.
3. In Supabase Auth `Email` / `Magic Link` aktivieren.
4. In GitHub folgende Werte hinterlegen:
   - Repository Variable `PUBLIC_SUPABASE_URL`
   - Repository Variable `PUBLIC_SUPABASE_ANON_KEY`
   - Repository Secret `SUPABASE_SERVICE_ROLE_KEY`
5. Optional lokal dieselben Werte als Umgebungsvariablen setzen und `node src/generateRuntimeConfig.js` ausfuehren.

## LLM Review

Fuer die AI-Bewertung werden diese Umgebungsvariablen ausgewertet:

- `LLM_PROVIDER=minimax`
- `LLM_API_KEY`
- `LLM_MODEL` (optional, Default `MiniMax-M2.5`)
- `LLM_BASE_URL` (optional, Default `https://api.minimax.io`)
- `LLM_ENABLED=true|false`
- `LLM_MAX_RECORDS` (optional, `0` = alle Datensaetze)

Der Wochenlauf faellt bei fehlender oder fehlerhafter LLM-Konfiguration sauber auf `ungeprueft` zurueck.
