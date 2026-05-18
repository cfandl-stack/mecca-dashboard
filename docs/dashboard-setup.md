# Dashboard Setup

## Backup

Vor den Aenderungen wurde ein Rueckfall-Archiv des damaligen `HEAD` erstellt:

`C:\Users\c.fandl\Documents\AI_home\Codex\Ausschreibung Automatisierung\Dashboard_mecca_backup_pre_upgrade_2026-04-20.zip`

## GitHub Actions

Der Wochenlauf ist fuer GitHub Actions ausgelegt:

1. Workflow `Update Ausschreibungen` laeuft montags automatisch und kann zusaetzlich manuell gestartet werden.
2. Der Workflow erzeugt `dash/ausschreibungen.csv`, `dash/data.json` und `dash/data_embed.js`.
3. Bei Aenderungen commitet der Workflow genau diese Datenartefakte nach `master`.
4. Der separate Workflow `Deploy to GitHub Pages` veroeffentlicht danach den aktuellen Stand.

## LLM Review

Fuer die AI-Bewertung werden diese Umgebungsvariablen ausgewertet:

- `LLM_PROVIDER=minimax`
- `LLM_API_KEY`
- `LLM_MODEL` (optional, Default `MiniMax-M2.5`)
- `LLM_BASE_URL` (optional, Default `https://api.minimax.io`)
- `LLM_ENABLED=true|false`
- `LLM_MAX_RECORDS` (optional, `0` = alle Datensaetze)

Der Wochenlauf faellt bei fehlender oder fehlerhafter LLM-Konfiguration sauber auf `ungeprueft` zurueck.
