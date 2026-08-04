# Dashboard Setup

## Backup

Vor den Aenderungen wurde ein Rueckfall-Archiv des damaligen `HEAD` erstellt:

`C:\Users\c.fandl\Documents\AI_home\Codex\Ausschreibung Automatisierung\Dashboard_mecca_backup_pre_upgrade_2026-04-20.zip`

## GitHub Actions

Der Wochenlauf ist fuer GitHub Actions ausgelegt:

1. Workflow `Update Ausschreibungen` laeuft montags automatisch und kann zusaetzlich manuell gestartet werden.
2. Der Workflow erzeugt `dash/ausschreibungen.csv`, `dash/data.json` und `dash/data_embed.js`.
3. Die erzeugten Datensaetze sind bewusst als `ungeprueft` markiert; der Lauf verwendet keine externe KI und keine Zugangsdaten.
4. Bei Aenderungen commitet der Workflow genau diese Datenartefakte nach `master`.
5. Die Codex Automation `Ausschreibungen nachtraeglich bewerten` startet danach, aktualisiert zuerst ihren lokalen Stand und bewertet alle Datensaetze fachlich.
6. Der separate Workflow `Deploy to GitHub Pages` veroeffentlicht danach den aktuellen Stand.

## Codex-Bewertung

Die fachliche Bewertung erfolgt ausschliesslich durch Codex Automation. Es gibt keine lokale Heuristik, keinen API-Provider und keine KI-Umgebungsvariablen mehr. Der Wochenfeed bleibt deshalb auch bei einer nicht verfuegbaren Bewertungsautomation konsistent und liefert `ungeprueft` markierte Datensaetze.
