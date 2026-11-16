# Passaggio a Livello

Automated `.ics` calendar generator predicting Italian railway level crossing (*passaggio a livello*) closures from real-time ViaggiaTreno timetable and train transit data.

## How to Use It

1. Find the station you're interested in inside the [`calendars`](https://github.com/marcofaggian/passaggio-a-livello/tree/main/calendars) folder.
2. Copy its raw `.ics` URL using the format `https://github.com/marcofaggian/passaggio-a-livello/raw/refs/heads/main/calendars/<STATION_CODE>_<STATION_NAME>.ics` (for example, `https://github.com/marcofaggian/passaggio-a-livello/raw/refs/heads/main/calendars/S02672_VENEZIA_CARPENEDO.ics`) and subscribe to/import it by URL in your favourite calendar app (Apple Calendar, Google Calendar, Outlook, Thunderbird, etc.).

## How It Works

1. **Data Collection:** Fetches departure boards and train details from the ViaggiaTreno API across configured Italian stations (`REGION_PRESET`), using an incremental JSON cache (`state_cache.json`) and rate limiting to minimize upstream requests.
2. **Transit & Closure Inference:** Interpolates non-stop train transits between stations, coalesces overlapping barrier closures, and applies safety buffers (`MIN_SAFE_REOPEN_SEC`) to avoid false reopening windows.
3. **Calendar Export:** Outputs per-station/crossing `.ics` files into `OUTPUT_DIR` ready to be subscribed to from any calendar client.

## Configuration

Configure behavior via environment variables:

| Variable | Default | Description |
|---|---|---|
| `REGION_PRESET` | `ITALY_ALL` | Target station preset or region filter |
| `OUTPUT_DIR` | `calendars` | Directory where `.ics` files are written |
| `STATE_CACHE_PATH` | `state_cache.json` | Path to the persistent incremental state cache |
| `MIN_SAFE_REOPEN_SEC` | `120` | Minimum gap (in seconds) required to treat consecutive closures as separate events |
| `RATE_LIMIT_RPS` | `3.5` | Maximum upstream HTTP requests per second |

## Running Locally

Requires **Go 1.22+**.

```bash
go run .
```

Or with custom environment variables:

```bash
REGION_PRESET=ITALY_ALL \
OUTPUT_DIR=calendars \
STATE_CACHE_PATH=state_cache.json \
MIN_SAFE_REOPEN_SEC=120 \
RATE_LIMIT_RPS=3.5 \
go run .
```

## GitHub Actions Automation

To run on a schedule and persist the incremental cache between runs:

```yaml
- uses: actions/setup-go@v5
  with:
    go-version: '1.22'

- uses: actions/cache@v4
  with:
    path: state_cache.json
    key: vt-state-${{ github.run_id }}
    restore-keys: vt-state-

- run: go run .
  env:
    REGION_PRESET: ITALY_ALL
    OUTPUT_DIR: calendars
    STATE_CACHE_PATH: state_cache.json
    MIN_SAFE_REOPEN_SEC: 120
    RATE_LIMIT_RPS: 3.5
```

## Data Source & Attribution

Real-time train timetables and transit updates are sourced from the public [ViaggiaTreno](http://www.viaggiatreno.it/) endpoints (RFI / Gruppo FS Italiane). Many thanks to ViaggiaTreno for making live railway circulation data publicly accessible.

## Disclaimer

This is an unofficial, independent project and is not affiliated with, endorsed by, or connected to RFI, Trenitalia, or Gruppo FS Italiane. Closure predictions are inferred from public railway timetable and delay feeds and do not replace official railway signage or signals.