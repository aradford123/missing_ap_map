# MissingAPMap

Scans a Cisco Catalyst Center site hierarchy for access points that are
**assigned to a floor** but have **no position on the floor map** (a common
data-hygiene gap that breaks heatmaps and RF planning).

For every floor it compares:

- `GET /dna/intent/api/v1/networkDevices/assignedToSite?siteId={floorId}` — devices assigned to the floor
- `GET /dna/intent/api/v2/floors/{floorId}/accessPointPositions` — APs with a map position on that floor

APs assigned to the floor but not present (by MAC address) in the positions
list are reported as missing placement.

## Setup

```bash
uv sync
```

Credentials come from environment variables (never CLI args, so they don't
end up in shell history or `ps`):

```bash
export DNAC_USER=myuser
export DNAC_PASSWORD='mypassword'
```

If unset, they default to the Cisco DevNet Always-On sandbox credentials
(`devnetuser` / `Cisco123!`), which is only useful for smoke-testing the tool.

`$DNAC` and `$DNAC_VERSION` set the defaults for `--dnac` and `--version`
below (falling back to the DevNet sandbox host and API version `2.3.7.9` if
unset).

## Usage

```bash
# Scan every floor on a Catalyst Center
uv run missing_ap_map.py --dnac 10.10.10.121

# Scope the scan to one building/subtree
uv run missing_ap_map.py --dnac 10.10.10.121 --site "Global/HQ/Building1"

# Verbose logging, more/fewer concurrent floor lookups
uv run missing_ap_map.py --dnac 10.10.10.121 -v --workers 20

# Explicit output path
uv run missing_ap_map.py --dnac 10.10.10.121 -o report.csv
```

Run once per Catalyst Center — since this is meant to be re-run across
several controllers, pass a different `--dnac` each time.

### Arguments

| Flag | Description |
|---|---|
| `--dnac` | Catalyst Center hostname/IP (defaults to `$DNAC`, then the DevNet sandbox) |
| `--version` | Catalyst Center API version to target (defaults to `$DNAC_VERSION`, then `2.3.7.9`) |
| `--site` | Only scan floors whose site hierarchy contains this substring. Omit to scan **every** floor. |
| `--workers` | Number of floors processed concurrently (default 10) |
| `--rate-limit` | Max API calls/minute, paced across all workers (default 100). `0` disables pacing - don't do that against a controller you don't control. |
| `--inventory-batch-size` | Device IDs per batched inventory call (default 25, recommended range 20-50) |
| `--cache-file` | Path to the incremental scan-progress cache (default `output/<dnac>/missing_ap_scan_cache.jsonl`) |
| `--fresh` | Ignore any existing cache and rescan every floor from scratch |
| `-o, --output` | CSV report path (default `output/<dnac>/missing_ap_report_<timestamp>.csv`) |
| `--verify-ssl` | Verify the controller's TLS certificate (default off, for self-signed appliance certs) |
| `-v, --verbose` | Debug logging |

## Resuming a large scan

Every floor's result is appended to the cache file as soon as it's scanned
(not batched to the end), so the scan can be safely killed (Ctrl-C, timeout,
box reboot) and re-run with the same `--cache-file` to pick up where it left
off: floors already marked `scanned`/`empty` are reused as-is, and floors
that previously `error`ed are automatically retried. Pass `--fresh` to
discard the cache and start over.

A live progress line (floors done/total, rate, ETA) prints to the terminal
while it runs.

## Output

A CSV report (one row per floor that has at least one AP assigned, skipping
floors with nothing assigned) with columns:

`site_hierarchy, floor_id, status, aps_assigned, aps_positioned, aps_missing, missing_ap_names, error`

A console summary prints the totals: floors scanned, floors with no APs
assigned, floors that errored, floors fully positioned, floors missing at
least one AP position, and the overall AP counts (positioned vs. missing).

## How it scales

- Floors are fetched in one paginated pass (`site_design.get_sites`, 500 at a
  time) and then processed concurrently via a thread pool (`--workers`).
- Before doing any per-floor work, the assigned-device *count* endpoint is
  checked first; floors with zero assigned devices are skipped immediately
  (no positions call, no detail row) — this matters when scanning
  environments where a lot of floors have nothing assigned.
- A failure on one floor (API error, timeout, etc.) is logged and recorded
  as an `error` row rather than aborting the whole scan.
- API calls are paced client-side (`--rate-limit`, default 100/min) rather
  than relying on the controller to enforce its own limit - that shouldn't
  be assumed, and a quiet/unloaded controller can tolerate far more than a
  busy production one.
- Device inventory lookup runs in two passes instead of one call per floor:
  every floor's assigned-device IDs are collected first, then resolved
  together in small batches (`--inventory-batch-size`, default 25) before
  each floor finishes with its positions check. A device is only ever on
  one floor, so this isn't deduplicating lookups - it's packing more IDs
  into each `get_device_list` call so fewer, fuller round trips do the same
  work. The batch size is capped well below the API's 500-ID ceiling on
  purpose: a bigger batch means a bigger payload/slower call, and if one
  batch call fails even after retries, only the floors whose devices were
  in that batch get marked `error` - not the whole run.
- Progress is cached incrementally (see "Resuming a large scan" above), so a
  scan across thousands of floors can be interrupted and resumed instead of
  restarting from zero.
