#!/usr/bin/env python
"""Scan a Catalyst Center site hierarchy for APs assigned to a floor that have
no map placement (position) on that floor.

Compares, per floor:
  GET /dna/intent/api/v1/networkDevices/assignedToSite?siteId={floorId}
  GET /dna/intent/api/v2/floors/{floorId}/accessPointPositions
"""
import argparse
import csv
import json
import logging
import os
import random
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field

from dnacentersdk import api
from dnacentersdk.exceptions import ApiError

from dnac_config import DNAC, DNAC_USER, DNAC_PASSWORD, DNAC_VERSION

logger = logging.getLogger(__name__)

PAGE_SIZE = 500
# get_device_list's `id` filter accepts a comma-separated list up to this
# API ceiling - but that's not the default batch size to actually use (see
# DEFAULT_INVENTORY_BATCH_SIZE): a device belongs to exactly one floor, so
# packing more IDs per call doesn't save duplicate lookups, only round
# trips - and a call this big both costs more per-request (bigger payload,
# more backend work resolved in one shot) and, if it fails, takes out every
# floor whose devices were in that one batch.
INVENTORY_BATCH_SIZE = 500
# Small enough to keep a single failed/slow batch call from taking out too
# many floors at once, while still cutting per-floor inventory calls down
# by roughly this factor.
DEFAULT_INVENTORY_BATCH_SIZE = 25
# Server-side throttling on assignedToSite/accessPointPositions has been
# observed to be unenforced on at least one dev controller - that's not
# something to rely on. Pace client-side regardless, so a run against a
# busier/loaded controller doesn't hammer it just because a quiet one allowed it.
DEFAULT_RATE_LIMIT_PER_MIN = 100

ASSIGNED_DEVICE_ID_KEY = "deviceId"
DEVICE_MAC_KEYS = ("apEthernetMacAddress", "macAddress", "mac_address")
# accessPointPositions keys entries by the AP's wired Ethernet MAC, not its
# radio/base MAC - get_device_list returns both, under different fields, so
# apEthernetMacAddress must be tried first or every AP falsely shows missing.
DEVICE_NAME_KEYS = ("apName", "hostname", "deviceName", "name")
DEVICE_FAMILY_KEYS = ("family", "type")
POSITION_MAC_KEYS = ("macAddress", "mac_address")
KNOWN_AP_FAMILY_VALUES = {"unified ap", "unified aps"}
# Catalyst Center still returns a position record for an AP that has never
# been placed on the map - x/y are set to this sentinel rather than the
# entry being omitted, so presence alone doesn't mean "positioned".
UNPOSITIONED_COORD = -1.0

_family_warning_emitted = False


class RateLimiter:
    """Paces calls to at most `rate` per minute, shared across all threads.

    Strict spacing (not a bursty token bucket) - every acquire() reserves the
    next free slot under a lock, so total throughput is capped regardless of
    how many worker threads are calling it concurrently.
    """

    def __init__(self, rate_per_minute):
        self.min_interval = 60.0 / rate_per_minute if rate_per_minute > 0 else 0.0
        self.lock = threading.Lock()
        self.next_slot = time.monotonic()

    def acquire(self):
        if self.min_interval <= 0:
            return
        with self.lock:
            now = time.monotonic()
            wait = self.next_slot - now
            if wait < 0:
                wait = 0.0
                self.next_slot = now
            self.next_slot += self.min_interval
        if wait > 0:
            time.sleep(wait)


def _call_with_retry(fn, attempts=3, base_delay=1.0):
    """Run fn() with exponential backoff + jitter on ApiError. Re-raises the
    last error if every attempt fails - callers decide what a permanent
    failure means for their blast radius (e.g. one inventory batch vs. one
    floor), this just protects against transient blips."""
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except ApiError as exc:
            if attempt == attempts:
                raise
            delay = base_delay * (2 ** (attempt - 1)) + random.uniform(0, base_delay)
            logger.warning(
                "Retrying after ApiError (attempt %d/%d): %s - waiting %.1fs",
                attempt, attempts, exc, delay,
            )
            time.sleep(delay)


class ProgressReporter:
    """Live single-line progress indicator for a long-running floor scan."""

    def __init__(self, total, already_done=0):
        self.total = total
        self.done = already_done
        self.start = time.monotonic()
        self.lock = threading.Lock()
        self._enabled = sys.stderr.isatty()

    def advance(self):
        with self.lock:
            self.done += 1
            done, total = self.done, self.total
            elapsed = time.monotonic() - self.start
        rate_per_min = (done / elapsed * 60.0) if elapsed > 0 else 0.0
        remaining = total - done
        eta_min = (remaining / rate_per_min) if rate_per_min > 0 else 0.0
        pct = 100.0 * done / total if total else 100.0
        line = (
            f"\r  {done}/{total} floors ({pct:5.1f}%) "
            f"| {rate_per_min:5.1f} floors/min | ETA {eta_min:5.1f} min   "
        )
        if self._enabled:
            sys.stderr.write(line)
            sys.stderr.flush()
        elif done == total or done % 25 == 0:
            logger.info(line.strip())

    def finish(self):
        if self._enabled:
            sys.stderr.write("\n")
            sys.stderr.flush()


@dataclass
class FloorResult:
    floor_id: str
    site_hierarchy: str
    status: str  # "empty", "scanned", or "error"
    assigned: int = 0
    positioned: int = 0
    missing: list = field(default_factory=list)
    error: str = ""


# Floor scan results are cached one JSON object per line, keyed by floor_id,
# so an interrupted run can resume without re-scanning already-completed
# floors. "error" floors are re-attempted on resume since they never got a
# real result; "scanned"/"empty" ones are reused as-is.
RESUMABLE_STATUSES = ("scanned", "empty")


def _result_to_record(result):
    return {
        "floor_id": result.floor_id,
        "site_hierarchy": result.site_hierarchy,
        "status": result.status,
        "assigned": result.assigned,
        "positioned": result.positioned,
        "missing": result.missing,
        "error": result.error,
        "scanned_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }


def _record_to_result(record):
    return FloorResult(
        floor_id=record["floor_id"],
        site_hierarchy=record.get("site_hierarchy", record["floor_id"]),
        status=record.get("status", "error"),
        assigned=record.get("assigned", 0),
        positioned=record.get("positioned", 0),
        missing=record.get("missing", []),
        error=record.get("error", ""),
    )


def load_cache(cache_path):
    """Return {floor_id: FloorResult} for every previously-cached floor,
    last-write-wins (a floor can appear more than once if a prior run
    retried an errored floor)."""
    cache = {}
    if not cache_path or not os.path.exists(cache_path):
        return cache
    with open(cache_path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line:
                continue
            try:
                record = json.loads(line)
                cache[record["floor_id"]] = _record_to_result(record)
            except (json.JSONDecodeError, KeyError) as exc:
                logger.warning("Skipping malformed cache line %d: %s", lineno, exc)
    return cache


class CacheWriter:
    """Thread-safe append-only writer so completed floors survive a crash or
    Ctrl-C - each result is flushed immediately, not batched to the end."""

    def __init__(self, cache_path):
        self.lock = threading.Lock()
        self.file = None
        if cache_path:
            os.makedirs(os.path.dirname(cache_path) or ".", exist_ok=True)
            self.file = open(cache_path, "a")

    def write(self, result):
        if not self.file:
            return
        line = json.dumps(_result_to_record(result))
        with self.lock:
            self.file.write(line + "\n")
            self.file.flush()

    def close(self):
        if self.file:
            self.file.close()


def _first_present(d, keys):
    for key in keys:
        value = d.get(key)
        if value:
            return value
    return None


def _normalize_mac(mac):
    return mac.strip().lower() if mac else None


def _has_real_position(position_entry):
    position = position_entry.get("position") or {}
    x, y = position.get("x"), position.get("y")
    if x is None or y is None:
        return False
    return not (x == UNPOSITIONED_COORD and y == UNPOSITIONED_COORD)


def _is_access_point(device):
    global _family_warning_emitted
    family = _first_present(device, DEVICE_FAMILY_KEYS)
    if family is None:
        if not _family_warning_emitted:
            logger.warning(
                "Assigned device has no recognizable family/type field; "
                "including it without filtering (id=%s). Adjust DEVICE_FAMILY_KEYS "
                "if this controller uses a different field name.",
                device.get(ASSIGNED_DEVICE_ID_KEY),
            )
            _family_warning_emitted = True
        return True
    family_lower = str(family).lower()
    return family_lower in KNOWN_AP_FAMILY_VALUES or "access point" in family_lower


def _paginate(fetch_page, page_size=PAGE_SIZE, rate_limiter=None):
    offset = 1
    items = []
    while True:
        if rate_limiter:
            rate_limiter.acquire()
        response = fetch_page(limit=page_size, offset=offset)
        page = response.get("response", [])
        if not page:
            break
        items.extend(page)
        if len(page) < page_size:
            break
        offset += page_size
    return items


def fetch_all_floors(dnac_api, site_filter=None, page_size=PAGE_SIZE, rate_limiter=None):
    """Return every floor in the site hierarchy, optionally scoped to floors
    whose nameHierarchy contains site_filter as a substring."""
    floors = _paginate(
        lambda limit, offset: dnac_api.site_design.get_sites(
            limit=limit, offset=offset, type="floor"
        ),
        page_size,
        rate_limiter=rate_limiter,
    )
    if site_filter:
        floors = [f for f in floors if site_filter in f.get("nameHierarchy", "")]
    return floors


def _chunked(items, size):
    for i in range(0, len(items), size):
        yield items[i : i + size]


def fetch_device_inventory(dnac_api, device_ids, batch_size=DEFAULT_INVENTORY_BATCH_SIZE, rate_limiter=None):
    """assignedToSite only returns deviceId/siteId/siteType - no family, MAC, or
    hostname - so those have to be looked up separately from device inventory.

    Returns (inventory, failed_ids). A batch that still fails after retries
    doesn't abort the whole lookup - its IDs land in failed_ids so the
    caller can mark just the floors that depend on them, instead of one bad
    batch losing every floor waiting on inventory."""
    inventory = {}
    failed_ids = set()
    for batch in _chunked(sorted(set(device_ids)), batch_size):
        if rate_limiter:
            rate_limiter.acquire()
        try:
            response = _call_with_retry(
                lambda: dnac_api.devices.get_device_list(id=",".join(batch), limit=batch_size)
            )
        except ApiError as exc:
            logger.error("Inventory batch of %d device(s) failed after retries: %s", len(batch), exc)
            failed_ids.update(batch)
            continue
        for device in response.get("response", []):
            device_id = device.get("id")
            if device_id:
                inventory[device_id] = device
    return inventory, failed_ids


def fetch_floor_devices(dnac_api, floor, page_size=PAGE_SIZE, rate_limiter=None):
    """Stage 1 (per-floor, concurrent): count + assigned-devices list only.

    Returns a terminal FloorResult directly for a floor with nothing
    assigned (or that errors here) - otherwise returns a pending dict for
    stage 2, deferring inventory lookup so it can be batched globally
    across every floor's device IDs instead of one tiny call per floor.
    """
    floor_id = floor["id"]
    site_hierarchy = floor.get("nameHierarchy", floor_id)
    try:
        if rate_limiter:
            rate_limiter.acquire()
        count_response = dnac_api.site_design.get_site_assigned_network_devices_count(
            site_id=floor_id
        )
        assigned_total = count_response.get("response", 0) or 0
        if not assigned_total:
            return FloorResult(floor_id, site_hierarchy, status="empty")

        assigned_devices = _paginate(
            lambda limit, offset: dnac_api.site_design.get_site_assigned_network_devices(
                site_id=floor_id, limit=limit, offset=offset
            ),
            page_size,
            rate_limiter=rate_limiter,
        )
        device_ids = [
            d[ASSIGNED_DEVICE_ID_KEY] for d in assigned_devices if d.get(ASSIGNED_DEVICE_ID_KEY)
        ]
        return {
            "floor_id": floor_id,
            "site_hierarchy": site_hierarchy,
            "assigned_devices": assigned_devices,
            "device_ids": device_ids,
        }
    except ApiError as exc:
        logger.error("API error on floor %s (%s): %s", site_hierarchy, floor_id, exc)
        return FloorResult(floor_id, site_hierarchy, status="error", error=str(exc))
    except Exception as exc:  # noqa: BLE001 - keep the scan going for one bad floor
        logger.error("Unexpected error on floor %s (%s): %s", site_hierarchy, floor_id, exc)
        return FloorResult(floor_id, site_hierarchy, status="error", error=str(exc))


def finish_floor_scan(dnac_api, pending, inventory, page_size=PAGE_SIZE, rate_limiter=None):
    """Stage 2 (per-floor, concurrent): enrich with the globally-resolved
    inventory dict, filter to APs, fetch positions, and diff."""
    floor_id = pending["floor_id"]
    site_hierarchy = pending["site_hierarchy"]
    try:
        enriched_devices = [
            {**d, **inventory.get(d.get(ASSIGNED_DEVICE_ID_KEY), {})}
            for d in pending["assigned_devices"]
        ]

        ap_devices = [d for d in enriched_devices if _is_access_point(d)]
        if not ap_devices:
            return FloorResult(floor_id, site_hierarchy, status="empty")

        positioned = _paginate(
            lambda limit, offset: dnac_api.site_design.get_access_points_positions(
                floor_id=floor_id, limit=limit, offset=offset
            ),
            page_size,
            rate_limiter=rate_limiter,
        )
        positioned_macs = {
            _normalize_mac(_first_present(p, POSITION_MAC_KEYS))
            for p in positioned
            if _has_real_position(p)
        }
        positioned_macs.discard(None)

        missing = []
        positioned_count = 0
        for device in ap_devices:
            mac = _normalize_mac(_first_present(device, DEVICE_MAC_KEYS))
            name = (
                _first_present(device, DEVICE_NAME_KEYS)
                or mac
                or device.get(ASSIGNED_DEVICE_ID_KEY, "unknown")
            )
            if mac and mac in positioned_macs:
                positioned_count += 1
            else:
                missing.append(name)

        return FloorResult(
            floor_id,
            site_hierarchy,
            status="scanned",
            assigned=len(ap_devices),
            positioned=positioned_count,
            missing=missing,
        )
    except ApiError as exc:
        logger.error("API error on floor %s (%s): %s", site_hierarchy, floor_id, exc)
        return FloorResult(floor_id, site_hierarchy, status="error", error=str(exc))
    except Exception as exc:  # noqa: BLE001 - keep the scan going for one bad floor
        logger.error("Unexpected error on floor %s (%s): %s", site_hierarchy, floor_id, exc)
        return FloorResult(floor_id, site_hierarchy, status="error", error=str(exc))


def write_report(results, output_path):
    os.makedirs(os.path.dirname(output_path) or ".", exist_ok=True)
    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "site_hierarchy",
                "floor_id",
                "status",
                "aps_assigned",
                "aps_positioned",
                "aps_missing",
                "missing_ap_names",
                "error",
            ]
        )
        for r in results:
            if r.status == "empty":
                continue
            writer.writerow(
                [
                    r.site_hierarchy,
                    r.floor_id,
                    r.status,
                    r.assigned,
                    r.positioned,
                    len(r.missing),
                    "; ".join(r.missing),
                    r.error,
                ]
            )


def print_summary(results):
    scanned = [r for r in results if r.status == "scanned"]
    empty = [r for r in results if r.status == "empty"]
    errored = [r for r in results if r.status == "error"]
    fully_positioned = [r for r in scanned if not r.missing]
    with_missing = [r for r in scanned if r.missing]
    total_missing = sum(len(r.missing) for r in scanned)
    total_positioned = sum(r.positioned for r in scanned)

    print()
    print("=== Missing AP Placement Summary ===")
    print(f"Floors scanned:                 {len(results)}")
    print(f"  Floors with no APs assigned:  {len(empty)}")
    print(f"  Floors that errored:         {len(errored)}")
    print(f"  Floors with APs assigned:     {len(scanned)}")
    print(f"    Fully positioned:           {len(fully_positioned)}")
    print(f"    Missing >=1 AP position:    {len(with_missing)}")
    print(f"Total APs correctly positioned:  {total_positioned}")
    print(f"Total APs missing placement:     {total_missing}")
    if errored:
        print()
        print("Floors that could not be scanned (see CSV/log for details):")
        for r in errored:
            print(f"  - {r.site_hierarchy} ({r.floor_id}): {r.error}")


def parse_args():
    parser = argparse.ArgumentParser(
        description="Find APs assigned to a floor with no map placement."
    )
    parser.add_argument("--dnac", default=DNAC, help="Catalyst Center hostname or IP")
    parser.add_argument(
        "--version",
        default=DNAC_VERSION,
        help="Catalyst Center API version to target (default: %(default)s)",
    )
    parser.add_argument(
        "--site",
        default=None,
        help="Only scan floors whose site hierarchy contains this substring "
        "(e.g. 'Global/HQ'). Omit to scan every floor in the hierarchy.",
    )
    parser.add_argument(
        "--workers",
        type=int,
        default=10,
        help="Number of floors to process concurrently (default: 10)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default=None,
        help="CSV output path (default: output/<dnac>/missing_ap_report_<timestamp>.csv)",
    )
    parser.add_argument(
        "--verify-ssl",
        action="store_true",
        help="Verify the Catalyst Center TLS certificate (default: off)",
    )
    parser.add_argument(
        "--rate-limit",
        type=int,
        default=DEFAULT_RATE_LIMIT_PER_MIN,
        help="Max API calls per minute, paced across all workers (default: %(default)s). "
        "0 disables pacing entirely - not recommended on a controller you don't control.",
    )
    parser.add_argument(
        "--inventory-batch-size",
        type=int,
        default=DEFAULT_INVENTORY_BATCH_SIZE,
        help="Device IDs per batched get_device_list call during the global inventory "
        "lookup (default: %(default)s; recommended range 20-50). The API allows up to "
        "500, but a device is only ever on one floor, so bigger batches don't save "
        "duplicate lookups - they just raise the payload size and the blast radius "
        "of one failed call.",
    )
    parser.add_argument(
        "--cache-file",
        default=None,
        help="Path to the incremental scan-progress cache (default: "
        "output/<dnac>/missing_ap_scan_cache.jsonl). Re-running with the same cache file "
        "resumes: already-scanned floors are skipped, errored floors are retried.",
    )
    parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignore any existing cache file and rescan every floor from scratch "
        "(the cache file itself is overwritten, not merged).",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main():
    args = parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s",
    )

    output_path = args.output
    if not output_path:
        timestamp = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
        output_path = os.path.join("output", args.dnac, f"missing_ap_report_{timestamp}.csv")

    cache_path = args.cache_file or os.path.join("output", args.dnac, "missing_ap_scan_cache.jsonl")
    if args.fresh and os.path.exists(cache_path):
        os.remove(cache_path)
    cached_results = {} if args.fresh else load_cache(cache_path)

    dnac_api = api.DNACenterAPI(
        base_url="https://{}:443".format(args.dnac),
        username=DNAC_USER,
        password=DNAC_PASSWORD,
        verify=args.verify_ssl,
        version=args.version,
    )
    rate_limiter = RateLimiter(args.rate_limit) if args.rate_limit > 0 else None
    if rate_limiter:
        logger.info("Pacing API calls to %d/min", args.rate_limit)

    logger.info("Fetching floors from %s%s", args.dnac, f" (filter: {args.site})" if args.site else "")
    floors = fetch_all_floors(dnac_api, site_filter=args.site, rate_limiter=rate_limiter)
    logger.info("Found %d floor(s) to scan", len(floors))
    if not floors:
        print("No floors found matching the given criteria.")
        sys.exit(0)

    reusable = {
        f["id"]: cached_results[f["id"]]
        for f in floors
        if f["id"] in cached_results and cached_results[f["id"]].status in RESUMABLE_STATUSES
    }
    todo_floors = [f for f in floors if f["id"] not in reusable]
    if reusable:
        logger.info(
            "Resuming from cache: %d/%d floors already scanned, %d remaining",
            len(reusable), len(floors), len(todo_floors),
        )

    results_by_id = dict(reusable)
    cache_writer = CacheWriter(cache_path)
    progress = ProgressReporter(total=len(floors), already_done=len(reusable))

    def _finalize(result):
        results_by_id[result.floor_id] = result
        cache_writer.write(result)
        progress.advance()

    try:
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            # Pass 1: per-floor count + assigned-devices list. Floors with
            # nothing assigned (or that error here) are already terminal.
            pending = []
            future_to_floor = {
                executor.submit(fetch_floor_devices, dnac_api, floor, rate_limiter=rate_limiter): floor
                for floor in todo_floors
            }
            for future in as_completed(future_to_floor):
                result = future.result()
                if isinstance(result, FloorResult):
                    _finalize(result)
                else:
                    pending.append(result)

            # One global batch step: every remaining floor's device IDs get
            # resolved together in small batches, instead of one call per
            # floor - see DEFAULT_INVENTORY_BATCH_SIZE for why the batch
            # size is capped well below the API's 500-ID ceiling.
            if pending:
                all_device_ids = sorted({d for p in pending for d in p["device_ids"]})
                logger.info(
                    "Resolving device inventory for %d floor(s), %d unique device(s) "
                    "(batch size %d)",
                    len(pending), len(all_device_ids), args.inventory_batch_size,
                )
                inventory, failed_ids = fetch_device_inventory(
                    dnac_api,
                    all_device_ids,
                    batch_size=args.inventory_batch_size,
                    rate_limiter=rate_limiter,
                )

                # A batch that failed even after retries only takes out the
                # floors whose devices were in it - not the whole run.
                if failed_ids:
                    still_pending = []
                    for p in pending:
                        affected = failed_ids.intersection(p["device_ids"])
                        if affected:
                            _finalize(FloorResult(
                                p["floor_id"], p["site_hierarchy"], status="error",
                                error=f"device inventory lookup failed for {len(affected)}/"
                                f"{len(p['device_ids'])} assigned device(s) after retries",
                            ))
                        else:
                            still_pending.append(p)
                    pending = still_pending

            # Pass 2: per-floor AP filtering, positions fetch, and diff,
            # using the now-fully-resolved inventory dict.
            if pending:
                future_to_pending = {
                    executor.submit(
                        finish_floor_scan, dnac_api, p, inventory, rate_limiter=rate_limiter
                    ): p
                    for p in pending
                }
                for future in as_completed(future_to_pending):
                    _finalize(future.result())
    finally:
        progress.finish()
        cache_writer.close()

    results = [results_by_id[f["id"]] for f in floors]

    write_report(results, output_path)
    print(f"\nDetailed report written to: {output_path}")
    print(f"Scan cache written to: {cache_path}")
    print_summary(results)


if __name__ == "__main__":
    main()
