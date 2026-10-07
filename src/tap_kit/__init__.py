import json
import sys
import time
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional
import requests
from singer import get_bookmark, write_record, write_schema, get_logger, parse_args
from singer.catalog import Catalog, CatalogEntry, Schema


REQUIRED_CONFIG_KEYS = ["api_key"]
CONFIG = {}
STATE = {}
LOGGER = get_logger()

DEFAULT_PER_PAGE = 500
MAX_RETRIES = 5
REQUEST_TIMEOUT = 60

STREAM_CONFIG = {
    "broadcasts": {
        "replication_method": "INCREMENTAL",
        "replication_key": "created_at",
        "key_properties": ["id"],
    },
    "broadcast_stats": {
        "replication_method": "FULL_TABLE",
        "replication_key": None,
        "key_properties": ["id", "synced_at"],
    },
    "subscribers": {
        "replication_method": "INCREMENTAL",
        "replication_key": "created_at",
        "key_properties": ["id"],
    },
    "subscriber_stats": {
        "replication_method": "FULL_TABLE",
        "replication_key": None,
        "key_properties": ["id", "synced_at"],
    },
}


def utc_now() -> str:
    """Current UTC time as an ISO 8601 string, used to stamp stat snapshots."""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


class KitAPI:
    """Client for the Kit v4 API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.kit.com/v4"
        self.headers = {
            "X-Kit-Api-Key": api_key,
            "Accept": "application/json",
        }

    def _get(
        self, path: str, params: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """GET a path, retrying on rate limit (429), server errors (5xx), and
        transient network failures with exponential backoff."""
        url = f"{self.base_url}{path}"

        for attempt in range(MAX_RETRIES + 1):
            last_attempt = attempt == MAX_RETRIES
            try:
                response = requests.get(
                    url, headers=self.headers, params=params, timeout=REQUEST_TIMEOUT
                )
            except requests.exceptions.RequestException as e:
                if last_attempt:
                    raise
                wait = 2**attempt
                LOGGER.warning(
                    f"Request error on {path}: {e}, retrying in {wait}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            if response.status_code == 429 and not last_attempt:
                retry_after = response.headers.get("Retry-After")
                wait = float(retry_after) if retry_after else 2**attempt
                LOGGER.warning(
                    f"Rate limited on {path}, retrying in {wait}s "
                    f"(attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            if response.status_code >= 500 and not last_attempt:
                wait = 2**attempt
                LOGGER.warning(
                    f"Server error {response.status_code} on {path}, retrying in "
                    f"{wait}s (attempt {attempt + 1}/{MAX_RETRIES})"
                )
                time.sleep(wait)
                continue

            response.raise_for_status()
            return response.json()

    def list_broadcasts(
        self, after: Optional[str] = None, per_page: int = DEFAULT_PER_PAGE
    ) -> Dict[str, Any]:
        """Fetch a page of broadcasts."""
        params = {"per_page": per_page}
        if after:
            params["after"] = after
        return self._get("/broadcasts", params)

    def list_broadcast_stats(
        self, after: Optional[str] = None, per_page: int = DEFAULT_PER_PAGE
    ) -> Dict[str, Any]:
        """Fetch a page of broadcast stats (batch endpoint)."""
        params = {"per_page": per_page}
        if after:
            params["after"] = after
        return self._get("/broadcasts/stats", params)

    def list_subscribers(
        self, after: Optional[str] = None, per_page: int = DEFAULT_PER_PAGE
    ) -> Dict[str, Any]:
        """Fetch a page of subscribers."""
        params = {"per_page": per_page}
        if after:
            params["after"] = after
        return self._get("/subscribers", params)

    def get_subscriber_stats(self, subscriber_id: int) -> Dict[str, Any]:
        """Fetch stats for a single subscriber."""
        return self._get(f"/subscribers/{subscriber_id}/stats")


def load_schemas() -> Dict[str, Schema]:
    """Define schemas for each stream."""
    schemas = {}

    broadcast_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "publication_id": {"type": ["null", "integer"]},
            "created_at": {"type": "string", "format": "date-time"},
            "subject": {"type": ["null", "string"]},
            "preview_text": {"type": ["null", "string"]},
            "description": {"type": ["null", "string"]},
            "content": {"type": ["null", "string"]},
            "public": {"type": ["null", "boolean"]},
            "published_at": {"type": ["null", "string"], "format": "date-time"},
            "send_at": {"type": ["null", "string"], "format": "date-time"},
            "thumbnail_alt": {"type": ["null", "string"]},
            "thumbnail_url": {"type": ["null", "string"]},
            "public_url": {"type": ["null", "string"]},
            "email_address": {"type": ["null", "string"]},
            "email_template": {"type": ["null", "object"]},
            "subscriber_filter": {"type": ["null", "array", "object"]},
            "status": {"type": ["null", "string"]},
        },
    }

    broadcast_stats_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "synced_at": {"type": "string", "format": "date-time"},
            "recipients": {"type": ["null", "integer"]},
            "open_rate": {"type": ["null", "number"]},
            "emails_opened": {"type": ["null", "integer"]},
            "click_rate": {"type": ["null", "number"]},
            "unsubscribe_rate": {"type": ["null", "number"]},
            "unsubscribes": {"type": ["null", "integer"]},
            "total_clicks": {"type": ["null", "integer"]},
            "show_total_clicks": {"type": ["null", "boolean"]},
            "status": {"type": ["null", "string"]},
            "progress": {"type": ["null", "number"]},
            "open_tracking_disabled": {"type": ["null", "boolean"]},
            "click_tracking_disabled": {"type": ["null", "boolean"]},
        },
    }

    subscriber_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "first_name": {"type": ["null", "string"]},
            "email_address": {"type": "string"},
            "state": {"type": ["null", "string"]},
            "created_at": {"type": "string", "format": "date-time"},
            "fields": {"type": ["null", "object"]},
        },
    }

    subscriber_stats_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "synced_at": {"type": "string", "format": "date-time"},
            "sent": {"type": ["null", "integer"]},
            "opened": {"type": ["null", "integer"]},
            "clicked": {"type": ["null", "integer"]},
            "bounced": {"type": ["null", "integer"]},
            "open_rate": {"type": ["null", "number"]},
            "click_rate": {"type": ["null", "number"]},
            "last_sent": {"type": ["null", "string"], "format": "date-time"},
            "last_opened": {"type": ["null", "string"], "format": "date-time"},
            "last_clicked": {"type": ["null", "string"], "format": "date-time"},
            "sends_since_last_open": {"type": ["null", "integer"]},
            "sends_since_last_click": {"type": ["null", "integer"]},
        },
    }

    schemas["broadcasts"] = Schema.from_dict(broadcast_schema)
    schemas["broadcast_stats"] = Schema.from_dict(broadcast_stats_schema)
    schemas["subscribers"] = Schema.from_dict(subscriber_schema)
    schemas["subscriber_stats"] = Schema.from_dict(subscriber_stats_schema)

    return schemas


def discover() -> Catalog:
    """Discover available streams and schemas."""
    schemas = load_schemas()
    streams = []

    for stream_id, schema in schemas.items():
        stream_settings = STREAM_CONFIG[stream_id]
        replication_method = stream_settings["replication_method"]
        replication_key = stream_settings["replication_key"]
        key_properties = stream_settings["key_properties"]

        metadata_entry = {
            "inclusion": "available",
            "table-key-properties": key_properties,
            "schema-name": stream_id,
        }
        if replication_key:
            metadata_entry["valid-replication-keys"] = [replication_key]

        stream_metadata = [{"metadata": metadata_entry, "breadcrumb": []}]

        catalog_entry = CatalogEntry(
            stream=stream_id,
            tap_stream_id=stream_id,
            schema=schema,
            key_properties=key_properties,
            replication_key=replication_key,
            metadata=stream_metadata,
            replication_method=replication_method,
        )

        streams.append(catalog_entry)

    return Catalog(streams)


def sync_broadcasts(
    config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog
) -> None:
    """Sync broadcasts stream (entity records, no stats)."""
    api = KitAPI(config["api_key"])

    broadcasts_catalog = catalog.get_stream("broadcasts")
    if not broadcasts_catalog:
        LOGGER.error("Broadcasts stream not found in catalog")
        return

    get_bookmark(
        state,
        "broadcasts",
        broadcasts_catalog.replication_key,
        config.get("start_date"),
    )

    write_schema(
        "broadcasts",
        broadcasts_catalog.schema.to_dict(),
        key_properties=broadcasts_catalog.key_properties,
    )

    after = None
    total_records = 0

    while True:
        response = api.list_broadcasts(after=after)
        broadcasts = response.get("broadcasts") or []

        for broadcast in broadcasts:
            write_record("broadcasts", broadcast)
            total_records += 1

        pagination = response.get("pagination") or {}
        if not pagination.get("has_next_page"):
            break
        after = pagination.get("end_cursor")
        if not after:
            break

    LOGGER.info(f"Synced {total_records} broadcast records")


def sync_broadcast_stats(
    config: Dict[str, Any], catalog: Catalog, synced_at: str
) -> None:
    """Sync broadcast_stats snapshot stream via the batch stats endpoint."""
    api = KitAPI(config["api_key"])

    stats_catalog = catalog.get_stream("broadcast_stats")
    if not stats_catalog:
        LOGGER.error("Broadcast_stats stream not found in catalog")
        return

    write_schema(
        "broadcast_stats",
        stats_catalog.schema.to_dict(),
        key_properties=stats_catalog.key_properties,
    )

    after = None
    total_records = 0

    while True:
        response = api.list_broadcast_stats(after=after)
        broadcasts = response.get("broadcasts") or []

        for broadcast in broadcasts:
            stats = broadcast.get("stats") or {}
            record = {"id": broadcast["id"], "synced_at": synced_at, **stats}
            write_record("broadcast_stats", record)
            total_records += 1

        pagination = response.get("pagination") or {}
        if not pagination.get("has_next_page"):
            break
        after = pagination.get("end_cursor")
        if not after:
            break

    LOGGER.info(f"Synced {total_records} broadcast_stats records")


def sync_subscribers(
    config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog
) -> List[int]:
    """Sync subscribers stream. Returns the list of subscriber ids seen."""
    api = KitAPI(config["api_key"])

    subscribers_catalog = catalog.get_stream("subscribers")
    if not subscribers_catalog:
        LOGGER.error("Subscribers stream not found in catalog")
        return []

    get_bookmark(
        state,
        "subscribers",
        subscribers_catalog.replication_key,
        config.get("start_date"),
    )

    write_schema(
        "subscribers",
        subscribers_catalog.schema.to_dict(),
        key_properties=subscribers_catalog.key_properties,
    )

    after = None
    total_records = 0
    subscriber_ids: List[int] = []

    while True:
        response = api.list_subscribers(after=after)
        subscribers = response.get("subscribers") or []

        for subscriber in subscribers:
            write_record("subscribers", subscriber)
            subscriber_ids.append(subscriber["id"])
            total_records += 1

        pagination = response.get("pagination") or {}
        if not pagination.get("has_next_page"):
            break
        after = pagination.get("end_cursor")
        if not after:
            break

    LOGGER.info(f"Synced {total_records} subscriber records")
    return subscriber_ids


def sync_subscriber_stats(
    config: Dict[str, Any],
    catalog: Catalog,
    subscriber_ids: List[int],
    synced_at: str,
) -> None:
    """Sync subscriber_stats snapshot stream (one API call per subscriber)."""
    api = KitAPI(config["api_key"])

    stats_catalog = catalog.get_stream("subscriber_stats")
    if not stats_catalog:
        LOGGER.error("Subscriber_stats stream not found in catalog")
        return

    write_schema(
        "subscriber_stats",
        stats_catalog.schema.to_dict(),
        key_properties=stats_catalog.key_properties,
    )

    total_records = 0
    failed_ids: List[int] = []

    for subscriber_id in subscriber_ids:
        try:
            response = api.get_subscriber_stats(subscriber_id)
        except requests.exceptions.RequestException as e:
            LOGGER.warning(f"Error fetching stats for subscriber {subscriber_id}: {e}")
            failed_ids.append(subscriber_id)
            continue

        subscriber = response.get("subscriber") or {}
        stats = subscriber.get("stats") or {}
        record = {
            "id": subscriber.get("id", subscriber_id),
            "synced_at": synced_at,
            **stats,
        }
        write_record("subscriber_stats", record)
        total_records += 1

    LOGGER.info(
        f"Synced {total_records} subscriber_stats records"
        + (
            f"; {len(failed_ids)} failed after retries: {failed_ids}"
            if failed_ids
            else ""
        )
    )


def sync(config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog) -> None:
    """Sync all selected streams."""
    stream_ids = {stream.tap_stream_id for stream in catalog.streams}
    synced_at = utc_now()

    if "broadcasts" in stream_ids:
        sync_broadcasts(config, state, catalog)

    if "broadcast_stats" in stream_ids:
        sync_broadcast_stats(config, catalog, synced_at)

    if "subscribers" in stream_ids or "subscriber_stats" in stream_ids:
        subscriber_ids = sync_subscribers(config, state, catalog)
        if "subscriber_stats" in stream_ids:
            sync_subscriber_stats(config, catalog, subscriber_ids, synced_at)


def main() -> None:
    """Main entry point."""
    args = parse_args(REQUIRED_CONFIG_KEYS)

    config = args.config
    state = args.state
    catalog = args.catalog if args.catalog else discover()

    if args.discover:
        catalog = discover()
        json.dump(catalog.to_dict(), sys.stdout, indent=2)
    else:
        sync(config, state, catalog)


if __name__ == "__main__":
    main()
