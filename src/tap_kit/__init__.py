import json
import os
import sys
from typing import Dict, Any
import requests
from singer import get_bookmark, write_record, write_schema, get_logger, parse_args
from singer.catalog import Catalog, CatalogEntry, Schema
from singer.utils import strptime_to_utc


REQUIRED_CONFIG_KEYS = ["api_key"]
CONFIG = {}
STATE = {}
LOGGER = get_logger()


class KitAPI:
    """Client for the Kit API"""

    def __init__(self, api_key: str):
        self.api_key = api_key
        self.base_url = "https://api.kit.com/v4"
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def get_broadcasts(self, page: int = 1) -> Dict[str, Any]:
        """Fetch broadcasts from Kit API"""
        url = f"{self.base_url}/broadcasts"
        params = {"page": page}

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()

        return response.json()

    def get_broadcast_stats(self, broadcast_id: int) -> Dict[str, Any]:
        """Fetch stats for a specific broadcast from Kit API"""
        url = f"{self.base_url}/broadcasts/{broadcast_id}/stats"

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()

    def get_subscribers(self, page: int = 1) -> Dict[str, Any]:
        """Fetch subscribers from Kit API"""
        url = f"{self.base_url}/subscribers"
        params = {"page": page}

        response = requests.get(url, headers=self.headers, params=params)
        response.raise_for_status()

        return response.json()

    def get_subscriber_stats(self, subscriber_id: int) -> Dict[str, Any]:
        """Fetch stats for a specific subscriber from Kit API"""
        url = f"{self.base_url}/subscribers/{subscriber_id}/stats"

        response = requests.get(url, headers=self.headers)
        response.raise_for_status()

        return response.json()


def get_abs_path(path: str) -> str:
    """Get absolute path"""
    return os.path.join(os.path.dirname(os.path.realpath(__file__)), path)


def load_schemas() -> Dict[str, Schema]:
    """Load schemas from schemas folder"""
    schemas = {}

    # Define broadcast schema based on Kit API documentation
    broadcast_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "created_at": {"type": "string", "format": "date-time"},
            "subject": {"type": "string"},
            "description": {"type": "string"},
            "content": {"type": "string"},
            "public": {"type": "boolean"},
            "published_at": {"type": ["null", "string"], "format": "date-time"},
            "send_at": {"type": ["null", "string"], "format": "date-time"},
            "thumbnail_alt": {"type": ["null", "string"]},
            "thumbnail_url": {"type": ["null", "string"]},
            "email_address": {"type": "string"},
            "email_layout_template": {"type": "string"},
            "stats": {
                "type": ["null", "object"],
                "properties": {
                    "recipients": {"type": ["null", "integer"]},
                    "open_rate": {"type": ["null", "number"]},
                    "click_rate": {"type": ["null", "number"]},
                    "unsubscribes": {"type": ["null", "integer"]},
                    "total_clicks": {"type": ["null", "integer"]},
                    "show_total_clicks": {"type": ["null", "boolean"]},
                    "status": {"type": ["null", "string"]},
                    "progress": {"type": ["null", "number"]},
                },
            },
        },
    }

    # Define subscriber schema based on Kit API documentation
    subscriber_schema = {
        "type": "object",
        "properties": {
            "id": {"type": "integer"},
            "first_name": {"type": ["null", "string"]},
            "email_address": {"type": "string"},
            "state": {"type": "string"},
            "created_at": {"type": "string", "format": "date-time"},
            "fields": {"type": "object"},
            "stats": {
                "type": ["null", "object"],
                "properties": {
                    "total_opens": {"type": ["null", "integer"]},
                    "total_clicks": {"type": ["null", "integer"]},
                    "average_open_rate": {"type": ["null", "number"]},
                    "average_click_rate": {"type": ["null", "number"]},
                },
            },
        },
    }

    schemas["broadcasts"] = Schema.from_dict(broadcast_schema)
    schemas["subscribers"] = Schema.from_dict(subscriber_schema)

    return schemas


def discover() -> Catalog:
    """Discover available streams and schemas"""
    schemas = load_schemas()
    streams = []

    for stream_id, schema in schemas.items():
        key_properties = ["id"]
        replication_key = "created_at"

        stream_metadata = [
            {
                "metadata": {
                    "inclusion": "available",
                    "table-key-properties": key_properties,
                    "valid-replication-keys": [replication_key],
                    "schema-name": stream_id,
                },
                "breadcrumb": [],
            }
        ]

        catalog_entry = CatalogEntry(
            stream=stream_id,
            tap_stream_id=stream_id,
            schema=schema,
            key_properties=key_properties,
            replication_key=replication_key,
            metadata=stream_metadata,
            replication_method="INCREMENTAL",
        )

        streams.append(catalog_entry)

    return Catalog(streams)


def sync_broadcasts(
    config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog
) -> None:
    """Sync broadcasts stream"""
    api = KitAPI(config["api_key"])

    # Get the broadcasts stream from catalog
    broadcasts_catalog = catalog.get_stream("broadcasts")
    if not broadcasts_catalog:
        LOGGER.error("Broadcasts stream not found in catalog")
        return

    bookmark_column = broadcasts_catalog.replication_key

    start = get_bookmark(
        state, "broadcasts", bookmark_column, config.get("start_date")
    )
    if start:
        start = strptime_to_utc(start)

    write_schema(
        "broadcasts", broadcasts_catalog.schema.to_dict(), key_properties=["id"]
    )

    page = 1
    total_records = 0

    while True:
        try:
            response = api.get_broadcasts(page=page)

            if not response.get("broadcasts"):
                break

            broadcasts = response["broadcasts"]

            for broadcast in broadcasts:
                # Fetch stats for each broadcast
                try:
                    stats_response = api.get_broadcast_stats(broadcast["id"])
                    broadcast["stats"] = stats_response.get("stats")
                except requests.exceptions.RequestException as e:
                    LOGGER.warning(f"Error fetching stats for broadcast {broadcast['id']}: {e}")
                    broadcast["stats"] = None

                # Write record
                write_record("broadcasts", broadcast)
                total_records += 1

            # Check if we have more pages
            total_broadcasts = response.get("total_broadcasts", 0)
            page_info = response.get("page", 1)
            
            if page * 50 >= total_broadcasts:  # Kit API default page size is 50
                break

            page += 1

        except requests.exceptions.RequestException as e:
            LOGGER.error(f"Error fetching broadcasts: {e}")
            break

    LOGGER.info(f"Synced {total_records} broadcast records")


def sync_subscribers(
    config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog
) -> None:
    """Sync subscribers stream"""
    api = KitAPI(config["api_key"])

    # Get the subscribers stream from catalog
    subscribers_catalog = catalog.get_stream("subscribers")
    if not subscribers_catalog:
        LOGGER.error("Subscribers stream not found in catalog")
        return

    bookmark_column = subscribers_catalog.replication_key

    start = get_bookmark(
        state, "subscribers", bookmark_column, config.get("start_date")
    )
    if start:
        start = strptime_to_utc(start)

    write_schema(
        "subscribers", subscribers_catalog.schema.to_dict(), key_properties=["id"]
    )

    page = 1
    total_records = 0

    while True:
        try:
            response = api.get_subscribers(page=page)

            if not response.get("subscribers"):
                break

            subscribers = response["subscribers"]

            for subscriber in subscribers:
                # Fetch stats for each subscriber
                try:
                    stats_response = api.get_subscriber_stats(subscriber["id"])
                    subscriber["stats"] = stats_response.get("stats")
                except requests.exceptions.RequestException as e:
                    LOGGER.warning(f"Error fetching stats for subscriber {subscriber['id']}: {e}")
                    subscriber["stats"] = None

                # Write record
                write_record("subscribers", subscriber)
                total_records += 1

            # Check if we have more pages
            total_subscribers = response.get("total_subscribers", 0)
            page_info = response.get("page", 1)
            
            if page * 50 >= total_subscribers:  # Kit API default page size is 50
                break

            page += 1

        except requests.exceptions.RequestException as e:
            LOGGER.error(f"Error fetching subscribers: {e}")
            break

    LOGGER.info(f"Synced {total_records} subscriber records")


def sync(config: Dict[str, Any], state: Dict[str, Any], catalog: Catalog) -> None:
    """Sync all streams"""
    for stream in catalog.streams:
        if stream.tap_stream_id == "broadcasts":
            sync_broadcasts(config, state, catalog)
        elif stream.tap_stream_id == "subscribers":
            sync_subscribers(config, state, catalog)


def main() -> None:
    """Main entry point"""
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
