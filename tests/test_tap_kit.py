#!/usr/bin/env python3
"""Unit tests for tap-kit"""

import sys
import os
from unittest.mock import Mock, patch, call
import pytest
import requests

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

from tap_kit import (
    KitAPI,
    discover,
    sync_broadcasts,
    sync_broadcast_stats,
    sync_subscribers,
    sync_subscriber_stats,
)


class TestKitAPI:
    """Test the KitAPI client"""

    def test_init(self):
        api = KitAPI("test_api_key")
        assert api.api_key == "test_api_key"
        assert api.base_url == "https://api.kit.com/v4"
        assert api.headers["X-Kit-Api-Key"] == "test_api_key"
        assert api.headers["Accept"] == "application/json"
        assert "Authorization" not in api.headers

    @patch("requests.get")
    def test_list_broadcasts_passes_cursor(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"broadcasts": [], "pagination": {}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        api = KitAPI("test_key")
        api.list_broadcasts(after="CURSOR123", per_page=100)

        mock_get.assert_called_once_with(
            "https://api.kit.com/v4/broadcasts",
            headers=api.headers,
            params={"per_page": 100, "after": "CURSOR123"},
        )

    @patch("requests.get")
    def test_list_broadcasts_omits_after_when_none(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"broadcasts": [], "pagination": {}}
        mock_response.raise_for_status.return_value = None
        mock_get.return_value = mock_response

        api = KitAPI("test_key")
        api.list_broadcasts()

        _, kwargs = mock_get.call_args
        assert "after" not in kwargs["params"]

    @patch("tap_kit.time.sleep", return_value=None)
    @patch("requests.get")
    def test_retries_on_rate_limit(self, mock_get, mock_sleep):
        rate_limited = Mock()
        rate_limited.status_code = 429
        rate_limited.headers = {"Retry-After": "1"}

        ok = Mock()
        ok.status_code = 200
        ok.headers = {}
        ok.json.return_value = {"subscriber": {"id": 1, "stats": {}}}
        ok.raise_for_status.return_value = None

        mock_get.side_effect = [rate_limited, ok]

        api = KitAPI("test_key")
        result = api.get_subscriber_stats(1)

        assert mock_get.call_count == 2
        mock_sleep.assert_called_once_with(1.0)
        assert result == {"subscriber": {"id": 1, "stats": {}}}


class TestDiscover:
    """Test stream discovery"""

    def test_discover_streams(self):
        catalog = discover()
        stream_ids = {s.tap_stream_id for s in catalog.streams}
        assert stream_ids == {
            "broadcasts",
            "broadcast_stats",
            "subscribers",
            "subscriber_stats",
        }

    def test_replication_methods(self):
        catalog = discover()
        by_id = {s.tap_stream_id: s for s in catalog.streams}

        assert by_id["broadcasts"].replication_method == "INCREMENTAL"
        assert by_id["broadcasts"].replication_key == "created_at"
        assert by_id["broadcast_stats"].replication_method == "FULL_TABLE"
        assert by_id["broadcast_stats"].replication_key is None
        assert by_id["subscribers"].replication_method == "INCREMENTAL"
        assert by_id["subscriber_stats"].replication_method == "FULL_TABLE"
        assert by_id["subscriber_stats"].replication_key is None

    def test_key_properties(self):
        catalog = discover()
        by_id = {s.tap_stream_id: s for s in catalog.streams}

        assert by_id["broadcasts"].key_properties == ["id"]
        assert by_id["subscribers"].key_properties == ["id"]
        assert by_id["broadcast_stats"].key_properties == ["id", "synced_at"]
        assert by_id["subscriber_stats"].key_properties == ["id", "synced_at"]


class TestSyncBroadcasts:
    """Test broadcasts entity sync (no inline stats)"""

    def setup_method(self):
        self.config = {"api_key": "test_key"}
        self.state = {}
        self.catalog = discover()

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_single_page_emits_entities_without_stats(
        self, mock_write_record, mock_write_schema
    ):
        mock_api = Mock()
        mock_api.list_broadcasts.return_value = {
            "broadcasts": [
                {"id": 1, "subject": "A"},
                {"id": 2, "subject": "B"},
            ],
            "pagination": {"has_next_page": False},
        }

        with patch("tap_kit.KitAPI", return_value=mock_api):
            sync_broadcasts(self.config, self.state, self.catalog)

        assert mock_write_record.call_count == 2
        first_record = mock_write_record.call_args_list[0].args[1]
        assert first_record["id"] == 1
        assert "stats" not in first_record
        mock_api.list_broadcast_stats.assert_not_called()

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_multi_page_follows_cursor(self, mock_write_record, mock_write_schema):
        mock_api = Mock()
        mock_api.list_broadcasts.side_effect = [
            {
                "broadcasts": [{"id": 1, "subject": "A"}],
                "pagination": {"has_next_page": True, "end_cursor": "NEXT"},
            },
            {
                "broadcasts": [{"id": 2, "subject": "B"}],
                "pagination": {"has_next_page": False},
            },
        ]

        with patch("tap_kit.KitAPI", return_value=mock_api):
            sync_broadcasts(self.config, self.state, self.catalog)

        assert mock_write_record.call_count == 2
        assert mock_api.list_broadcasts.call_args_list == [
            call(after=None),
            call(after="NEXT"),
        ]

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_empty_response(self, mock_write_record, mock_write_schema):
        mock_api = Mock()
        mock_api.list_broadcasts.return_value = {
            "broadcasts": [],
            "pagination": {"has_next_page": False},
        }

        with patch("tap_kit.KitAPI", return_value=mock_api):
            sync_broadcasts(self.config, self.state, self.catalog)

        mock_write_record.assert_not_called()


class TestSyncBroadcastStats:
    """Test broadcast_stats snapshot sync"""

    def setup_method(self):
        self.config = {"api_key": "test_key"}
        self.catalog = discover()

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_flattens_and_stamps_snapshot(self, mock_write_record, mock_write_schema):
        mock_api = Mock()
        mock_api.list_broadcast_stats.side_effect = [
            {
                "broadcasts": [
                    {"id": 1, "stats": {"recipients": 100, "open_rate": 0.5}},
                ],
                "pagination": {"has_next_page": True, "end_cursor": "C2"},
            },
            {
                "broadcasts": [
                    {"id": 2, "stats": {"recipients": 200, "open_rate": 0.4}},
                ],
                "pagination": {"has_next_page": False},
            },
        ]

        with patch("tap_kit.KitAPI", return_value=mock_api):
            sync_broadcast_stats(self.config, self.catalog, "2026-10-07T00:00:00Z")

        assert mock_write_record.call_count == 2
        first = mock_write_record.call_args_list[0].args[1]
        assert first == {
            "id": 1,
            "synced_at": "2026-10-07T00:00:00Z",
            "recipients": 100,
            "open_rate": 0.5,
        }
        assert mock_api.list_broadcast_stats.call_args_list == [
            call(after=None),
            call(after="C2"),
        ]


class TestSyncSubscribers:
    """Test subscribers sync"""

    def setup_method(self):
        self.config = {"api_key": "test_key"}
        self.state = {}
        self.catalog = discover()

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_returns_ids_and_follows_cursor(self, mock_write_record, mock_write_schema):
        mock_api = Mock()
        mock_api.list_subscribers.side_effect = [
            {
                "subscribers": [{"id": 10, "email_address": "a@x.com"}],
                "pagination": {"has_next_page": True, "end_cursor": "C2"},
            },
            {
                "subscribers": [{"id": 20, "email_address": "b@x.com"}],
                "pagination": {"has_next_page": False},
            },
        ]

        with patch("tap_kit.KitAPI", return_value=mock_api):
            ids = sync_subscribers(self.config, self.state, self.catalog)

        assert ids == [10, 20]
        assert mock_write_record.call_count == 2
        assert mock_api.list_subscribers.call_args_list == [
            call(after=None),
            call(after="C2"),
        ]


class TestSyncSubscriberStats:
    """Test subscriber_stats sync"""

    def setup_method(self):
        self.config = {"api_key": "test_key"}
        self.catalog = discover()

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_flattens_nested_stats(self, mock_write_record, mock_write_schema):
        mock_api = Mock()
        mock_api.get_subscriber_stats.side_effect = [
            {"subscriber": {"id": 10, "stats": {"sent": 5, "opened": 3}}},
            {"subscriber": {"id": 20, "stats": {"sent": 8, "opened": 1}}},
        ]

        with patch("tap_kit.KitAPI", return_value=mock_api):
            sync_subscriber_stats(
                self.config, self.catalog, [10, 20], "2026-10-07T00:00:00Z"
            )

        assert mock_write_record.call_count == 2
        first = mock_write_record.call_args_list[0].args[1]
        assert first == {
            "id": 10,
            "synced_at": "2026-10-07T00:00:00Z",
            "sent": 5,
            "opened": 3,
        }

    @patch("tap_kit.write_schema")
    @patch("tap_kit.write_record")
    def test_continues_on_error(self, mock_write_record, mock_write_schema):
        mock_api = Mock()
        mock_api.get_subscriber_stats.side_effect = [
            requests.exceptions.RequestException("boom"),
            {"subscriber": {"id": 20, "stats": {"sent": 8}}},
        ]

        with patch("tap_kit.KitAPI", return_value=mock_api):
            sync_subscriber_stats(
                self.config, self.catalog, [10, 20], "2026-10-07T00:00:00Z"
            )

        assert mock_write_record.call_count == 1
        assert mock_write_record.call_args_list[0].args[1]["id"] == 20


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
