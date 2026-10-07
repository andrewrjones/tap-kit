# tap-kit

A [Singer](https://singer.io) tap for extracting data from the [Kit API](https://developers.kit.com/).

## Installation

```bash
pip install tap-kit
```

Or run directly with `uvx`.

```bash
uvx tap-kit --help

# Example with CSV target
uvx tap-kit --config config.json | uvx --with setuptools target-csv
```

## Configuration

Create a `config.json` file with your Kit v4 API key:

```json
{
  "api_key": "your_kit_v4_api_key_here",
  "start_date": "2024-01-01T00:00:00Z"
}
```

- `api_key` (required): Your Kit v4 API key, sent as the `X-Kit-Api-Key` header.
- `start_date` (optional): ISO 8601 timestamp used as the initial incremental bookmark.

You can find your API key in your [Kit API settings](https://app.kit.com/account_settings/developer_settings).

## Usage

### Discover available streams

```bash
tap-kit --config config.json --discover
```

This will output a catalog of available streams in JSON format.

### Run the tap

```bash
tap-kit --config config.json --catalog catalog.json
```

Or to use the default catalog:

```bash
tap-kit --config config.json
```

### State management

To maintain state between runs (for incremental syncs):

```bash
tap-kit --config config.json --state state.json
```

## Streams

Stats are modelled as their own snapshot streams (`broadcast_stats`,
`subscriber_stats`) rather than merged into the entity records. Engagement metrics
change over time, so each sync stamps every stat row with a `synced_at` timestamp
and uses a composite primary key of `[id, synced_at]`. This captures a point-in-time
snapshot per run: on append-style targets every run adds rows; on merge/upsert
targets (e.g. BigQuery MERGE on key properties) the new `synced_at` inserts rather
than overwriting. Partition the snapshot tables on `synced_at` in your warehouse.

### Broadcasts

The `broadcasts` stream extracts broadcast entity data from your Kit account.

**Schema:**
- `id` (integer): Unique broadcast ID
- `publication_id` (integer, nullable): Publication ID
- `created_at` (datetime): When the broadcast was created
- `subject` (string, nullable): Broadcast subject line
- `preview_text` (string, nullable): Inbox preview text
- `description` (string, nullable): Broadcast description
- `content` (string, nullable): Broadcast content
- `public` (boolean, nullable): Whether the broadcast is public
- `published_at` (datetime, nullable): When the broadcast was published
- `send_at` (datetime, nullable): When the broadcast was/will be sent
- `thumbnail_alt` (string, nullable): Thumbnail alt text
- `thumbnail_url` (string, nullable): Thumbnail URL
- `public_url` (string, nullable): Public URL for the broadcast
- `email_address` (string, nullable): Sender email address
- `email_template` (object, nullable): Email template (id, name)
- `subscriber_filter` (array/object, nullable): Subscriber targeting filter
- `status` (string, nullable): Broadcast status

**Replication Method:** INCREMENTAL
**Replication Key:** `created_at`
**Primary Key:** `id`

### Broadcast Stats

The `broadcast_stats` stream snapshots delivery/engagement stats for every
broadcast, fetched in bulk from the `/broadcasts/stats` batch endpoint.

**Schema:**
- `id` (integer): Broadcast ID
- `synced_at` (datetime): Snapshot timestamp for this sync run
- `recipients` (integer, nullable): Number of recipients
- `open_rate` (number, nullable): Email open rate
- `emails_opened` (integer, nullable): Number of emails opened
- `click_rate` (number, nullable): Email click rate
- `unsubscribe_rate` (number, nullable): Unsubscribe rate
- `unsubscribes` (integer, nullable): Number of unsubscribes
- `total_clicks` (integer, nullable): Total number of clicks
- `show_total_clicks` (boolean, nullable): Whether to show total clicks
- `status` (string, nullable): Broadcast status
- `progress` (number, nullable): Send progress percentage
- `open_tracking_disabled` (boolean, nullable): Whether open tracking is off
- `click_tracking_disabled` (boolean, nullable): Whether click tracking is off

**Replication Method:** FULL_TABLE
**Primary Key:** `id`, `synced_at`

### Subscribers

The `subscribers` stream extracts subscriber data from your Kit account.

**Schema:**
- `id` (integer): Unique subscriber ID
- `first_name` (string, nullable): Subscriber first name
- `email_address` (string): Subscriber email address
- `state` (string, nullable): Subscriber state (e.g., "active", "inactive")
- `created_at` (datetime): When the subscriber was created
- `fields` (object, nullable): Custom fields

**Replication Method:** INCREMENTAL
**Replication Key:** `created_at`

### Subscriber Stats

The `subscriber_stats` stream snapshots per-subscriber engagement stats.

**Schema:**
- `id` (integer): Subscriber ID
- `synced_at` (datetime): Snapshot timestamp for this sync run
- `sent` (integer, nullable): Emails sent
- `opened` (integer, nullable): Emails opened
- `clicked` (integer, nullable): Emails clicked
- `bounced` (integer, nullable): Emails bounced
- `open_rate` (number, nullable): Open rate
- `click_rate` (number, nullable): Click rate
- `last_sent` (datetime, nullable): Last send timestamp
- `last_opened` (datetime, nullable): Last open timestamp
- `last_clicked` (datetime, nullable): Last click timestamp
- `sends_since_last_open` (integer, nullable): Sends since last open
- `sends_since_last_click` (integer, nullable): Sends since last click

**Replication Method:** FULL_TABLE
**Primary Key:** `id`, `synced_at`

**Note:** This stream makes one API call per subscriber. Kit rate-limits to 120
requests per rolling 60 seconds, so large subscriber lists will sync slowly; the
tap automatically retries on rate-limit (HTTP 429) responses.

## Development

This project is managed with `uv`.

### Setup

1. Clone the repository
1. Create a `config.json` file with your API key

### Testing

```bash
# Discover streams
uv run tap-kit --config config.json --discover

# Run sync
uv run tap-kit --config config.json
```

## License

This project is licensed under the MIT License.
