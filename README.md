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

Create a `config.json` file with your Kit API key:

```json
{
  "api_key": "your_kit_api_key_here"
}
```

You can find your API key in your [Kit API settings](https://app.kit.com/account_settings/api).

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

### Broadcasts

The `broadcasts` stream extracts broadcast data from your Kit account.

**Schema:**
- `id` (integer): Unique broadcast ID
- `created_at` (datetime): When the broadcast was created
- `subject` (string): Broadcast subject line
- `description` (string): Broadcast description
- `content` (string): Broadcast content
- `public` (boolean): Whether the broadcast is public
- `published_at` (datetime, nullable): When the broadcast was published
- `send_at` (datetime, nullable): When the broadcast was/will be sent
- `thumbnail_alt` (string, nullable): Thumbnail alt text
- `thumbnail_url` (string, nullable): Thumbnail URL
- `email_address` (string): Sender email address
- `email_layout_template` (string): Email layout template
- `stats` (object, nullable): Broadcast statistics
  - `recipients` (integer, nullable): Number of recipients
  - `open_rate` (number, nullable): Email open rate
  - `click_rate` (number, nullable): Email click rate
  - `unsubscribes` (integer, nullable): Number of unsubscribes
  - `total_clicks` (integer, nullable): Total number of clicks
  - `show_total_clicks` (boolean, nullable): Whether to show total clicks
  - `status` (string, nullable): Broadcast status
  - `progress` (number, nullable): Send progress percentage

**Replication Method:** INCREMENTAL
**Replication Key:** `created_at`

**Note:** The tap fetches statistics for each broadcast using a separate API call to include real-time performance metrics.

### Subscribers

The `subscribers` stream extracts subscriber data from your Kit account.

**Schema:**
- `id` (integer): Unique subscriber ID
- `first_name` (string, nullable): Subscriber first name
- `email_address` (string): Subscriber email address
- `state` (string): Subscriber state (e.g., "active", "inactive")
- `created_at` (datetime): When the subscriber was created
- `fields` (object): Custom fields

**Replication Method:** INCREMENTAL
**Replication Key:** `created_at`

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
