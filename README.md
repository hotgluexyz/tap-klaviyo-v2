# tap-klaviyo

This is a [Singer](https://singer.io) tap that produces JSON-formatted
data from the Klaviyo API following the [Singer
spec](https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md).

This tap:

- Pulls raw data from the [Klaviyo metrics API](https://www.klaviyo.com/docs/api/metrics)
- Outputs the schema for each resource
- Incrementally pulls data based on the input state for incremental endpoints
- Updates full tables for global exclusions and lists endpoints
- **NEW**: Provides report-style streams with aggregated metrics using Klaviyo's [Query Metric Aggregates API](https://developers.klaviyo.com/en/reference/query_metric_aggregates)

## Quick start

1. Install

   ```bash
   > virtualenv -p python3 venv
   > source venv/bin/activate
   > pip install tap-klaviyo
   ```

2. Create the config file

   Create a JSON file containing your API key or client ID and client Secret and start date.

   ```json
   {
     "api_key": "pk_XYZ",
     "start_date": "2017-01-01T00:00:00Z",
     "client_id": "client_id",
     "client_secret": "client_secret",
   }
   ```

3. [Optional] Create the initial state file

   You can provide JSON file that contains a date for the metrics endpoints to force the application to only fetch events since those dates. If you omit the file it will fetch all
   commits and issues.

   ```json
   {
     "bookmarks": {
       "receive": { "since": "2017-04-01T00:00:00Z" },
       "open": { "since": "2017-04-01T00:00:00Z" }
     }
   }
   ```

4. [Optional] Run discover command and save catalog into catalog file

   ```bash
   tap-klaviyo --config config.json --discover
   ```

5. Run the application

   `tap-klaviyo` can be run with:

   ```bash
   tap-klaviyo --config config.json [--state state.json] [--catalog catalog.json]
   ```

## API revision 2026-07-15: breaking changes

The tap requests revision `2026-07-15` (set in `KlaviyoStream.http_headers`). Klaviyo
changed the shape of several objects between `2024-10-15` and this revision. The tap
passes those changes through as-is rather than rewriting them, so the fields below have
moved or changed type and anything reading them downstream needs updating.

Four streams are affected. `events`, `metrics`, `lists`, `list_members`, `templates` and
all report streams are unchanged.

### campaign_messages

Message content moved into a `definition` object.

Before:

    {"channel": "email", "label": "Email 1", "content": {"subject": "Testing"}}

After:

    {"definition": {"channel": "email", "label": "Email 1", "content": {"subject": "Testing"}}}

`channel`, `label`, `content` and `render_options` are no longer top-level fields. They are
now under `definition`, along with `notification_type`, `options` and `kv_pairs` for push
messages. Which keys are present depends on the channel: `label` is email only, and
`render_options` is SMS only.

`campaign_id` and `template_id` are added by the tap and stay at the top level.

### campaigns

`send_strategy` was flattened, and the fields it contains now depend on `method`.

Before:

    {"method": "static", "options_static": {"datetime": "...", "is_local": true},
     "options_throttled": null, "options_sto": null}

After:

    {"method": "static", "datetime": "...", "options": {"is_local": true}}

`options_static`, `options_throttled` and `options_sto` are gone. Depending on the method
you now get `datetime` and `options` (static), `datetime` and `throttle_percentage`
(throttled), `date` (smart send time), or `method` on its own (immediate, A/B test,
unsupported).

Push campaigns are also synced now. `CampaignsStream.channels` covers email, SMS and
mobile_push; the endpoint requires a channel filter, so only listed channels are pulled.
Existing email and SMS sync positions are kept, and push backfills once on the first run.

### contacts

The profile conversation relationship became plural and multi-channel.

Before:

    "relationships": {"conversation": {"links": {...}}}

After:

    "relationships": {"conversations": {"links": {...}}}

The related URLs change too. `/profiles/{id}/conversations` returns a list with one entry
per channel (SMS, WhatsApp, Instagram), which is why it cannot be represented as the single
object the old field held.

Note this only changed on `/profiles`. `/lists/{id}/profiles` still returns the singular
`conversation`, so `list_members` is unchanged and its schema differs from `contacts` on
purpose.

### reviews

`status` changed from a string to an object.

Before:

    "status": "published"

After:

    "status": {"value": "published"}

The object also carries `rejection_reason` when a review has been rejected. `email` is now
nullable. Verified against a live account: all 110 reviews changed shape.

### Notes for future revision bumps

Records are flattened before they are emitted (`post_process` lifts `attributes` to the top
level), so static schemas in `tap_klaviyo/schemas/` must declare fields at the top level,
not nested under `attributes`.

Schemas are also merged with what discovery infers from a live response, and discovery keeps
any field the static file does not mention. If a field should not appear in the catalog,
removing it from the static schema is not enough.

## Report Streams

The tap now includes report-style streams that provide aggregated metrics using Klaviyo's Query Metric Aggregates API. These streams offer pre-built reports for common analytics needs while allowing customization through configuration.

### Default Report Streams

The tap automatically includes the following default report streams:

- **emails_sent_per_day**: Daily count of emails sent, grouped by campaign name and message
- **emails_opened_per_day**: Daily unique count of email opens, grouped by campaign name and message  
- **emails_clicked_per_day**: Daily unique count of email clicks, grouped by campaign name and message
- **flows_triggered_per_day**: Daily count of flow triggers, grouped by flow name and channel
- **campaign_performance_daily**: Daily email performance metrics (opens and counts), grouped by campaign name and channel

### Custom Report Configuration

You can define custom report streams by adding a `custom_reports` array to your configuration file:

```json
{
  "api_key": "pk_XYZ",
  "start_date": "2017-01-01T00:00:00Z",
  "custom_reports": [
    {
      "name": "campaign_opens_per_day",
      "metric_id": "SZ95bT",
      "dimensions": "Campaign Name,$message",
      "aggregation_types": "unique",
      "interval": "day"
    },
    {
      "name": "flow_performance_weekly",
      "metric_id": "WEC6yf", 
      "dimensions": "Campaign Name,$message",
      "measurements": "count,unique",
      "interval": "week"
    }
  ]
}
```

### Report Configuration Options

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `name` | string | Yes | Unique name for the report stream |
| `metric_id` | string | Yes | Klaviyo metric ID (e.g., "SZ95bT", "WEC6yf") |
| `metric_name` | string | No | Klaviyo Metric name|
| `dimensions` | string | No | Comma-separated attributes to group data by (e.g., "Campaign Name,$message") |
| `aggregation_types` | string | No | Comma-separated aggregation types: "count", "unique", "sum_value" (default: "count") |
| `interval` | string | No | Time interval: "day", "week", "month" (default: "day") |

### Supported Dimensions

Common dimensions you can use for grouping include:

- **Campaign dimensions**: "Campaign Name", "Campaign Channel"
- **Flow dimensions**: "Flow Name", "Flow Channel" 
- **Message dimensions**: "$message", "$subject"
- **Profile dimensions**: "$email", "$phone_number"
- **Date dimensions**: "datetime" (automatically handled by interval)

### Supported aggregation types

The following aggregation types are supported:

- **count**: Total number of events
- **unique**: Number of unique profiles
- **sum_value**: Sum of numeric values

### Example Use Cases

1. **Email Campaign Performance**: Track daily opens, clicks, and sends by campaign
2. **Flow Analytics**: Monitor flow performance across different channels
3. **Customer Engagement**: Analyze profile engagement patterns over time

---
