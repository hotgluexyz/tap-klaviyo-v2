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

## API revision bump to 2026-07-15: breaking changes

The tap requests revision `2026-07-15` (set in `KlaviyoStream.http_headers`). Klaviyo
changed the shape of several objects between `2024-10-15` and this revision. The tap doesn't reshape the data — it passes it through as-is. That means the fields below have moved or changed type, so anything consuming them downstream needs to catch up.

Four streams change shape: `campaign_messages`, `campaigns`, `contacts` and `reviews`.
`events` keeps its shape but can return more rows, described at the end. `metrics`, `lists`,
`list_members`, `templates` and all report streams are unaffected.

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

The profile conversation relationship became plural and multi-channel. The field is renamed
from `conversation` to `conversations`, and the URLs inside it change to match.

Before:

    "relationships": {
      "conversation": {
        "links": {
          "self": ".../api/profiles/{id}/relationships/conversation/",
          "related": ".../api/profiles/{id}/conversation/"
        }
      }
    }

After:

    "relationships": {
      "conversations": {
        "links": {
          "self": ".../api/profiles/{id}/relationships/conversations/",
          "related": ".../api/profiles/{id}/conversations/"
        }
      }
    }

Three things change: the field name, and both URLs under `links`.

Following the related URL returns something different in kind. The plural endpoint returns
`data` as a list, with one entry per channel, each carrying its own `channel` attribute:

    {
      "data": [
        {
          "type": "conversation",
          "id": "conv_instagram_1",
          "attributes": { "channel": "instagram" },
          "relationships": { "profile": { ... } },
          "links": { "self": "string" }
        },
        {
          "type": "conversation",
          "id": "conv_whatsapp_2",
          "attributes": { "channel": "whatsapp" },
          "relationships": { "profile": { ... } },
          "links": { "self": "string" }
        }
      ],
      "links": { "self": "string", "prev": "string", "next": "string" }
    }

One profile, two conversations. Note the `data` array and the two different `channel`
values.

The singular endpoint still responds on this revision, but returns `data` as a single
object, so it can only ever represent one channel:

    {
      "data": {
        "type": "conversation",
        "id": "conv_instagram_1",
        "attributes": { "channel": "instagram" },
        "relationships": { "profile": { ... } },
        "links": { "self": "string" }
      },
      "links": { "self": "string" }
    }

Same profile, but `data` is a single object. There is nowhere for the WhatsApp conversation
to go, so it is simply not returned.

A list against a single object is why the new field cannot be mapped back to the old one.
A profile with conversations on two channels has two entries, and collapsing them into the
singular key would drop one. The plural response is also paginated (`prev`/`next` links),
which the singular one is not.

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

### events (no schema change, but more rows)

The record shape is identical, so nothing downstream breaks. What changed is which events
come back. From this revision `GET /events` also returns events whose metric cannot be
resolved, because the metric was deleted or soft-deleted. Earlier revisions dropped those
silently.

Those records arrive with a null metric relationship:

    "relationships": {"metric": {"data": null, "links": {...}}, "profile": {...}}

The schema already allows this (`relationships.metric`, `.data` and `.data.id` are all
nullable) and nothing in the tap dereferences the metric per record, so they sync without
error. The practical effect is that the `events` stream can emit rows it would previously
have skipped, and a full re-sync will not necessarily match a historical load row for row.

Klaviyo added a `has(metric)` filter to exclude them. The tap deliberately does not apply
it: extra rows are preferable to silently dropping data, and this only affects accounts
with deleted metrics. To restore the old behaviour, add `has(metric)` to the filter in
`EventsStream.get_url_params`. Note Klaviyo only accepts a flat `and(...)`, so it has to be
folded into the existing datetime filter rather than nested.

The per-metric `events_<metric>` streams filter on `equals(metric_id,...)` and are
unaffected.

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
