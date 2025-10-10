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
