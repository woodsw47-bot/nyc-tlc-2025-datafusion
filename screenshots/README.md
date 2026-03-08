# NYC TLC 2025 DataFusion Analysis

## What the project does
- Loads NYC Yellow Taxi 2025 trip data from Parquet files.
- Uses Rust and Apache DataFusion to perform analytics.
- Computes aggregations using both the DataFrame API and SQL.
- Prints results to the terminal.

## Dataset source
NYC TLC Trip Record Data  
https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## How to download the data
Example PowerShell script used:
for ($i=1; $i -le 12; $i++) {
$m = "{0:D2}" -f $i
$url = "https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-$m.parquet
"
curl.exe -A "Mozilla/5.0" -L $url -o "data/yellow_tripdata_2025-$m.parquet"
}

## How to run the project
cargo run --release

The program loads the Parquet files and prints aggregation results.

## Aggregation explanations

### Aggregation 1
Trips and revenue by month.  
Groups taxi trips by pickup month and calculates trip count, total revenue, and average fare.

### Aggregation 2
Tip behavior by payment type.  
Groups trips by payment type and calculates average tip and tip rate.

## Output

![Program Output](screenshots/output.png)