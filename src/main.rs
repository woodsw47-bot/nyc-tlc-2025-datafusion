use datafusion::error::Result;
use datafusion::functions_aggregate::expr_fn::{avg, count, sum};
use datafusion::prelude::*;

fn parquet_glob_2025() -> String {
    "data/yellow_tripdata_2025-*.parquet".to_string()
}

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(80));
    println!("{title}");
    println!("{}", "=".repeat(80));
}

#[tokio::main]
async fn main() -> Result<()> {
    let ctx = SessionContext::new();
    let path = parquet_glob_2025();

    // Register dataset for SQL queries
    ctx.register_parquet(
        "yellow_taxi_2025",
        &path,
        ParquetReadOptions::default(),
    )
    .await?;

    // Load dataframe for DataFrame API queries
    let df = ctx
        .read_parquet(&path, ParquetReadOptions::default())
        .await?;

    println!("Loaded all 2025 Yellow Taxi Parquet files.");

    // ==================================================
    // Aggregation 1 - DataFrame API
    // ==================================================
    print_section("Aggregation 1 - DataFrame API: Trips and revenue by month");

    let agg1_df = df
        .clone()
        .aggregate(
            vec![date_part(lit("month"), col("tpep_pickup_datetime")).alias("pickup_month")],
            vec![
                count(lit(1)).alias("trip_count"),
                sum(col("total_amount")).alias("total_revenue"),
                avg(col("fare_amount")).alias("avg_fare"),
            ],
        )?
        .sort(vec![col("pickup_month").sort(true, true)])?;

    agg1_df.show().await?;

    // ==================================================
    // Aggregation 1 - SQL
    // ==================================================
    print_section("Aggregation 1 - SQL: Trips and revenue by month");

    let agg1_sql = ctx
        .sql(
            r#"
            SELECT
                date_part('month', tpep_pickup_datetime) AS pickup_month,
                COUNT(*) AS trip_count,
                SUM(total_amount) AS total_revenue,
                AVG(fare_amount) AS avg_fare
            FROM yellow_taxi_2025
            GROUP BY date_part('month', tpep_pickup_datetime)
            ORDER BY pickup_month ASC
            "#,
        )
        .await?;

    agg1_sql.show().await?;

    // ==================================================
    // Aggregation 2 - DataFrame API
    // aggregate first, then compute tip_rate
    // ==================================================
    print_section("Aggregation 2 - DataFrame API: Tip behavior by payment type");

    let agg2_base = df
        .clone()
        .aggregate(
            vec![col("payment_type")],
            vec![
                count(lit(1)).alias("trip_count"),
                avg(col("tip_amount")).alias("avg_tip_amount"),
                sum(col("tip_amount")).alias("sum_tip_amount"),
                sum(col("total_amount")).alias("sum_total_amount"),
            ],
        )?;

    let agg2_df = agg2_base
        .select(vec![
            col("payment_type"),
            col("trip_count"),
            col("avg_tip_amount"),
            (col("sum_tip_amount") / col("sum_total_amount")).alias("tip_rate"),
        ])?
        .sort(vec![col("trip_count").sort(false, true)])?;

    agg2_df.show().await?;

    // ==================================================
    // Aggregation 2 - SQL
    // ==================================================
    print_section("Aggregation 2 - SQL: Tip behavior by payment type");

    let agg2_sql = ctx
        .sql(
            r#"
            SELECT
                payment_type,
                COUNT(*) AS trip_count,
                AVG(tip_amount) AS avg_tip_amount,
                SUM(tip_amount) / SUM(total_amount) AS tip_rate
            FROM yellow_taxi_2025
            GROUP BY payment_type
            ORDER BY trip_count DESC
            "#,
        )
        .await?;

    agg2_sql.show().await?;

    println!("\nAll aggregations completed successfully.");

    Ok(())
}