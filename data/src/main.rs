use datafusion::prelude::*;
use datafusion::error::Result;

fn parquet_paths_for_2025() -> Vec<String> {
    (1..=12)
        .map(|m| format!("data/yellow_tripdata_2025-{m:02}.parquet"))
        .collect()
}

fn print_section(title: &str) {
    println!("\n{}", "=".repeat(80));
    println!("{title}");
    println!("{}", "=".repeat(80));
}

#[tokio::main]
async fn main() -> Result<()> {

    let ctx = SessionContext::new();
    let paths = parquet_paths_for_2025();

    // Register dataset for SQL
    ctx.register_parquet(
        "yellow_taxi_2025",
        paths.clone(),
        ParquetReadOptions::default(),
    ).await?;

    // Load dataframe
    let df = ctx
        .read_parquet(paths.clone(), ParquetReadOptions::default())
        .await?;

    println!("Loaded all 12 monthly Yellow Taxi Parquet files for 2025.");

    // ------------------------------
    // Aggregation 1 (DataFrame API)
    // ------------------------------
    print_section("Aggregation 1 - DataFrame API: Trips and revenue by month");

    let agg1_df = df.clone()
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

    // ------------------------------
    // Aggregation 1 (SQL)
    // ------------------------------
    print_section("Aggregation 1 - SQL: Trips and revenue by month");

    let agg1_sql = ctx.sql(
        r#"
        SELECT
            date_part('month', tpep_pickup_datetime) AS pickup_month,
            COUNT(*) AS trip_count,
            SUM(total_amount) AS total_revenue,
            AVG(fare_amount) AS avg_fare
        FROM yellow_taxi_2025
        GROUP BY date_part('month', tpep_pickup_datetime)
        ORDER BY pickup_month ASC
        "#
    ).await?;

    agg1_sql.show().await?;

    // ------------------------------
    // Aggregation 2 (DataFrame API)
    // ------------------------------
    print_section("Aggregation 2 - DataFrame API: Tip behavior by payment type");

    let agg2_df = df.clone()
        .aggregate(
            vec![col("payment_type")],
            vec![
                count(lit(1)).alias("trip_count"),
                avg(col("tip_amount")).alias("avg_tip_amount"),
                (sum(col("tip_amount")) / sum(col("total_amount"))).alias("tip_rate"),
            ],
        )?
        .sort(vec![col("trip_count").sort(false, true)])?;

    agg2_df.show().await?;

    // ------------------------------
    // Aggregation 2 (SQL)
    // ------------------------------
    print_section("Aggregation 2 - SQL: Tip behavior by payment type");

    let agg2_sql = ctx.sql(
        r#"
        SELECT
            payment_type,
            COUNT(*) AS trip_count,
            AVG(tip_amount) AS avg_tip_amount,
            SUM(tip_amount) / SUM(total_amount) AS tip_rate
        FROM yellow_taxi_2025
        GROUP BY payment_type
        ORDER BY trip_count DESC
        "#
    ).await?;

    agg2_sql.show().await?;

    println!("\nAll aggregations completed successfully.");

    Ok(())
}