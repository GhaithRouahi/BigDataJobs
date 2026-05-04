package tn.insat.tp2.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Spark analysis for Chicago outdoor air quality CSV (xfya-dxtq schema).
 * - Computes per-sensor summaries (PM2.5, NO2)
 * - Computes daily citywide averages
 * - Detects anomalies using 2*std deviation over global means
 * Writes outputs if an output base path is provided (arg1).
 */
public class ChicagoAirQualitySpark {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: ChicagoAirQualitySpark <input.csv> [output_base_dir]");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Chicago Outdoor Air Quality (Spark)")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true)
                .option("inferSchema", true)
                .csv(args[0])
                .withColumn("datasourceid", col("datasourceid"))
                .withColumn("time", col("time"))
                .withColumn("pm25", col("pm2_5concmassindividual_value").cast("double"))
                .withColumn("no2", col("no2concindividual_value").cast("double"))
                .withColumn("date", to_date(col("time")));

        // Per-sensor summaries
        Dataset<Row> perSensor = df.groupBy("datasourceid")
                .agg(
                        avg("pm25").alias("pm25_avg"),
                        min("pm25").alias("pm25_min"),
                        max("pm25").alias("pm25_max"),
                        count("pm25").alias("pm25_count"),
                        avg("no2").alias("no2_avg"),
                        min("no2").alias("no2_min"),
                        max("no2").alias("no2_max"),
                        count("no2").alias("no2_count")
                )
                .orderBy(desc("pm25_avg"));

        System.out.println("=== Per-Sensor Summary ===");
        perSensor.show(20, false);

        // Daily citywide averages
        Dataset<Row> daily = df.groupBy("date")
                .agg(
                        avg("pm25").alias("pm25_avg"),
                        avg("no2").alias("no2_avg"),
                        count(lit(1)).alias("records")
                )
                .orderBy("date");

        System.out.println("=== Daily Citywide Averages ===");
        daily.show(20, false);

        // Anomalies: z-score against global mean/std for pm25/no2
        Row stats = df.agg(
                avg("pm25").alias("pm25_mean"), stddev("pm25").alias("pm25_std"),
                avg("no2").alias("no2_mean"), stddev("no2").alias("no2_std")
        ).first();

        double pm25Mean = stats.getAs("pm25_mean") == null ? 0.0 : stats.getDouble(stats.fieldIndex("pm25_mean"));
        double pm25Std  = stats.getAs("pm25_std")  == null ? 0.0 : stats.getDouble(stats.fieldIndex("pm25_std"));
        double no2Mean  = stats.getAs("no2_mean")  == null ? 0.0 : stats.getDouble(stats.fieldIndex("no2_mean"));
        double no2Std   = stats.getAs("no2_std")   == null ? 0.0 : stats.getDouble(stats.fieldIndex("no2_std"));

        Dataset<Row> anomalies = df.filter(
                (pm25Std > 0 ? col("pm25").gt(pm25Mean + 2 * pm25Std) : lit(false))
                        .or(no2Std > 0 ? col("no2").gt(no2Mean + 2 * no2Std) : lit(false))
        ).select("datasourceid", "time", "pm25", "no2");

        System.out.println("=== Detected Anomalies ===");
        anomalies.show(20, false);

        if (args.length >= 2) {
            String out = args[1];
            perSensor.coalesce(1).write().mode("overwrite").option("header", true).csv(out + "/per_sensor");
            daily.coalesce(1).write().mode("overwrite").option("header", true).csv(out + "/daily");
            anomalies.coalesce(1).write().mode("overwrite").option("header", true).csv(out + "/anomalies");
        }

        spark.stop();
    }
}
