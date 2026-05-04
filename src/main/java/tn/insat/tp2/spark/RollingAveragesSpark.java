package tn.insat.tp2.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import static org.apache.spark.sql.functions.*;

/**
 * Computes 24-hour rolling averages per sensor for PM2.5 and NO2.
 * Output includes datasourceid, time, rolling_pm25_24h, rolling_no2_24h.
 */
public class RollingAveragesSpark {
    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: RollingAveragesSpark <input.csv> <output_dir>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("Rolling Averages 24h (PM2.5/NO2)")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true).option("inferSchema", true)
                .csv(args[0])
                .withColumn("datasourceid", col("datasourceid"))
                .withColumn("time", to_timestamp(col("time")))
                .withColumn("pm25", col("pm2_5concmassindividual_value").cast("double"))
                .withColumn("no2", col("no2concindividual_value").cast("double"));

        WindowSpec w = Window.partitionBy("datasourceid")
                .orderBy(col("time").cast("long"))
                .rangeBetween(-24L * 3600L, 0L); // last 24h by seconds

        Dataset<Row> out = df
                .withColumn("rolling_pm25_24h", avg("pm25").over(w))
                .withColumn("rolling_no2_24h", avg("no2").over(w))
                .select("datasourceid", "time", "rolling_pm25_24h", "rolling_no2_24h");

        out.coalesce(1).write().mode("overwrite").option("header", true).csv(args[1]);
        spark.stop();
    }
}
