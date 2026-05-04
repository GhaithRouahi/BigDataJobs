package tn.insat.tp2.spark;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

/**
 * Computes correlation between PM2.5 and NO2 overall and per sensor.
 */
public class CorrelationSpark {
    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.println("Usage: CorrelationSpark <input.csv>");
            System.exit(1);
        }

        SparkSession spark = SparkSession.builder()
                .appName("PM2.5-NO2 Correlation")
                .getOrCreate();

        Dataset<Row> df = spark.read()
                .option("header", true).option("inferSchema", true)
                .csv(args[0])
                .withColumn("datasourceid", col("datasourceid"))
                .withColumn("pm25", col("pm2_5concmassindividual_value").cast("double"))
                .withColumn("no2", col("no2concindividual_value").cast("double"))
                .na().drop(new String[]{"pm25","no2"});

        System.out.println("=== Overall Correlation (PM2.5, NO2) ===");
        double overall = df.stat().corr("pm25", "no2");
        System.out.println("corr(pm25,no2) = " + overall);

        System.out.println("\n=== Top Sensors by |Correlation| ===");
        // Approximate per-sensor correlation via aggregation of covariance components
        Dataset<Row> perSensor = df.groupBy("datasourceid")
                .agg(
                        count(lit(1)).alias("n"),
                        avg("pm25").alias("mx"),
                        avg("no2").alias("my"),
                        avg(col("pm25").multiply(col("no2"))).alias("mxy"),
                        avg(expr("pm25 * pm25")).alias("mx2"),
                        avg(expr("no2 * no2")).alias("my2")
                )
                .withColumn("cov", col("mxy").minus(col("mx").multiply(col("my"))))
                .withColumn("varx", col("mx2").minus(col("mx").multiply(col("mx"))))
                .withColumn("vary", col("my2").minus(col("my").multiply(col("my"))))
                .withColumn("corr", col("cov").divide(sqrt(col("varx").multiply(col("vary")))))
                .select("datasourceid", "n", "corr")
                .filter(col("n").geq(10))
                .orderBy(desc(abs(col("corr"))));

        perSensor.show(20, false);
        spark.stop();
    }
}
