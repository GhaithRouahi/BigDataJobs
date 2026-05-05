Chicago Air Quality (Chicago Data Portal) — Big Data Project

Overview
- Dataset: Outdoor air quality sensors (xfya-dxtq.csv) from Chicago Data Portal.
- Components:
  - Hadoop MapReduce (TP1): per-sensor averages; daily city averages.
  - Spark (TP2): per-sensor and daily summaries; anomaly detection.
  - HBase (TP4): CSV ingestion into `chicago_outdoor_air_quality` table.
  - Extras: exceedances + hourly MR jobs, rolling averages in Spark, HBase query tools, and a lightweight dashboard.

Build
```
mvn -DskipTests package
```

MapReduce Jobs (HDFS cluster assumed)
```
# Per-sensor averages (PM2.5, NO2)
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp1.csv.SensorAveragesJob /path/xfya-dxtq.csv /out/sensor_averages

# Daily city averages
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp1.csv.DailyCityAveragesJob /path/xfya-dxtq.csv /out/daily_averages

# Sensor exceedances (configurable thresholds)
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar \
  -Dpm25.threshold=35.4 -Dno2.threshold=100 tn.insat.tp1.csv.SensorExceedanceJob \
  /path/xfya-dxtq.csv /out/exceedances

# Hourly city averages (yyyy-MM-ddTHH)
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp1.csv.HourlyAveragesJob /path/xfya-dxtq.csv /out/hourly_averages
```

Spark Job
```
spark-submit --class tn.insat.tp2.spark.ChicagoAirQualitySpark target/chicago-air-quality-bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar xfya-dxtq.csv out/spark

# 24h rolling averages per sensor
spark-submit --class tn.insat.tp2.spark.RollingAveragesSpark target/chicago-air-quality-bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar xfya-dxtq.csv out/rolling

```
```

HBase Ingestion
```
# Requires HBase client config on classpath (hbase-site.xml, etc.)
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp4.hbase.CsvToHBase xfya-dxtq.csv 10000

# Queries
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp4.hbase.HBaseQueries count
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp4.hbase.HBaseQueries latest DIPDE7442
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp4.hbase.HBaseQueries exportDay 2026-02-01 out/export-2026-02-01.csv
```

Dashboard (lightweight)
```
# Serve charts from Spark outputs (default base: out/spark)
java -cp target/chicago-air-quality-bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar tn.insat.dashboard.DashboardServer 8080 out/spark

# Then open http://localhost:8080
```

Notes
- CSV columns referenced: datasourceid, time, pm2_5concmassindividual_value (PM2.5), no2concindividual_value (NO2), optional humidity/temperature/lat/lon.
- Date extracted from time as yyyy-MM-dd.
- Outputs include record counts to help with data sparsity.
