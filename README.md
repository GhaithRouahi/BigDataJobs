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

Run On Docker Container (all main classes)
```
# Assumes your project JARs are available in the container at /root/app.jar and /root/app-all.jar
# Example copy step (from host):
# docker cp target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar hadoop-master:/root/app.jar
# docker cp target/chicago-air-quality-bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar hadoop-master:/root/app-all.jar

# TP1 - SensorAveragesJob
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.SensorAveragesJob /root/xfya-dxtq.csv /root/out/sensor_averages"

# TP1 - DailyCityAveragesJob
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.DailyCityAveragesJob /root/xfya-dxtq.csv /root/out/daily_averages"

# TP1 - HourlyAveragesJob
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.HourlyAveragesJob /root/xfya-dxtq.csv /root/out/hourly_averages"

# TP1 - SensorExceedanceJob
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar -Dpm25.threshold=35.4 -Dno2.threshold=100 tn.insat.tp1.csv.SensorExceedanceJob /root/xfya-dxtq.csv /root/out/exceedances"

# TP2 - ChicagoAirQualitySpark
docker exec -it hadoop-master bash -lc "spark-submit --class tn.insat.tp2.spark.ChicagoAirQualitySpark /root/app-all.jar /root/xfya-dxtq.csv /root/out/spark"

# TP2 - RollingAveragesSpark
docker exec -it hadoop-master bash -lc "spark-submit --class tn.insat.tp2.spark.RollingAveragesSpark /root/app-all.jar /root/xfya-dxtq.csv /root/out/rolling"

# TP4 - CsvToHBase
docker exec -it hadoop-master bash -lc "hbase org.apache.hadoop.util.RunJar /root/app.jar tn.insat.tp4.hbase.CsvToHBase /root/xfya-dxtq.csv 10000"

# TP4 - HBaseQueries (examples)
docker exec -it hadoop-master bash -lc "CP=\"$(hbase classpath)\"; java -cp /root/app.jar:$CP tn.insat.tp4.hbase.HBaseQueries count"
docker exec -it hadoop-master bash -lc "CP=\"$(hbase classpath)\"; java -cp /root/app.jar:$CP tn.insat.tp4.hbase.HBaseQueries latest DIPDE7442"
docker exec -it hadoop-master bash -lc "CP=\"$(hbase classpath)\"; java -cp /root/app.jar:$CP tn.insat.tp4.hbase.HBaseQueries exportDay 2026-02-01 /root/export-2026-02-01.csv"

# DashboardServer
docker exec -it hadoop-master bash -lc "java -cp /root/app-all.jar tn.insat.dashboard.DashboardServer 8080 /root/out/spark"
```

Notes
- CSV columns referenced: datasourceid, time, pm2_5concmassindividual_value (PM2.5), no2concindividual_value (NO2), optional humidity/temperature/lat/lon.
- Date extracted from time as yyyy-MM-dd.
- Outputs include record counts to help with data sparsity.
