Chicago Air Quality (Chicago Data Portal) — Big Data Project

Overview
- Dataset: Outdoor air quality sensors (xfya-dxtq.csv) from Chicago Data Portal.
- Components:
  - Hadoop MapReduce (TP1): per-sensor averages; daily city averages.
  - Spark (TP2): per-sensor and daily summaries; anomaly detection.
  - HBase (TP4): CSV ingestion into `chicago_outdoor_air_quality` table.

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
```

Spark Job
```
spark-submit --class tn.insat.tp2.spark.ChicagoAirQualitySpark target/chicago-air-quality-bigdata-1.0-SNAPSHOT-jar-with-dependencies.jar xfya-dxtq.csv out/spark
```

HBase Ingestion
```
# Requires HBase client config on classpath (hbase-site.xml, etc.)
hadoop jar target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar tn.insat.tp4.hbase.CsvToHBase xfya-dxtq.csv 10000
```

Notes
- CSV columns referenced: datasourceid, time, pm2_5concmassindividual_value (PM2.5), no2concindividual_value (NO2), optional humidity/temperature/lat/lon.
- Date extracted from time as yyyy-MM-dd.
- Outputs include record counts to help with data sparsity.
