Hadoop + Spark + HBase runbook (Docker cluster)

## 0) Start all services after container restart

Bash/Linux:
```bash
docker exec -it hadoop-master bash -lc "\
  start-dfs.sh && \
  start-yarn.sh && \
  start-hbase.sh && \
  sleep 3 && \
  jps | grep -E 'NameNode|DataNode|ResourceManager|NodeManager|HMaster|HRegionServer' \
"
```

PowerShell:
```powershell
docker exec -it hadoop-master bash -lc "start-dfs.sh && start-yarn.sh && start-hbase.sh && sleep 3 && jps | grep -E 'NameNode|DataNode|ResourceManager|NodeManager|HMaster|HRegionServer'"
```

## 1) Prepare input data

Copy CSV to container:
```bash
docker cp xfya-dxtq.csv hadoop-master:/root/
```

Upload CSV to HDFS:

Bash/Linux:
```bash
docker exec -it hadoop-master bash -lc "\
  hdfs dfs -mkdir -p /data && \
  hdfs dfs -put -f /root/xfya-dxtq.csv /data/xfya-dxtq.csv && \
  hdfs dfs -ls /data \
"
```

PowerShell:
```powershell
docker exec -it hadoop-master bash -lc "hdfs dfs -mkdir -p /data && hdfs dfs -put -f /root/xfya-dxtq.csv /data/xfya-dxtq.csv && hdfs dfs -ls /data"
```

## 2) Build and copy application JAR

Build locally:
```bash
mvn clean package -DskipTests
```

Copy JAR to container:
```bash
docker cp target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar hadoop-master:/root/app.jar
```

## 3) Run TP1 MapReduce jobs

Daily city averages:
```bash
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.DailyCityAveragesJob hdfs:///data/xfya-dxtq.csv hdfs:///out/tp1/daily"
```

Hourly averages:
```bash
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.HourlyAveragesJob hdfs:///data/xfya-dxtq.csv hdfs:///out/tp1/hourly"
```

Sensor averages:
```bash
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.SensorAveragesJob hdfs:///data/xfya-dxtq.csv hdfs:///out/tp1/sensor"
```

Sensor exceedance:
```bash
docker exec -it hadoop-master bash -lc "hadoop jar /root/app.jar tn.insat.tp1.csv.SensorExceedanceJob hdfs:///data/xfya-dxtq.csv hdfs:///out/tp1/exceedance"
```

## 4) Run TP2 Spark jobs

### 4.1 ChicagoAirQualitySpark (recommended command with GUAVA fix)

Bash/Linux inside docker exec (exact pattern you provided):
```bash
docker exec -it hadoop-master bash -lc ' \
GUAVA=$(ls -1 /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar 2>/dev/null | head -n1); \
echo "Using GUAVA=$GUAVA"; \
hdfs dfs -rm -r -f /out/spark && \
spark-submit \
  --master yarn \
  --conf spark.yarn.user.classpath.first=true \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.driver.extraClassPath=$GUAVA \
  --conf spark.executor.extraClassPath=$GUAVA \
  --class tn.insat.tp2.spark.ChicagoAirQualitySpark \
  /root/app.jar hdfs:///data/xfya-dxtq.csv hdfs:///out/spark && \
hdfs dfs -ls /out/spark'
```

PowerShell-friendly one line:
```powershell
docker exec -it hadoop-master bash -lc "GUAVA=$(ls -1 /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar 2>/dev/null | head -n1); echo \"Using GUAVA=$GUAVA\"; hdfs dfs -rm -r -f /out/spark && spark-submit --master yarn --conf spark.yarn.user.classpath.first=true --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --conf spark.driver.extraClassPath=$GUAVA --conf spark.executor.extraClassPath=$GUAVA --class tn.insat.tp2.spark.ChicagoAirQualitySpark /root/app.jar hdfs:///data/xfya-dxtq.csv hdfs:///out/spark && hdfs dfs -ls /out/spark"
```

### 4.2 RollingAveragesSpark

Bash/Linux:
```bash
docker exec -it hadoop-master bash -lc ' \
GUAVA=$(ls -1 /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar 2>/dev/null | head -n1); \
echo "Using GUAVA=$GUAVA"; \
hdfs dfs -rm -r -f /out/rolling && \
spark-submit \
  --master yarn \
  --conf spark.yarn.user.classpath.first=true \
  --conf spark.driver.userClassPathFirst=true \
  --conf spark.executor.userClassPathFirst=true \
  --conf spark.driver.extraClassPath=$GUAVA \
  --conf spark.executor.extraClassPath=$GUAVA \
  --class tn.insat.tp2.spark.RollingAveragesSpark \
  /root/app.jar hdfs:///data/xfya-dxtq.csv hdfs:///out/rolling && \
hdfs dfs -ls /out/rolling'
```

PowerShell:
```powershell
docker exec -it hadoop-master bash -lc "GUAVA=$(ls -1 /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar 2>/dev/null | head -n1); echo \"Using GUAVA=$GUAVA\"; hdfs dfs -rm -r -f /out/rolling && spark-submit --master yarn --conf spark.yarn.user.classpath.first=true --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --conf spark.driver.extraClassPath=$GUAVA --conf spark.executor.extraClassPath=$GUAVA --class tn.insat.tp2.spark.RollingAveragesSpark /root/app.jar hdfs:///data/xfya-dxtq.csv hdfs:///out/rolling && hdfs dfs -ls /out/rolling"
```

## 5) Run TP4 HBase ingestion

Recommended:
```bash
docker exec -it hadoop-master bash -lc "hbase org.apache.hadoop.util.RunJar /root/app.jar tn.insat.tp4.hbase.CsvToHBase /root/xfya-dxtq.csv 10000"
```

Alternative with HBase classpath:
```bash
docker exec -it hadoop-master bash -lc "CP=\"$(hbase classpath)\"; java -cp /root/app.jar:$CP tn.insat.tp4.hbase.CsvToHBase /root/xfya-dxtq.csv 10000"
```

## 6) Query HBase data with HBaseQueries

Three query modes available: latest, count, exportDay.

### 6.1 Mode: latest (get most recent row for a sensor)

**Bash/Linux:**
```bash
docker exec -it hadoop-master bash -lc "\
  CP=\"\$(hbase classpath)\"; \
  java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries latest DIPDE7442 \
"
```

**PowerShell:**
```powershell
docker exec -it hadoop-master bash -lc "CP=\"\$(hbase classpath)\"; java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries latest DIPDE7442"
```

Output format: rowkey, pm25, no2, humidity, temp, lat, lon, location, datasourceid, time, sensor_name

### 6.2 Mode: count (total row count in table)

**Bash/Linux:**
```bash
docker exec -it hadoop-master bash -lc "\
  CP=\"\$(hbase classpath)\"; \
  java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries count \
"
```

**PowerShell:**
```powershell
docker exec -it hadoop-master bash -lc "CP=\"\$(hbase classpath)\"; java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries count"
```

Output: Row count: <number>

### 6.3 Mode: exportDay (export all rows for a specific date)

**Bash/Linux:**
```bash
docker exec -it hadoop-master bash -lc "\
  CP=\"\$(hbase classpath)\"; \
  java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries exportDay 2026-02-01 /root/export-2026-02-01.csv \
"
```

**PowerShell:**
```powershell
docker exec -it hadoop-master bash -lc "CP=\"\$(hbase classpath)\"; java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries exportDay 2026-02-01 /root/export-2026-02-01.csv"
```

Then retrieve the CSV from container:
```bash
docker cp hadoop-master:/root/export-2026-02-01.csv ./
```

Output: CSV file with columns: rowkey, pm25, no2, humidity, temp, lat, lon, location, sensor_name, datasourceid, time

## 7) Verify outputs

Check HDFS outputs:
```bash
docker exec -it hadoop-master bash -lc "hdfs dfs -ls -R /out | head -100"
```

Check HBase table:
```bash
docker exec -it hadoop-master bash -lc "echo \"scan 'chicago_outdoor_air_quality', {LIMIT => 5}\" | hbase shell -n 2>/dev/null | sed -n '1,120p'"
```

Query HBase row count:
```bash
docker exec -it hadoop-master bash -lc "CP=\"\$(hbase classpath)\"; java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries count"
```

Get latest record for a sensor:
```bash
docker exec -it hadoop-master bash -lc "CP=\"\$(hbase classpath)\"; java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries latest DIPDE7442"
```

Export a full day to CSV:
```bash
docker exec -it hadoop-master bash -lc "CP=\"\$(hbase classpath)\"; java -cp /root/app.jar:\$CP tn.insat.tp4.hbase.HBaseQueries exportDay 2026-02-01 /root/export-2026-02-01.csv" && docker cp hadoop-master:/root/export-2026-02-01.csv ./
```

## 8) Run the Dashboard Server

The dashboard visualizes Spark outputs (per_sensor, daily, anomalies, rolling averages).

### 8.1 Prepare dashboard data

Copy Spark outputs from HDFS to container local filesystem:

**Bash/Linux:**
```bash
docker exec -it hadoop-master bash -lc "\
  mkdir -p /dashboard/spark_out && \
  hdfs dfs -get /out/spark/* /dashboard/spark_out/ 2>/dev/null && \
  hdfs dfs -get /out/rolling /dashboard/rolling_out 2>/dev/null && \
  ls -lh /dashboard/ \
"
```

**PowerShell:**
```powershell
docker exec -it hadoop-master bash -lc "mkdir -p /dashboard/spark_out && hdfs dfs -get /out/spark/* /dashboard/spark_out/ 2>/dev/null && hdfs dfs -get /out/rolling /dashboard/rolling_out 2>/dev/null && ls -lh /dashboard/"
```

### 8.2 Start the dashboard

Kill any existing dashboard process first:
```bash
docker exec -it hadoop-master bash -lc "pkill -f DashboardServer"
```

### 8.2.1 Install Chart.js locally (for graphs)

If the container has no internet, download Chart.js on the host and copy it into the container. Prefer Chart.js v4 (UMD build):

Host (download + copy):
```bash
curl -sSL -o chart.umd.min.js https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js
docker exec -it hadoop-master bash -lc "mkdir -p /dashboard/static"
docker cp chart.umd.min.js hadoop-master:/dashboard/static/chart.min.js
rm chart.umd.min.js
```

Or, if the container has internet, run inside container:
```bash
docker exec -it hadoop-master bash -lc "mkdir -p /dashboard/static && curl -sSL -o /dashboard/static/chart.min.js https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"
```

The dashboard will look for `/static/chart.min.js` and render interactive charts.

Start on port 9090:

**Bash/Linux:**
```bash
docker exec -it hadoop-master bash -lc "\
  java -cp /root/app.jar tn.insat.dashboard.DashboardServer 9090 /dashboard/spark_out \
"
```

**PowerShell:**
```powershell
docker exec -it hadoop-master bash -lc "java -cp /root/app.jar tn.insat.dashboard.DashboardServer 9090 /dashboard/spark_out"
```

### 8.3 Port forwarding from host to container

If port 9090 is not mapped in Docker, create a socat relay (without recreating the container):

```bash
docker run -d --rm --name dashboard-forward \
  -p 9090:9090 \
  --network hadoop \
  alpine/socat TCP-LISTEN:9090,reuseaddr,fork TCP:hadoop-master:9090
```

Verify it's running:
```bash
docker ps | grep dashboard-forward
```

To stop the forwarder later:
```bash
docker stop dashboard-forward
```

### 8.4 Access the dashboard

Open your browser and navigate to:
```
http://localhost:9090
```

### 8.5 If dashboard is blank (no charts appear)

**Test API endpoints directly:**

Test per_sensor endpoint:
```bash
docker exec -it hadoop-master bash -lc "curl -s http://localhost:9090/api/per_sensor | head -c 500"
```

Test daily endpoint:
```bash
docker exec -it hadoop-master bash -lc "curl -s http://localhost:9090/api/daily | head -c 500"
```

**Common issues:**
- **Chart.js CDN not loading** (no internet in container):
  - The dashboard relies on external CDN. If charts don't appear, it's likely Chart.js failed to load.
  - Check browser console (F12) for errors.
  - Alternative: run a simple table-view by querying the API directly from terminal above.

- **Dashboard shows "Reading Spark outputs from:" but no data:**
  - Verify CSV files exist: `docker exec -it hadoop-master ls -lh /dashboard/spark_out/*/`
  - Verify CSV files have content: `docker exec -it hadoop-master head -2 /dashboard/spark_out/per_sensor/part-*.csv`
  - Check if rolling_out is populated: `docker exec -it hadoop-master ls -lh /dashboard/rolling_out/`

### 8.6 Quick API validation

View JSON data directly (bypasses Chart.js):

**Per-sensor data (top 10 sensors by PM2.5):**
```bash
docker exec -it hadoop-master bash -lc "curl -s http://localhost:9090/api/per_sensor | python3 -m json.tool 2>/dev/null | head -50"
```

**Daily averages:**
```bash
docker exec -it hadoop-master bash -lc "curl -s http://localhost:9090/api/daily | python3 -m json.tool 2>/dev/null | head -30"
```

**Anomalies:**
```bash
docker exec -it hadoop-master bash -lc "curl -s http://localhost:9090/api/anomalies | python3 -m json.tool 2>/dev/null | head -30"
```

### 8.7 Dashboard server info

- Default port: 9090
- Base directory: /dashboard/spark_out (contains per_sensor/, daily/, anomalies/)
- Rolling averages dir: /dashboard/rolling_out (auto-discovered)
- All endpoints serve JSON from CSV files
- No external database required; reads directly from disk

## 9) Matplotlib visualization on the host

If you want a more readable offline visualization without using the dashboard UI, use the Python script in `scripts/visualize.py`.

### 9.1 Install Python dependencies

Create a virtual environment if needed, then install the required packages:

```bash
python -m venv .venv
source .venv/bin/activate   # on Windows use: .venv\Scripts\activate
pip install -r scripts/requirements.txt
```

### 9.2 Generate plots from container outputs

Copy the Spark outputs from the container and generate PNG charts:

```bash
python scripts/visualize.py --copy-from-container
```

This creates a `plots/` folder with:
- `per_sensor_top15.png`
- `daily_pm25.png`
- `anomalies.png`
- `rolling_<sensor>.png`

### 9.3 Optional flags

- `--show` opens the figures interactively after saving them.
- `--sensor <SENSOR_ID>` selects a specific sensor for the rolling-averages plot.
- `--outdir <folder>` changes the output directory for the PNG files.

Example:

```bash
python scripts/visualize.py --copy-from-container --sensor DIPDE7442 --show
```

## Notes
- The GUAVA flags in Spark submit are useful when there is a classpath conflict between Spark and Hadoop libraries.
- If Spark output path already exists, remove it first with hdfs dfs -rm -r -f <path>.
- If HBase fails with missing classes, use hbase org.apache.hadoop.util.RunJar or include $(hbase classpath) in java -cp.
- Dashboard expects CSV files in subdirectories (per_sensor/, daily/, anomalies/). If no data appears, check that Spark jobs completed successfully and outputs exist in HDFS.
- Dashboard server is lightweight with no external dependencies; it reads CSV files directly and converts them to JSON on-the-fly.
- Rolling averages chart dropdown auto-populates from rolling_out directory; ensure RollingAveragesSpark job has completed.

## Troubleshooting

**Dashboard shows empty/blank page:**
- API endpoints ARE working (verified above)
- Chart.js CDN may not be loading (no internet in container)
- Check browser console (F12) for JavaScript errors
- Verify CSV data exists: `docker exec -it hadoop-master ls -lh /dashboard/spark_out/*/part-*.csv`
- Query API directly with curl (see section 8.5 above)

**Dashboard shows empty charts:**
- Verify Spark jobs completed: `hdfs dfs -ls -R /out`
- Check CSV files copied to container: `docker exec -it hadoop-master ls -lh /dashboard/spark_out`
- Verify the CSV files have data: `docker exec -it hadoop-master head -5 /dashboard/spark_out/per_sensor/part-*.csv`
- Test API with curl: `docker exec -it hadoop-master bash -lc "curl -s http://localhost:9090/api/per_sensor | head -c 200"`

**Spark job fails with classpath errors:**
- Ensure GUAVA variable resolves: `docker exec -it hadoop-master ls -1 /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar`
- Check HDFS output directory exists and is writable: `hdfs dfs -ls /out`

**HBase ingestion is slow:**
- Reduce the row_limit parameter: `... CsvToHBase /root/xfya-dxtq.csv 10000` (default is all rows)
- Monitor progress in container logs; writes are batched for performance

**Dashboard port already in use:**
- Kill existing process: `docker exec -it hadoop-master bash -lc "pkill -f DashboardServer"`
- Check running processes: `docker exec -it hadoop-master bash -lc "ps aux | grep -i dashboard"`
- Change the port in startup command: use 9090, 9091, etc.

## Quick Reference (Copy-Paste Workflow)

```bash
# 1. Start all services
docker exec -it hadoop-master bash -lc "start-dfs.sh && start-yarn.sh && start-hbase.sh && sleep 3"

# 2. Upload data
docker cp xfya-dxtq.csv hadoop-master:/root/
docker exec -it hadoop-master bash -lc "hdfs dfs -mkdir -p /data && hdfs dfs -put -f /root/xfya-dxtq.csv /data/"

# 3. Build and deploy
mvn clean package -DskipTests
docker cp target/chicago-air-quality-bigdata-1.0-SNAPSHOT.jar hadoop-master:/root/app.jar

# 4. Run Spark jobs (with GUAVA classpath fix)
docker exec -it hadoop-master bash -lc 'GUAVA=$(ls -1 /usr/local/hadoop/share/hadoop/common/lib/guava-*.jar 2>/dev/null | head -n1); spark-submit --master yarn --conf spark.yarn.user.classpath.first=true --conf spark.driver.userClassPathFirst=true --conf spark.executor.userClassPathFirst=true --conf spark.driver.extraClassPath=$GUAVA --conf spark.executor.extraClassPath=$GUAVA --class tn.insat.tp2.spark.ChicagoAirQualitySpark /root/app.jar hdfs:///data/xfya-dxtq.csv hdfs:///out/spark && hdfs dfs -ls /out/spark'

# 5. Copy outputs for dashboard
docker exec -it hadoop-master bash -lc "mkdir -p /dashboard/spark_out && hdfs dfs -get /out/spark/* /dashboard/spark_out/ 2>/dev/null && ls -lh /dashboard/spark_out"

# 6. Start dashboard
docker exec -it hadoop-master bash -lc "java -cp /root/app.jar tn.insat.dashboard.DashboardServer 8080 /dashboard/spark_out &"

# 7. Open browser to http://localhost:8080
```
