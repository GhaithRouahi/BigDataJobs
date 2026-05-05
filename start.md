HBase quick start + TP4 run (inside Docker cluster)

1) Start HBase (Master + RegionServer)
```bash
docker exec -it hadoop-master bash -lc "\
	if command -v start-hbase.sh >/dev/null 2>&1; then \
		start-hbase.sh; \
	else \
		(command -v hbase-daemon.sh >/dev/null 2>&1 && hbase-daemon.sh start master && hbase-daemon.sh start regionserver) || true; \
	fi; \
	echo \"=== HBase status (simple) ===\"; \
	echo \"status 'simple'\" | hbase shell -n 2>/dev/null | sed -n '1,80p' \
"
```

2) Ingest CSV into HBase (TP4)
- Recommended (uses HBase classpath automatically):
```bash
docker exec -it hadoop-master bash -lc "\
	hbase org.apache.hadoop.util.RunJar /root/app.jar tn.insat.tp4.hbase.CsvToHBase /root/xfya-dxtq.csv 10000 \
"
```

- Alternative (manual classpath):
```bash
docker exec -it hadoop-master bash -lc "\
	CP=\"$(hbase classpath)\"; \
	java -cp /root/app.jar:$CP tn.insat.tp4.hbase.CsvToHBase /root/xfya-dxtq.csv 10000 \
"
```

3) Verify data
```bash
docker exec -it hadoop-master bash -lc "\
	echo list | hbase shell -n 2>/dev/null | grep -E 'TABLE|chicago_outdoor_air_quality' || true; \
	echo \"scan 'chicago_outdoor_air_quality', {LIMIT => 5}\" | hbase shell -n 2>/dev/null | sed -n '1,160p' \
"
```

4) Optional utilities
```bash
# Count all rows
docker exec -it hadoop-master bash -lc "\
	CP=\"$(hbase classpath)\"; \
	java -cp /root/app.jar:$CP tn.insat.tp4.hbase.HBaseQueries count \
"

# Latest record by datasource id
docker exec -it hadoop-master bash -lc "\
	CP=\"$(hbase classpath)\"; \
	java -cp /root/app.jar:$CP tn.insat.tp4.hbase.HBaseQueries latest DIPDE7442 \
"

# Export a day to local CSV inside container
docker exec -it hadoop-master bash -lc "\
	CP=\"$(hbase classpath)\"; \
	java -cp /root/app.jar:$CP tn.insat.tp4.hbase.HBaseQueries exportDay 2026-02-01 /root/export-2026-02-01.csv \
	&& ls -lh /root/export-2026-02-01.csv \
"
```

Notes
- The original NoClassDefFoundError (Missing `org.apache.hadoop.hbase.client.Mutation`) happens if HBase client JARs are not on the classpath. Using `hbase org.apache.hadoop.util.RunJar ...` or `java -cp /root/app.jar:$(hbase classpath) ...` fixes it.
- Ensure RegionServer is running (not just Master). `start-hbase.sh` starts both; otherwise start with `hbase-daemon.sh start master && hbase-daemon.sh start regionserver`.