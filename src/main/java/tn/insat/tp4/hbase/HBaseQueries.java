package tn.insat.tp4.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileWriter;
import java.io.IOException;

/**
 * Simple HBase utilities for the chicago_outdoor_air_quality table.
 * Modes:
 *  - latest <sensorId>: prints latest row for sensor (client-side max by time)
 *  - count: prints total row count (full scan)
 *  - exportDay <yyyy-MM-dd> <out.csv>: exports rows for that date (client filter by time prefix)
 */
public class HBaseQueries {
    private static final TableName TABLE = TableName.valueOf("chicago_outdoor_air_quality");

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: HBaseQueries <mode> [args...]\nModes: latest <sensorId> | count | exportDay <yyyy-MM-dd> <out.csv>");
            System.exit(1);
        }
        Configuration config = HBaseConfiguration.create();
        try (Connection connection = ConnectionFactory.createConnection(config)) {
            switch (args[0]) {
                case "latest":
                    latest(connection, args.length >= 2 ? args[1] : null);
                    break;
                case "count":
                    count(connection);
                    break;
                case "exportDay":
                    if (args.length < 3) {
                        System.err.println("Usage: exportDay <yyyy-MM-dd> <out.csv>");
                        System.exit(2);
                    }
                    exportDay(connection, args[1], args[2]);
                    break;
                default:
                    System.err.println("Unknown mode: " + args[0]);
                    System.exit(3);
            }
        }
    }

    private static void latest(Connection connection, String sensorId) throws IOException {
        if (sensorId == null) {
            System.err.println("Provide sensorId");
            return;
        }
        try (Table table = connection.getTable(TABLE)) {
            Scan scan = new Scan();
            scan.setFilter(new PrefixFilter(Bytes.toBytes(sensorId + "_")));
            ResultScanner rs = table.getScanner(scan);
            Result latest = null;
            for (Result r : rs) {
                if (latest == null || Bytes.compareTo(r.getRow(), latest.getRow()) > 0) {
                    latest = r;
                }
            }
            if (latest == null) {
                System.out.println("No rows for sensor: " + sensorId);
                return;
            }
            printRow(latest);
        }
    }

    private static void count(Connection connection) throws IOException {
        long c = 0;
        try (Table table = connection.getTable(TABLE)) {
            ResultScanner rs = table.getScanner(new Scan().setCaching(1000));
            for (Result ignored : rs) { c++; }
        }
        System.out.println("Row count: " + c);
    }

    private static void exportDay(Connection connection, String day, String outPath) throws IOException {
        try (Table table = connection.getTable(TABLE);
             FileWriter fw = new FileWriter(outPath)) {
            fw.write("rowkey,pm25,no2,humidity,temp,lat,lon,location,sensor_name,datasourceid,time\n");
            Scan scan = new Scan();
            // simple client-side filter: read all and keep those with time prefix (costly but simple)
            ResultScanner rs = table.getScanner(scan);
            for (Result r : rs) {
                String time = get(r, "meta", "time");
                if (time == null || !time.startsWith(day)) continue;
                String line = String.join(",",
                        Bytes.toString(r.getRow()),
                        get(r, "m", "pm25"),
                        get(r, "m", "no2"),
                        get(r, "m", "humidity"),
                        get(r, "m", "temp"),
                        get(r, "g", "lat"),
                        get(r, "g", "lon"),
                        quote(get(r, "g", "location")),
                        quote(get(r, "meta", "sensor_name")),
                        get(r, "meta", "datasourceid"),
                        quote(time)
                );
                fw.write(line + "\n");
            }
        }
        System.out.println("Export complete: " + outPath);
    }

    private static String get(Result r, String cf, String q) {
        byte[] v = r.getValue(Bytes.toBytes(cf), Bytes.toBytes(q));
        return v == null ? "" : Bytes.toString(v);
    }

    private static void printRow(Result r) {
        System.out.println("Row: " + Bytes.toString(r.getRow()));
        System.out.println("  pm25=" + get(r, "m", "pm25") + ", no2=" + get(r, "m", "no2") + 
                ", humidity=" + get(r, "m", "humidity") + ", temp=" + get(r, "m", "temp"));
        System.out.println("  lat=" + get(r, "g", "lat") + ", lon=" + get(r, "g", "lon") +
                ", location=" + get(r, "g", "location"));
        System.out.println("  datasourceid=" + get(r, "meta", "datasourceid") + ", time=" + get(r, "meta", "time") +
                ", sensor_name=" + get(r, "meta", "sensor_name"));
    }

    private static String quote(String s) { return s == null ? "" : '"' + s.replace("\"","'") + '"'; }
}
