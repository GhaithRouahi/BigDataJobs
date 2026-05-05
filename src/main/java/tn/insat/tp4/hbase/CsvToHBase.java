package tn.insat.tp4.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import tn.insat.common.Csv;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;

/**
 * Ingests the Chicago outdoor air quality CSV into HBase.
 * Table: chicago_outdoor_air_quality
 * CFs: m (pm25,no2,humidity,temp), g (lat,lon,location), meta (sensor_name)
 * RowKey: <datasourceid>_<time>
 *
 * Usage: CsvToHBase <input.csv> [row_limit]
 */
public class CsvToHBase {
    private static final byte[] CF_M   = Bytes.toBytes("m");
    private static final byte[] CF_G   = Bytes.toBytes("g");
    private static final byte[] CF_META= Bytes.toBytes("meta");

    public static void main(String[] args) throws IOException {
        if (args.length < 1) {
            System.err.println("Usage: CsvToHBase <input.csv> [row_limit]");
            System.exit(1);
        }
        String input = args[0];
        long limit = args.length >= 2 ? Long.parseLong(args[1]) : Long.MAX_VALUE;

        Configuration config = HBaseConfiguration.create();
        // Allow overriding connection via env vars when running outside container
        String zkQuorum = System.getenv().getOrDefault("HBASE_ZK_QUORUM", null);
        String zkPort = System.getenv().getOrDefault("HBASE_ZK_PORT", null);
        String znodeParent = System.getenv().getOrDefault("HBASE_ZNODE_PARENT", null);
        if (zkQuorum != null && !zkQuorum.isEmpty()) config.set("hbase.zookeeper.quorum", zkQuorum);
        if (zkPort != null && !zkPort.isEmpty()) config.set("hbase.zookeeper.property.clientPort", zkPort);
        if (znodeParent != null && !znodeParent.isEmpty()) config.set("zookeeper.znode.parent", znodeParent);
        try (Connection connection = ConnectionFactory.createConnection(config);
             Admin admin = connection.getAdmin()) {
            TableName tableName = TableName.valueOf("chicago_outdoor_air_quality");
            ensureTable(admin, tableName);
            try (BufferedMutator mutator = connection.getBufferedMutator(tableName)) {
                ingestCsv(input, limit, mutator);
            }
        }
        System.out.println("HBase ingestion complete.");
    }

    private static void ensureTable(Admin admin, TableName tableName) throws IOException {
        if (!admin.tableExists(tableName)) {
            TableDescriptor td = TableDescriptorBuilder.newBuilder(tableName)
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_M))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_G))
                    .setColumnFamily(ColumnFamilyDescriptorBuilder.of(CF_META))
                    .build();
            admin.createTable(td);
            System.out.println("Created table: " + tableName.getNameAsString());
        }
    }

    private static void ingestCsv(String path, long limit, BufferedMutator mutator) throws IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(path))) {
            String line = br.readLine(); // header
            long count = 0;
            while ((line = br.readLine()) != null && count < limit) {
                String[] f = Csv.split(line);
                if (f.length < 43) continue;
                String id = f[0];
                String time = f[1];
                String sensorName = f[2];
                String no2 = f[22];
                String pm25 = f[24];
                String humid = f[27]; // relhumidambientindividual
                String temp = f[31];  // temperatureambientindividual
                String lat = f[39];
                String lon = f[40];
                String location = f[41];

                if (id == null || id.isEmpty() || time == null || time.isEmpty()) continue;
                String rowKey = (id + "_" + time).replace(':', '_');
                Put put = new Put(Bytes.toBytes(rowKey));

                if (pm25 != null && !pm25.isEmpty()) put.addColumn(CF_M, Bytes.toBytes("pm25"), Bytes.toBytes(pm25));
                if (no2  != null && !no2.isEmpty())  put.addColumn(CF_M, Bytes.toBytes("no2"),  Bytes.toBytes(no2));
                if (humid!= null && !humid.isEmpty())put.addColumn(CF_M, Bytes.toBytes("humidity"), Bytes.toBytes(humid));
                if (temp != null && !temp.isEmpty()) put.addColumn(CF_M, Bytes.toBytes("temp"), Bytes.toBytes(temp));

                if (lat  != null && !lat.isEmpty())  put.addColumn(CF_G, Bytes.toBytes("lat"), Bytes.toBytes(lat));
                if (lon  != null && !lon.isEmpty())  put.addColumn(CF_G, Bytes.toBytes("lon"), Bytes.toBytes(lon));
                if (location != null && !location.isEmpty()) put.addColumn(CF_G, Bytes.toBytes("location"), Bytes.toBytes(location));

                if (sensorName != null && !sensorName.isEmpty()) put.addColumn(CF_META, Bytes.toBytes("sensor_name"), Bytes.toBytes(sensorName));
                put.addColumn(CF_META, Bytes.toBytes("datasourceid"), Bytes.toBytes(id));
                put.addColumn(CF_META, Bytes.toBytes("time"), Bytes.toBytes(time));

                mutator.mutate(put);
                count++;
                if (count % 5000 == 0) System.out.println("Ingested rows: " + count);
            }
        }
    }
}
