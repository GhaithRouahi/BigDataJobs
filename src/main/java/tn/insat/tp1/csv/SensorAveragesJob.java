package tn.insat.tp1.csv;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import tn.insat.common.Csv;

import java.io.IOException;

/**
 * Computes per-sensor averages for PM2.5 and NO2 from the Chicago outdoor sensors CSV.
 * Input columns used: datasourceid(0), time(1), no2concindividual_value(22), pm2_5concmassindividual_value(24)
 */
public class SensorAveragesJob {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("\"datasourceid\"")) return; // skip header
            String[] f = Csv.split(line);
            if (f.length < 43) return;
            String id = f[0];
            String pm25 = f[24];
            String no2 = f[22];
            if (id == null || id.isEmpty()) return;
            if (pm25 != null && !pm25.isEmpty()) {
                context.write(new Text(id), new Text("PM25:" + pm25));
            }
            if (no2 != null && !no2.isEmpty()) {
                context.write(new Text(id), new Text("NO2:" + no2));
            }
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            double sumPM25 = 0, sumNO2 = 0; int cPM25 = 0, cNO2 = 0;
            for (Text v : values) {
                String s = v.toString();
                try {
                    if (s.startsWith("PM25:")) { sumPM25 += Double.parseDouble(s.substring(5)); cPM25++; }
                    else if (s.startsWith("NO2:")) { sumNO2 += Double.parseDouble(s.substring(4)); cNO2++; }
                } catch (NumberFormatException ignored) {}
            }
            String out = String.format("pm25_avg=%.3f, pm25_count=%d, no2_avg=%.3f, no2_count=%d",
                    cPM25 == 0 ? 0.0 : (sumPM25 / cPM25), cPM25,
                    cNO2 == 0 ? 0.0 : (sumNO2 / cNO2), cNO2);
            context.write(key, new Text(out));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SensorAveragesJob <input.csv> <output_dir>");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sensor Averages (PM2.5, NO2)");
        job.setJarByClass(SensorAveragesJob.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
