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
 * Counts pollutant exceedances per sensor.
 * Thresholds (configurable via -D): pm25.threshold (default 35.4), no2.threshold (default 100.0 ppb-equivalent scale if needed).
 * CSV fields: datasourceid(0), time(1), no2concindividual_value(22), pm2_5concmassindividual_value(24)
 */
public class SensorExceedanceJob {

    public static class Map extends Mapper<Object, Text, Text, Text> {
        double pm25Threshold;
        double no2Threshold;

        @Override
        protected void setup(Context context) {
            pm25Threshold = context.getConfiguration().getDouble("pm25.threshold", 35.4);
            no2Threshold = context.getConfiguration().getDouble("no2.threshold", 100.0);
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            if (line.startsWith("\"datasourceid\"")) return; // header
            String[] f = Csv.split(line);
            if (f.length < 43) return;
            String id = f[0];
            String pm25 = f[24];
            String no2 = f[22];
            if (id == null || id.isEmpty()) return;
            try {
                if (pm25 != null && !pm25.isEmpty() && Double.parseDouble(pm25) > pm25Threshold) {
                    context.write(new Text(id), new Text("PM25:1"));
                }
            } catch (NumberFormatException ignored) {}
            try {
                if (no2 != null && !no2.isEmpty() && Double.parseDouble(no2) > no2Threshold) {
                    context.write(new Text(id), new Text("NO2:1"));
                }
            } catch (NumberFormatException ignored) {}
        }
    }

    public static class Reduce extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int cPM25 = 0, cNO2 = 0;
            for (Text v : values) {
                String s = v.toString();
                if (s.startsWith("PM25:")) cPM25++;
                else if (s.startsWith("NO2:")) cNO2++;
            }
            context.write(key, new Text("pm25_exceedances=" + cPM25 + ", no2_exceedances=" + cNO2));
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: SensorExceedanceJob <input.csv> <output_dir> [-Dpm25.threshold=35.4 -Dno2.threshold=100]");
            System.exit(1);
        }
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Sensor Exceedances (PM2.5/NO2)");
        job.setJarByClass(SensorExceedanceJob.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
