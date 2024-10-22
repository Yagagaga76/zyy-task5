import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class StockCount {

    public static class StockMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text stockCode = new Text();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String line = value.toString();
            String[] columns = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"); // Split on commas unless they are in quotes
            if (columns.length > 0) {
                String lastColumn = columns[columns.length - 1].replaceAll("\"", "").trim(); // Remove quotes and trim whitespace
                stockCode.set(lastColumn);
                context.write(stockCode, one);
            }
        }
    }

    public static class StockReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<String, Integer> countMap = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(key.toString(), sum); // Store the sum for each stock code
        }

        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Sort the map by values in descending order
            List<Map.Entry<String, Integer>> list = new ArrayList<>(countMap.entrySet());
            list.sort(Map.Entry.<String, Integer>comparingByValue().reversed());

            // Output with rank
            int rank = 1;
            for (Map.Entry<String, Integer> entry : list) {
                context.write(new Text(rank + ": " + entry.getKey()), new IntWritable(entry.getValue()));
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Stock Count");
        job.setJarByClass(StockCount.class);
        job.setMapperClass(StockMapper.class);
        job.setReducerClass(StockReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}

