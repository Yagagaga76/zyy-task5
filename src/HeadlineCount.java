import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.*;

public class HeadlineCount {

    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Set<String> stopWords = new HashSet<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            URI[] cacheFiles = context.getCacheFiles();
            if (cacheFiles != null && cacheFiles.length > 0) {
                try (BufferedReader br = new BufferedReader(new InputStreamReader(
                        FileSystem.get(context.getConfiguration()).open(new Path(cacheFiles[0]))))) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        stopWords.add(line.trim().toLowerCase());
                    }
                }
            }
        }

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            String[] data = value.toString().split(",");
            if (data.length > 1) {
                String[] words = data[1].toLowerCase().replaceAll("[^a-zA-Z ]", "").split("\\s+");

                for (String w : words) {
                    if (!stopWords.contains(w) && !w.isEmpty()  && w.length() > 1) {
                        word.set(w);
                        context.write(word, one);
                    }
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        private TreeMap<String, Integer> countMap = new TreeMap<>();

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            countMap.put(key.toString(), sum);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            List<Map.Entry<String, Integer>> list = new ArrayList<>(countMap.entrySet());
            list.sort(Map.Entry.<String, Integer>comparingByValue().reversed());

            int rank = 1;
            for (Map.Entry<String, Integer> entry : list) {
                if (rank > 100) break; // Only output the top 100
                context.write(new Text(rank + ": " + entry.getKey()), new IntWritable(entry.getValue()));
                rank++;
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "Headline Word Count");
        job.setJarByClass(HeadlineCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.addCacheFile(new URI("hdfs:///input/stop-word-list.txt"));



        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}


