import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.StringTokenizer;
import java.util.TreeMap;

public class Recommend {


    public static class Mapper1 extends Mapper<Object, Text, Text, Text> {

        private IntWritable userId = new IntWritable();
        private IntWritable itemId = new IntWritable();
        private DoubleWritable score = new DoubleWritable();
        private HashMap<Integer, HashSet<Text>> scores = new HashMap<>();
        private HashMap<Integer, HashSet<Integer>> historyMatrix = new HashMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String[] values = itr.nextToken().split(",");
                userId.set(Integer.parseInt(values[0]));
                itemId.set(Integer.parseInt(values[1]));
                score.set(Double.parseDouble(values[2]));

                //Build score matrix
                if (!scores.containsKey(Integer.parseInt(values[0]))) {
                    scores.put(Integer.parseInt(values[0]), new HashSet<>());
                }
                scores.get(Integer.parseInt(values[0])).add(new Text(itemId.toString()+"="+score.toString()));

                // Build the history matrix
                if (!historyMatrix.containsKey(Integer.parseInt(values[0]))) {
                    historyMatrix.put(Integer.parseInt(values[0]), new HashSet<>());
                }
                // For each user add the items.
                historyMatrix.get(Integer.parseInt(values[0])).add(Integer.parseInt(values[1]));
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Build Cooccurrence matrix
            for (Integer user : historyMatrix.keySet()) {
                for (Integer item: historyMatrix.get(user)) {
                    for (Integer item2: historyMatrix.get(user)) {
                        if (item.equals(item2)) {
                            context.write(new Text("C-" + item.toString() + "=" + item2.toString()), new Text("0"));
                        } else {
                            context.write(new Text("C-" + item.toString() + "=" + item2.toString()), new Text("1"));
                        }
                    }
                }
            }

            //Output the scores
            for (Integer user : scores.keySet()) {
                context.write(new Text( "S-"+user.toString()), new Text(scores.get(user).toString()));
            }
        }
    }

    public static class Reducer1 extends Reducer<Text,Text,Text,Text> {
        private TreeMap<Integer, TreeMap<Integer, Integer>> cooccurenceMatrix = new TreeMap<>();
        private Text value = new Text();

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            // Load the co-occurence matrix
            if (key.toString().charAt(0)=='C') {
                Integer sum = 0;
                for (Text val: values) {
                    sum += Integer.parseInt(val.toString());
                }
                value.set(sum.toString());
                context.write(key, value);
            } else {
                for (Text val: values) {
                    context.write(key, val);
                }
            }
        }
    }

    public static class Mapper2 extends Mapper<Object, Text, Text, DoubleWritable> {

        private HashMap<Integer, HashMap<Integer, Double>> scores = new HashMap<>();
        private HashMap<Integer, HashMap<Integer, Integer>> coocurenceMatrix = new HashMap<>();

        // Load the scores and co-occurence matrix
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            if (key.toString().charAt(0) == 'C') {
                String[] values = key.toString().substring(2).split("=");
                if (!coocurenceMatrix.containsKey(Integer.parseInt(values[0]))) {
                    coocurenceMatrix.put(Integer.parseInt(values[0]), new HashMap<>());
                }
                coocurenceMatrix.get(Integer.parseInt(values[0])).put(Integer.parseInt(values[1]), Integer.parseInt(value.toString()));
            } else {
                if (!scores.containsKey(Integer.parseInt(key.toString().substring(2)))) {
                    scores.put(Integer.parseInt(key.toString().substring(2)), new HashMap<>());
                }
                String[] values = value.toString().substring(1, value.toString().length() - 1).split(", ");
                for (String val: values) {
                    String[] numbers = val.split("=");
                    scores.get(Integer.parseInt(key.toString().substring(2))).put(Integer.parseInt(numbers[0]), Double.parseDouble(numbers[1]));
                }
            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // Output the final scores
            for (Integer user: scores.keySet()) {
                for (Integer item : coocurenceMatrix.keySet()) {
                    for (Integer item2 : coocurenceMatrix.get(item).keySet()) {
                        if (scores.get(user).containsKey(item2)) {
                            context.write(new Text(user.toString() + "=" + item.toString()), new DoubleWritable(scores.get(user).get(item2) * coocurenceMatrix.get(item).get(item2)));
                        }
                    }
                }
            }
        }
    }

    // Compute the final sum
    public static class Reducer2 extends Reducer< Text, DoubleWritable, Text, DoubleWritable> {
        private DoubleWritable value = new DoubleWritable();

        public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            Double sum = 0.0;
            for (DoubleWritable val : values) {
                sum += val.get();
            }
            value.set(sum);
            if (sum > 0.0) {
                context.write(key, value);
            }
        }
    }


   // Map function to sort values by composite key
    public static class Mapper3 extends Mapper<Text, Text, CompositeKey, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] arguments = key.toString().split("=");
            context.write(new CompositeKey(arguments[0], Double.parseDouble(value.toString())), new Text(arguments[1]));
        }
    }


    public static class Reducer3 extends Reducer<CompositeKey, Text, CompositeKey, Text> {

        public void reduce(CompositeKey key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            for (Text val : values) {
                String test = val.toString() + ","+ key.returnValue();
                String[] arguments = test.split(",");
                context.write(key, new Text(arguments[0] + "," + arguments[1]));
            }
        }

    }

    // Final mapper to output the values in the desired format
    public static class Mapper4 extends Mapper<Text, Text, Text, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            String[] arguments = value.toString().split(",");
            context.write(new Text(key), new Text(arguments[0] + "," + arguments[1]));
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(CompositeKey.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            CompositeKey ip1 = (CompositeKey) w1;
            CompositeKey ip2 = (CompositeKey) w2;
            int cmp = ip1.getFirst().compareTo(ip2.getFirst());
            if (cmp != 0) {
                return cmp;
            }
            return ip2.getSecond().compareTo(ip1.getSecond());
        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Compute History Matrix and Co-Occurence Matrix");
        job1.setJarByClass(Recommend.class);
        FileInputFormat.setInputPaths(job1, new Path(args[0]));
        FileOutputFormat.setOutputPath(job1, new Path("recommendation/input/temp"));
        job1.setMapperClass(Mapper1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        job1.waitForCompletion(true);

        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2,"Compute Final Score");
        job2.setJarByClass(Recommend.class);
        FileInputFormat.setInputPaths(job2, new Path("recommendation/input/temp"));
        FileOutputFormat.setOutputPath(job2, new Path("recommendation/input/temp2"));
        job2.setMapperClass(Mapper2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setCombinerClass(Reducer2.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(DoubleWritable.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.waitForCompletion(true);

        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3,"Sorting Based on Composite Key");
        job3.setJarByClass(Recommend.class);
        FileInputFormat.setInputPaths(job3, new Path("recommendation/input/temp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[1]));
        job3.setMapperClass(Mapper3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setCombinerClass(Reducer3.class);
        job3.setSortComparatorClass(KeyComparator.class);
        job3.setOutputKeyClass(CompositeKey.class);
        job3.setOutputValueClass(Text.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        System.exit(job3.waitForCompletion(true) ? 0: 1);


    }
}