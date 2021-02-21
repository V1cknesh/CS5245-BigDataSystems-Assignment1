// Matric Number: A0113973A
// Name: Vicknesh S/O Jegathesan
// TopkCommonWords.java

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.*;

public class TopkCommonWords {


    // First Mapper to read the first text file
    public static class Mapper1 extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private TreeMap<String, IntWritable> firstText = new TreeMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String currentWord = itr.nextToken();
                if (firstText.get(currentWord) == null) {
                    firstText.put(currentWord, one);
                } else {
                    int number = firstText.get(currentWord).get();
                    number += 1;
                    firstText.remove(currentWord);
                    firstText.put(currentWord, new IntWritable(number));
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> firstSet = firstText.keySet();

            for (String word: firstSet) {
                context.write(new Text(word + "-A"), new Text(String.valueOf(firstText.get(word).get())));
            }
        }
    }

    // Second Mapper to read the second text file
    public static class Mapper2 extends Mapper<Object, Text, Text, Text>{

        private final static IntWritable one = new IntWritable(1);
        private TreeMap<String, IntWritable> secondText = new TreeMap<>();

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String currentWord = itr.nextToken();
                if (secondText.get(currentWord) == null) {
                    secondText.put(currentWord, one);
                } else {
                    int number = secondText.get(currentWord).get();
                    number += 1;
                    secondText.remove(currentWord);
                    secondText.put(currentWord, new IntWritable(number));
                }
            }
        }

        public void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> secondSet = secondText.keySet();

            for (String word: secondSet) {
                context.write(new Text(word + "-B"), new Text(String.valueOf(secondText.get(word).get())));
            }
        }
    }


    // Third Mapper to read the stopwords text file
    public static class Mapper3 extends Mapper<Object, Text, Text, Text>{

        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String word = itr.nextToken();
                context.write(new Text(word +"-S"), new Text(String.valueOf(0)));
                context.write(new Text(word +"-S"), new Text(String.valueOf(0)));
            }
        }
    }

    public static class Reducer1 extends Reducer<Text, Text, Text, Text> {

        public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                context.write(key, val);
            }
        }
    }

    // Mapper to filter stopwords and output the commonword and count
    public static class Mapper4 extends Mapper<Text, Text, Text, IntWritable> {
        private HashMap<String, IntWritable> firstText = new HashMap<>();
        private HashMap<String, IntWritable> secondText = new HashMap<>();
        private HashSet<String> stopWords = new HashSet<>();

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String count = itr.nextToken();
                if (key.toString().charAt(key.toString().length()-1) == 'A') {
                    firstText.put(key.toString().substring(0,key.toString().length() - 2), new IntWritable(Integer.parseInt(count)));
                } else if (key.toString().charAt(key.toString().length()-1) == 'B'){
                    secondText.put(key.toString().substring(0,key.toString().length() - 2), new IntWritable(Integer.parseInt(count)));
                } else {
                    stopWords.add(key.toString().substring(0,key.toString().length() - 2));
                }

            }
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            Set<String> firsttemp = firstText.keySet();
            Set<String> secondtemp = secondText.keySet();
            firsttemp.removeAll(stopWords);
            secondtemp.removeAll(stopWords);
            firsttemp.retainAll(secondtemp);

            for (String tempVal : firsttemp) {
                if (firstText.get(tempVal).get() > secondText.get(tempVal).get()) {
                    context.write(new Text(tempVal), secondText.get(tempVal));
                } else {
                    context.write(new Text(tempVal), firstText.get(tempVal));
                }
            }
        }
    }

    public static class Reducer2 extends Reducer<Text, IntWritable, Text, IntWritable> {

        public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

            for (IntWritable val : values) {
                context.write(key, val);
            }
        }
    }


    public static class Mapper5 extends Mapper<Text, Text, IntWritable, Text> {

        public void map(Text key, Text value, Context context) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString());
            while (itr.hasMoreTokens()) {
                String count = itr.nextToken();
                context.write(new IntWritable(Integer.parseInt(count)), new Text(key.toString()));
            }
        }
    }


    // Final Reducer to get the top K words
    public static class Reducer3 extends Reducer<IntWritable, Text, IntWritable, Text> {
        private int i = 0;
        private int K = 20; // Top K records


        public void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (Text val : values) {
                if (i < K) {
                    context.write(key, val);
                    i++;
                }
            }
        }
    }

    public static class KeyComparator extends WritableComparator {
        protected KeyComparator() {
            super(IntWritable.class, true);
        }

        @Override
        public int compare(WritableComparable w1, WritableComparable w2) {
            IntWritable ip1 = (IntWritable) w1;
            IntWritable ip2 = (IntWritable) w2;
            return ip2.compareTo(ip1);
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf1 = new Configuration();
        Job job1 = Job.getInstance(conf1, "Read in input");
        job1.setJarByClass(TopkCommonWords.class);
        job1.setCombinerClass(Reducer1.class);
        job1.setReducerClass(Reducer1.class);
        job1.setOutputKeyClass(Text.class);
        job1.setOutputValueClass(Text.class);
        MultipleInputs.addInputPath(job1, new Path(args[0]), TextInputFormat.class, Mapper1.class);
        MultipleInputs.addInputPath(job1, new Path(args[1]), TextInputFormat.class, Mapper2.class);
        MultipleInputs.addInputPath(job1, new Path(args[2]), TextInputFormat.class, Mapper3.class);
        FileOutputFormat.setOutputPath(job1, new Path("commonwords/input/temp"));
        job1.waitForCompletion(true);


        Configuration conf2 = new Configuration();
        Job job2 = Job.getInstance(conf2, "Filter stopwords and output the count");
        job2.setJarByClass(TopkCommonWords.class);
        job2.setMapperClass(Mapper4.class);
        job2.setCombinerClass(Reducer2.class);
        job2.setReducerClass(Reducer2.class);
        job2.setInputFormatClass(KeyValueTextInputFormat.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job2, new Path("commonwords/input/temp"));
        FileOutputFormat.setOutputPath(job2, new Path("commonwords/input/temp2"));
        job2.waitForCompletion(true);



        Configuration conf3 = new Configuration();
        Job job3 = Job.getInstance(conf3, "Top K words");
        job3.setJarByClass(TopkCommonWords.class);
        job3.setMapperClass(Mapper5.class);
        job3.setCombinerClass(Reducer3.class);
        job3.setReducerClass(Reducer3.class);
        job3.setSortComparatorClass(KeyComparator.class);
        job3.setInputFormatClass(KeyValueTextInputFormat.class);
        job3.setOutputKeyClass(IntWritable.class);
        job3.setOutputValueClass(Text.class);
        FileInputFormat.addInputPath(job3, new Path("commonwords/input/temp2"));
        FileOutputFormat.setOutputPath(job3, new Path(args[3]));
        System.exit(job3.waitForCompletion(true) ? 0: 1);
        
        
    }
}
