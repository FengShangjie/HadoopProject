package secondProject;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount {

	public static class NewMapper extends Mapper<Object, Text,Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String[] words = value.toString().split(" ");
			for (String str : words) {
				word.set(str);
				context.write(word, one);
			}
		}
		
	}
	
	public static class NewReducer extends Reducer<Text,IntWritable , Text, IntWritable>
	{
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			// TODO Auto-generated method stub
			int total = 0;
			for (IntWritable val : values) {
				total+=val.get();
			}
			context.write(key, new IntWritable(total));
		}
	}
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		// TODO Auto-generated method stub
		
		Configuration conf =  new Configuration();
		@SuppressWarnings("deprecation")
		Job job = new Job(conf, "wordcount");
		job.setJarByClass(WordCount.class);
		job.setMapperClass(NewMapper.class);
		job.setCombinerClass(NewReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		FileInputFormat.addInputPath(job, new Path("hdfs://root@192.168.123.128:9000/root/input/word.txt"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://root@192.168.123.128:9000/root/output/o3"));
		System.exit(job.waitForCompletion(true)? 0 : 1);
	}

}
