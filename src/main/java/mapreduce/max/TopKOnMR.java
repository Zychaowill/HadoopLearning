package mapreduce.max;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;

/**
 * 
 * <p>Title: TopKOnMR</p>
 * <p>Description: </p>
 * @author jangz
 * @date 2017/9/29 15:45
 */
public class TopKOnMR extends Configured implements Tool {
	
	private static final Logger log = Logger.getLogger(TopKOnMR.class);
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
		
		if (otherArgs.length != 2) {
			log.error("Usage: TopKOnMR <in> <out>");
			System.exit(2);
		}
		int resp = ToolRunner.run(conf, new TopKOnMR(), otherArgs);
		System.exit(resp);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path outputDir = new Path(args[1]);
		if (fs.exists(outputDir)) {
			fs.delete(outputDir, true);
		}
		
		Job job = Job.getInstance(getConf(), "TopKOnMRJob");
		
		job.setJarByClass(TopKOnMR.class);
		
		job.setMapperClass(TopKMapper.class);
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		job.setReducerClass(TopKReducer.class);
		job.setOutputKeyClass(LongWritable.class);
		job.setOutputValueClass(NullWritable.class);
		FileOutputFormat.setOutputPath(job, outputDir);
		
		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class TopKMapper extends Mapper<LongWritable, Text, LongWritable, NullWritable> {

		private long max = Long.MIN_VALUE;

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			final long tmp = Long.parseLong(value.toString());
			if (tmp > max) {
				max = tmp;
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}

	public static class TopKReducer extends Reducer<LongWritable, NullWritable, LongWritable, NullWritable> {
		
		private long max = Long.MIN_VALUE;
		
		@Override
		protected void reduce(LongWritable key, Iterable<NullWritable> v2s, Context context)
				throws IOException, InterruptedException {
			final long tmp = key.get();
			if (tmp > max) {
				max = tmp;
			}
		}
		
		@Override
		protected void cleanup(Reducer<LongWritable, NullWritable, LongWritable, NullWritable>.Context context)
				throws IOException, InterruptedException {
			context.write(new LongWritable(max), NullWritable.get());
		}
	}
}
