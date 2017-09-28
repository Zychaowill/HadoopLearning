package mapreduce.topk2;

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

public class GroupByAgeDescScoreExample extends Configured implements Tool {

	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 2) {
			System.out.println("Usage: GroupByAgeDescScoreExample <in> <out>");
			System.exit(2);
		}

		ToolRunner.run(conf, new GroupByAgeDescScoreExample(), otherArgs);
	}

	@Override
	public int run(String[] args) throws Exception {
		FileSystem fs = FileSystem.get(getConf());
		Path outPath = new Path(args[1]);
		if (fs.exists(outPath)) {
			fs.delete(outPath, true);
		}

		Job job = Job.getInstance(getConf(), "GroupByAgeDescScoreExampleJob");

		job.setJarByClass(GroupByAgeDescScoreExample.class);

		job.setMapperClass(MyMapper.class);
		job.setMapOutputKeyClass(Person.class);
		job.setMapOutputValueClass(NullWritable.class);
		FileInputFormat.setInputPaths(job, new Path(args[0]));

		job.setReducerClass(MyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Person.class);
		FileOutputFormat.setOutputPath(job, outPath);

		return job.waitForCompletion(true) ? 0 : 1;
	}

	public static class MyMapper extends Mapper<LongWritable, Text, Person, NullWritable> {

		private Person person = new Person();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			System.out.println("MyMapper in<" + key.get() + "," + value.toString() + ">");

			String line = value.toString();
			String[] infos = line.split("\t");

			String name = infos[0];
			Integer age = Integer.parseInt(infos[1]);
			String gender = infos[2];
			Integer score = Integer.parseInt(infos[3]);

			person.set(name, age, gender, score);
			context.write(person, NullWritable.get());
			System.out.println("MyMapper out<" + person + ">");
		}
	}

	public static class MyReducer extends Reducer<Person, NullWritable, Text, Person> {

		private Text k = new Text();

		@Override
		protected void reduce(Person key, Iterable<NullWritable> v2s, Context context)
				throws IOException, InterruptedException {
			System.out.println("MyReducer in<" + key + ">");

			String name = key.getName();
			k.set(name);

			context.write(k, key);

			System.out.println("MyReducer out<" + k + "," + key + ">");
		}
	}
}
