package compression;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ReflectionUtils;


public class CompressionExample {
	
	public static void main(String[] args) throws IOException, ClassNotFoundException {
		Configuration conf = new Configuration();
		
		Job job = Job.getInstance(conf, "Codec");
		
		job.setJarByClass(CompressionExample.class);
		
		String codecClassName = "org.apache.hadoop.io.compress.BZip2Codec";
//		String codecClassName = "org.apache.hadoop.io.compress.GzipCodec";
		
		Class<?> cls = Class.forName(codecClassName);
		CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(cls, conf);
		String inputFile = "/tmp/data";
		String outFile = inputFile + codec.getDefaultExtension();
		
		FileOutputStream fos = new FileOutputStream(outFile);
		CompressionOutputStream out = codec.createOutputStream(fos);
		FileInputStream in = new FileInputStream(inputFile);
		
		IOUtils.copyBytes(in, out, 4096, false);
		in.close();
		out.close();
	}
}
