package hadoop.myKmean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashSet;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.*;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

public class App {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {

		int iterator = 0;	//Counter for Loop of Jobs
		double error = 0;		//Average error 
		int number = 3;		//Number of Centroids
		Path in = new Path("/kmean/input/data.txt");
		Path center = new Path("/kmean/center");
		boolean stop = false;
		while (!stop) {
			/*
			 * Setup Job
			 */
			Configuration conf = new Configuration();
			conf.set("error", Double.toString(error));
			conf.set("number", Integer.toString(number));
			conf.set("iterator", Integer.toString(iterator));
			conf.set("center", center.toString());
			
			Job job = Job.getInstance(conf);
			job.setJobName("KMeans Clustering " + iterator);

			job.setMapperClass(MyMapper.class);
			job.setReducerClass(MyReducer.class);
			job.setJarByClass(MyMapper.class);
			
			Path out = new Path("/kmean/out" + iterator);
			FileSystem fs = FileSystem.get(conf);
			if(fs.exists(out)) 
				fs.delete(out, true);
			
			FileInputFormat.addInputPath(job, in);
			FileOutputFormat.setOutputPath(job, out);
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			// Run Job
			job.waitForCompletion(true);
			

			Path err = new Path("/kmean/err");
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(err)));
			error = Double.parseDouble(br.readLine());
			br.close();
			
			if(error < number * 10) 
				stop = true;
			
			Path errOut = new Path("/kmean/err");
			OutputStream name = fs.create(errOut);
			BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(name));
			brOut.write("0.0");
			brOut.close();
			
			iterator ++;
		}

	}
}
