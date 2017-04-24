package hadoop.myKmean;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MyReducer extends Reducer<Text, Text, Text, Text> {
	
	Configuration conf = new Configuration();
	double error = 0;
	
	
	public void reduce(Text key,  Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		

		Configuration conf = context.getConfiguration();
		FileSystem fs = FileSystem.get(conf);
		Path err = new Path("/kmean/err");
		BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(err)));
		error = Double.parseDouble(br.readLine());
		br.close();
		
		double x = 0, y =0;
		double num = 0;

		
		for(Text value : values) {
			Point raw = new Point(value.toString());
			x += raw.getX();
			y += raw.getY();
			num ++;
		}
		x = x/num;
		y = y/num;		

		Point oldCentroid = new Point(key.toString());
		Point newCentroid = new Point(x,y);
		
		double thisError = oldCentroid.distanceTo(newCentroid);
		error += thisError;
		
		Path errOut = new Path("/kmean/err");
		OutputStream name = fs.create(errOut);
		BufferedWriter brOut = new BufferedWriter(new OutputStreamWriter(name));
		brOut.write(error + "");
		brOut.close();
		
		
		context.write(new Text(newCentroid.toString()), new Text(""));
		
	}
}
