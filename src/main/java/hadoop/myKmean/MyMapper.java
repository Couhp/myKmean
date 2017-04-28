package hadoop.myKmean;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, Text> {

	ArrayList<Point> center = new ArrayList<Point>();
	int loop = 0;

	protected void setup(Context context) throws IOException, InterruptedException {
		super.setup(context);
		Configuration conf = context.getConfiguration();

		loop = Integer.parseInt(conf.get("iterator"));

		if (loop == 0) {
			Path centroids = new Path(conf.get("center"));
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroids)));
			String line;
			line = br.readLine();
			while (line != null) {
				// Add default centroid to center
				center.add(new Point(line));
				line = br.readLine();
			}

		}

		else {
			Path centroids = new Path("/user/phuoclh/kmean/out" + (loop - 1) + "/part-r-00000");
			FileSystem fs = FileSystem.get(conf);
			BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroids)));
			String line;
			line = br.readLine();
			while (line != null) {
				// Add default centroid to center
				center.add(new Point(line));
				line = br.readLine();
			}

		}

	}

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		Point p = new Point(value.toString());
		Point pCentroid = center.get(0);
		double minDis = Double.MAX_VALUE;

		for (Point centroid : center) {
			double dis = p.distanceTo(centroid);
			if (dis < minDis) {
				minDis = dis;
				pCentroid = centroid;
			}
		}

		context.write(new Text(pCentroid.toString()), new Text(p.toString()));

	}
}
