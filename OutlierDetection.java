package projectTwo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class OutlierDetection {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			String[] rk = conf.get("rk").split(",");
			int r = Integer.valueOf(rk[0]);
			int k = Integer.valueOf(rk[1]);
			// pointID, x, y
			String[] data = value.toString().split(",");
			int pointX = Integer.valueOf(data[1]);
			int pointY = Integer.valueOf(data[2]);
			//int quadrant = 1;
/*
 * We divide the gridspace into 16 quadrants and add a buffer along all of the quadrant boundries by adding the radius value to the borders
 * in the event that a point is right on the edge, some of its neighbors may be in a neighboring quadrant as described in:
 * Multi-Tactic Distance-based Outlier Detection (2017) by Cao et. al.
 */
			// output <Quadrant #: 0 or 1 for non-support vs support, r, k, pointX, pointY>
			//the following are quadrant excluding supporting points
			if(pointX > 5000 && pointY > 5000) {
				//quadrant 1 non-supporting
				//<'Quadrant 1': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 1") , new Text("0" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			else if(pointX > 5000 && pointY < 5001) {
				//quadrant 4 non-supporting
				//<'Quadrant 4': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 4") , new Text("0" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			else if(pointX < 5001 && pointY > 5000) {
				//quadrant 2 non-supporting
				//<'Quadrant 2': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 2") , new Text("0" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			else if(pointX < 5001 && pointY < 5001) {
				//quadrant 3 non-supporting
				//<'Quadrant 3': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 3") , new Text("0" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			//the following are quadrant assignments including supporting points
			//quadrant 1 supporting points
			if(pointX > 5000 - r && pointX < 5001 && pointY > 5000) {
				//quadrant 1 supporting point on quadrant 2 side
				//<'Quadrant 1': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 1") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX > 5000 && pointY > 5000 - r && pointY < 5001) {
				//quadrant 1 supporting point on quadrant 4 side
				//<'Quadrant 1': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 1") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX > 5000 - r && pointX < 5001 && pointY > 5000 - r && pointY < 5001) {
				//quadrant 1 supporting point on quadrant 3 side
				//<'Quadrant 1': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 1") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			//quadrant 2 supporting points
			if(pointX < 5001 && pointX > 5001 - r && pointY > 5000) {
				//quadrant 2 supporting point on quadrant 1 side
				//<'Quadrant 2': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 2") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX < 5001 && pointY > 5000 - r && pointY < 5001) {
				//quadrant 2 supporting point on quadrant 3 side
				//<'Quadrant 2': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 2") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX < 5001 + r && pointX > 5000 && pointY > 5000 - r && pointY < 5001) {
				//quadrant 2 supporting point on quadrant 4 side
				//<'Quadrant 2': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 2") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			//quadrant3 supporting points
			if(pointX < 5001 + r && pointX > 5000 && pointY < 5001) {
				//quadrant 3 supporting point on quadrant 4 side
				//<'Quadrant 3': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 3") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX < 5001 && pointY < 5001 + r && pointY > 5000) {
				//quadrant 3 supporting point on quadrant 2 side
				//<'Quadrant 3': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 3") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX < 5001 + r && pointX > 5000 && pointY < 5001 + r && pointY > 5000) {
				//quadrant 3 supporting point on quadrant 1 side
				//<'Quadrant 3': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 3") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			//quadrant 4 supporting points
			if(pointX > 5000 && pointY < 5001 + r && pointY > 5000) {
				//quadrant 4 supporting point on quadrant 1 side
				//<'Quadrant 4': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 4") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX > 5001 - r && pointX < 5000 && pointY < 5001) {
				//quadrant 4 supporting point on quadrant 3 side
				//<'Quadrant 4': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 4") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
			if(pointX > 5001 - r && pointX < 5000 && pointY < 5001 + r && pointY > 5000) {
				//quadrant 4 supporting point on quadrant 2 side
				//<'Quadrant 4': 0, r, k, pointX, pointY>
				context.write(new Text("Quadrant 4") , new Text("1" + "," + Integer.toString(r) + "," + Integer.toString(k) +
						"," + Integer.toString(pointX) + "," + Integer.toString(pointY)));
			}
		}
	}
	
//	public static class Combine extends Reducer<LongWritable, Text, Text, Text> {
//		public void reduce(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//		}
//	}
//	
//	public static class Reduce extends Reducer<Text, Text, Text, Text> {
//		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
//			List<String> points = new ArrayList<String>();
//            List<String> rectangles = new ArrayList<String>();
//            while (value.iterator().hasNext()) {
//                String inp = value.iterator().next().toString();
//                if (inp.split(",").length == 5) {
//                	rectangles.add(inp);
//                } else {
//                	points.add(inp);
//                }
//            }
//            for (String rectangle:rectangles) {
//                String[] rectValues = rectangle.split(",");
//                String rectID = rectValues[0];
//                int rectX1 = Integer.valueOf(rectValues[1]);
//                int rectY1 = Integer.valueOf(rectValues[2]);
//                int rectX2 = Integer.valueOf(rectValues[3]);
//                int rectY2 = Integer.valueOf(rectValues[4]);
//                for (String point:points) {
//                    String[] pointValues = point.split(",");
//                    int pointX = Integer.valueOf(pointValues[0]);
//                    int pointY = Integer.valueOf(pointValues[1]);
//                    if (pointX >= rectX1 && pointX <= rectX2 && pointY >= rectY1 && pointY <= rectY2) {
//                    	// <rectID, (x,y)>
//                        context.write(new Text(rectID), new Text("(" + Integer.toString(pointX) + "," + Integer.toString(pointY) + ")"));
//                    }
//                }
//            }
//		}
//	}
	
	public static void main(String[]args) throws Exception {
        Configuration conf = new Configuration();
        // r, k
        if (args.length == 4) {
            conf.set("rk",args[2]+","+args[3]);
        } else {
        	System.out.println("Missing input params...");
            System.exit(0);
        }
        Job job = Job.getInstance(conf, "OutlierDetection");
        job.setJarByClass(OutlierDetection.class);
        job.setMapperClass(Map.class);
//        job.setCombinerClass(Combine.class);
//        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // /user/ds503/input/p.csv id, x, y
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // /user/ds503/output/p2q2
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}