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

public class SpatialJoin {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			int windowX1;
			int windowY1;
			int windowX2;
			int windowY2;
			// get window coordinates if they were passed
			if(!(conf.get("windowCoords").equals("null"))) {
				String[] windowCoords = conf.get("windowCoords").split(",");
				windowX1 = Integer.valueOf(windowCoords[0]);
				windowY1 = Integer.valueOf(windowCoords[1]);
				windowX2 = Integer.valueOf(windowCoords[2]);
				windowY2 = Integer.valueOf(windowCoords[3]);
			} else {
				// otherwise we assume the window is the entire gridspace
				windowX1 = 0;
				windowY1 = 0;
				windowX2 = 10000;
				windowY2 = 10000;
			}
			String[] data = value.toString().split(",");
			// if the input is from r.csv
			if(data.length == 5) {
				int rectID = Integer.valueOf(data[0]);
				int rectX1 = Integer.valueOf(data[1]);
				int rectY1 = Integer.valueOf(data[2]);
				int width = Integer.valueOf(data[3]);
				int height = Integer.valueOf(data[4]);
				int rectX2 = rectX1 + width;
				int rectY2 = rectY1 + height;
				if(rectX1 >= windowX1 && rectX2 <= windowX2 && rectY1 >= windowY1 && rectY2 <= windowY2) {
					// <"rectangle", (rectID, rectX1, rectY1, rectX2, rectY2)>
					System.out.println("rectangle: " + rectX1 + " " + rectY1 + " " + rectX2 + " " + rectY2);
					System.out.println(value.toString());
					context.write(new Text("rectangle"), new Text(Integer.toString(rectID) + "," + Integer.toString(rectX1) + "," + Integer.toString(rectY1) +
							"," + Integer.toString(rectX2) + "," + Integer.toString(rectY2)));
				}
			}
			else {
				// if the input is from p.csv
				int pointID = Integer.valueOf(data[0]);
				int pointX = Integer.valueOf(data[1]);
				int pointY = Integer.valueOf(data[2]);
				if(pointX >= windowX1 && pointX <= windowX2 && pointY >= windowY1 && pointY <= windowY2) {
					System.out.println("point: " + pointX + " " + pointY);
					System.out.println(value.toString());
					// <"point", (pointX, pointY)>
					context.write(new Text("point"), new Text(Integer.toString(pointX) + "," + Integer.toString(pointY)));
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		public void reduce(Text key, Iterable<Text> value, Context context) throws IOException, InterruptedException {
			List<String> points = new ArrayList<String>();
            List<String> rectangles = new ArrayList<String>();
            while (value.iterator().hasNext()) {
                String inp = value.iterator().next().toString();
                if (inp.split(",").length == 5) {
                	System.out.println("map rectangle values: " + inp);
                	rectangles.add(inp);
                } else {
                	System.out.println("map point values: " + inp);
                	points.add(inp);
                }
            }
            for (String rectangle:rectangles) {
                String[] rectValues = rectangle.split(",");
                String rectID = rectValues[0];
                int rectX1 = Integer.valueOf(rectValues[1]);
                int rectY1 = Integer.valueOf(rectValues[2]);
                int rectX2 = Integer.valueOf(rectValues[3]);
                int rectY2 = Integer.valueOf(rectValues[4]);
                System.out.println("rectangle: " + rectX1 + " " + rectY1 + " " + rectX2 + " " + rectY2);
                System.out.println(points.toString());
                for (String point:points) {
                	System.out.println("am i here?");
                    String[] pointValues = point.split(",");
                    int pointX = Integer.valueOf(pointValues[0]);
                    int pointY = Integer.valueOf(pointValues[1]);
                    if (pointX >= rectX1 && pointX <= rectX2 && pointY >= rectY1 && pointY <= rectY2) {
                    	System.out.println("point: " + pointX + " " + pointY);
                    	// <rectID, (x,y)>
                        context.write(new Text(rectID), new Text("(" + Integer.toString(pointX) + "," + Integer.toString(pointY)));
                    }
                }
            }
		}
	}
	
	public static void main(String[]args) throws Exception {
        Configuration conf = new Configuration();
        // x1, y1, x2, y2
        if (args.length == 7) {
            conf.set("windowCoords",args[3]+","+args[4]+","+args[5]+","+args[6]);
        } else {
            conf.set("windowCoords","null");
        }
        Job job = Job.getInstance(conf, "SpatialJoin");
        job.setJarByClass(SpatialJoin.class);
        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // /user/ds503/input/p.csv id, x, y
        FileInputFormat.addInputPath(job, new Path(args[0]));
        // /user/ds503/input/r.csv id, bottom left x, bottom left y, height, width
        FileInputFormat.addInputPath(job, new Path(args[1]));
        // /user/ds503/output/p2q1
        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
