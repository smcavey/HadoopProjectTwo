import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.concurrent.ThreadLocalRandom;

public class CreateData {
	
	public static void main(String[]args) {
		int MAX_HEIGHT = 20;
		int MAX_WIDTH = 7;
		
		try(PrintWriter writer = new PrintWriter("p.csv")){
			StringBuilder rowBuilder = new StringBuilder();
			for(int i = 1; i < 6500001; i++) { // create 2M data points
				int x = ThreadLocalRandom.current().nextInt(1, 10001);
				int y = ThreadLocalRandom.current().nextInt(1, 10001);
				rowBuilder.append(String.valueOf(i)); // point id
				rowBuilder.append(",");
				rowBuilder.append(String.valueOf(x));
				rowBuilder.append(",");
				rowBuilder.append(String.valueOf(y));
				rowBuilder.append("\n");
				writer.write(rowBuilder.toString()); // write row to csv
				rowBuilder.setLength(0); // reset row contents
			}
		} catch (FileNotFoundException e){
			System.out.println(e.getMessage());
		}
		
		try(PrintWriter writer = new PrintWriter("r.csv")){
			StringBuilder rowBuilder = new StringBuilder();
			for(int i = 1; i < 4000000; i++) { // create 1.5M rectangles
				int x = ThreadLocalRandom.current().nextInt(1, 10001 - MAX_WIDTH); // bottom left x with constraints so rectangles stays in graph
				int y = ThreadLocalRandom.current().nextInt(1, 10001 - MAX_HEIGHT); // bottom left y with constraints so rectangle stays in graph
				int height = ThreadLocalRandom.current().nextInt(y, y + MAX_HEIGHT);
				int width = ThreadLocalRandom.current().nextInt(x, x + MAX_WIDTH);
				rowBuilder.append(String.valueOf(i)); // rectangle id
				rowBuilder.append(",");
				rowBuilder.append(String.valueOf(x));
				rowBuilder.append(",");
				rowBuilder.append(String.valueOf(y));
				rowBuilder.append(",");
				rowBuilder.append(String.valueOf(height));
				rowBuilder.append(",");
				rowBuilder.append(String.valueOf(width));
				rowBuilder.append("\n");
				writer.write(rowBuilder.toString()); // write row to csv
				rowBuilder.setLength(0); // reset row contents
			}
		} catch (FileNotFoundException e){
			System.out.println(e.getMessage());
		}
		
	}

}
