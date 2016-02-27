/*
Author Name: Sumanth Pikkili
UTA ID: 1001100941
CSE 6331 - 002
Assignment 4: Introduction to Data Science using Map-Reduce (Hadoop) and ML
 */

package cloudassignment4_ML;

import java.io.IOException;
import java.util.Iterator;
//import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;

//Reducer Class
public class AvgReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{
	//Declaring array lists to store the averages of temperature, precipitation and wind
	public static ArrayList<Integer> avgarray = new ArrayList<Integer>();
	public static ArrayList<Integer> avgprecparray = new ArrayList<Integer>();
	public static ArrayList<Integer> avgwindarray = new ArrayList<Integer>();
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		int sumTemp = 0;
		int sumprecp = 0;
		int sumwind = 0;
		int numItems = 0;
		//Fetching values and calculating averages
		while (values.hasNext()){
			String nextValue=values.next().toString();
			String Cli[] = nextValue.split(",");
			int currenttemp = Integer.parseInt(Cli[0]);
			int currprecp = Integer.parseInt(Cli[1]);
			int currwind = Integer.parseInt(Cli[2]);
			sumTemp +=currenttemp;
			sumprecp+=currprecp;
			sumwind+=currwind;
			numItems += 1;
		}
		int avgtemp = sumTemp/numItems;
		int avgprecp = sumprecp/numItems;
		int avgwind = sumwind/numItems;

	
		String result = "," + avgtemp + "," + avgprecp + "," + avgwind;

		output.collect(key, new Text(result)); 

	}
}