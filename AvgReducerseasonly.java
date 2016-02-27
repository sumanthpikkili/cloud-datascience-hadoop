/*

Author Name: Sumanth Pikkili
UTA ID: 1001100941
CSE 6331 - 002
Assignment 4: Introduction to Data Science using Map-Reduce (Hadoop) and ML

*/

package cloudassignment4_seasonly;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import java.util.ArrayList;


//Reducer Class
public class AvgReducer extends MapReduceBase implements Reducer<Text, Text, Text, Text>{

	//Declaring array lists to store the averages of temperature, wind and precipitation
	public static ArrayList<Integer> avgarray = new ArrayList<Integer>();
	public static ArrayList<Integer> avgprecparray = new ArrayList<Integer>();
	public static ArrayList<Integer> avgwindarray = new ArrayList<Integer>();
	@Override
	public void reduce(Text key, Iterator<Text> values,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {
		String compTempRes="";
		String compPrecp="";
		String compWind="";
		int sumTemp = 0;
		int sumprecp = 0;
		int sumwind = 0;
		int numItems = 0;

		//Fetching the values and finding the averages 
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

		avgarray.add(avgtemp);
		avgprecparray.add(avgprecp);
		avgwindarray.add(avgwind);
		int sizetemp = avgarray.size();

		//Comparing the averages of a season with the previous season
		if (sizetemp > 1) {
			int compInt = avgarray.get(sizetemp - 2);
			if(compInt < avgtemp) {
				compTempRes = "Getting Warmer than Last year season";
			} else if(compInt > avgtemp) {
				compTempRes = "Getting Colder than Last year season";
			} else if(compInt == avgtemp) {
				compTempRes = "Same Result as Last year season";
			}
		}

		if (sizetemp > 1) {
			int compInt0 = avgprecparray.get(sizetemp - 2);
			if(compInt0 < avgprecp) {
				compPrecp = "Getting Wetter than Last year season";
			} else if(compInt0 > avgprecp) {
				compPrecp = "Getting Dryer than Last year season";
			} else if(compInt0 == avgprecp) {
				compPrecp = "Same Result as Last year season";
			}
		}

		if (sizetemp > 1) {
			int compInt1 = avgwindarray.get(sizetemp - 2);
			if(compInt1 > avgwind) {
				compWind = "Getting more windy than Last year season";
			} else if(compInt1 < avgwind) {
				compWind = "Getting less windy than Last year season";
			} else if(compInt1 == avgwind) {
				compWind = "Same Result as Last Year season";
			}
		}

		//Outputting the results obtained
		String result = " ==== " + String.valueOf(avgtemp) + "DegF -" + compTempRes + " ==== " + 
				" ==== " + String.valueOf(avgprecp) + "mm -" + compPrecp + 
				" ==== " + String.valueOf(avgwind) + "miles/hr - " + compWind;

		output.collect(key, new Text(result)); 

	}
}