/*

Author Name: Sumanth Pikkili
UTA ID: 1001100941
CSE 6331 - 002
Assignment 4: Introduction to Data Science using Map-Reduce (Hadoop) and ML

*/

package cloudassignment4_seasonly;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.commons.lang.StringUtils;

//Mapper Class
public class AvgMapper extends MapReduceBase implements Mapper<LongWritable, Text, Text, Text>{
	@Override
	public void map(LongWritable key, Text value,
			OutputCollector<Text, Text> output, Reporter reporter)
					throws IOException {

		String[] line = value.toString().split(","); // fetching a record and splitting by ","
		String dataPart = line[2]; // Fetching Date - Second Column
		String avgtemp = line[3]; // Fetching Temperature - Third Column
		String precp = line[6]; //Fetching Precipitation - Sixth Column
		String wind = line[13]; //Fetching Wind - Thirteenth Column
		String data = "";

		String year  = dataPart.substring(0,4); //Fetching the year part of the date
		String month = dataPart.substring(4,6); //Fetching the month part of the date

		// Validating which season it is
		if(month.equals("03") || month.equals("04") || month.equals("05"))
		{
			data="SpringSeason";

		}

		else if(month.equals("06") || month.equals("07") || month.equals("08"))
		{
			data="SummerSeason";

		}

		else if(month.equals("09") || month.equals("10") || month.equals("11"))
		{
			data="FallSeason";

		}

		else if(month.equals("12") || month.equals("01") || month.equals("02"))
		{
			data="WinterSeason";

		}

		if(StringUtils.isNumeric(avgtemp)){
			//Passing the year and season as keys
			output.collect(new Text(year + data),new Text(avgtemp + "," + precp + "," + wind));
		}

	}
}
