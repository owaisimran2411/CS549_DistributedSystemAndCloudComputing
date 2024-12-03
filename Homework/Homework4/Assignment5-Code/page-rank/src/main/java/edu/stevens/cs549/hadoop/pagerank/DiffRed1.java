package edu.stevens.cs549.hadoop.pagerank;

import java.io.*;
import java.util.Iterator;

import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.io.*;

public class DiffRed1 extends Reducer<Text, Text, Text, Text> {

	public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		double[] ranks = new double[2];
		/* 
		 * ---- TODO: The list of values should contain two ranks.  Compute and output their difference.
		 */
		Iterator<Text> iterator = values.iterator();

		if(iterator.hasNext()) {
			ranks[0] = Double.parseDouble(iterator.next().toString());
		}
		if(iterator.hasNext()) {
			ranks[1] = Double.parseDouble(iterator.next().toString());
		}
		double difference = Math.abs(ranks[0] - ranks[1]);
//		System.out.println(String.format("%s %f", key.toString(), difference));

		context.write(key, new Text(String.valueOf(difference)));

	}
}
