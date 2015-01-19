package it.uniroma1.piccioli.tesi.sssp.out;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperOut extends Mapper<Text, Text, Text, Text> {
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		context.write(key, new Text(""));

	}
}
