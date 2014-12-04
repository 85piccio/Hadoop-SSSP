package it.uniroma1.piccioli.tesi.sssp;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSync extends Mapper<Text, Text, Text, Text> {
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String[] kTmp = key.toString().split("-");// FORMATO: "id-valore"
		String[] vTmp = value.toString().split("-");

		context.write(new Text(kTmp[0]), new Text("E-" + value.toString() + "-" + kTmp[1]));
		context.write(new Text(vTmp[0]), new Text("S-" + key.toString() + "-" + vTmp[1]));

	}
}
