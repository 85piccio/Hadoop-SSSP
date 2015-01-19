package it.uniroma1.piccioli.tesi.sssp.spread;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperSpread extends Mapper<Text, Text, Text, Text> {


	public final static String SOURCE_INDEX = "SOURCE_INDEX";
	
	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		String[] sources = context.getConfiguration().getStrings(SOURCE_INDEX);
		String source = sources[0];

		if (key.toString().contains("-") && value.toString().contains("-")) {//valori gia inizializzati

			context.write(value, key);
			
		} else {//1st iteration - inizializzo valori in input

			Text k = new Text(key.toString() + "-" + Long.MAX_VALUE);// FORMATO: "id-valore"
			Text v = new Text(value.toString() + "-" + Long.MAX_VALUE);

//			if (Long.parseLong(key.toString()) == source) {
			if (key.toString().equals(source)) {
				k = new Text(key.toString() + "-0");
			}
//			if (Long.parseLong(value.toString()) == source) {
			if (value.toString().equals(source)) {
				v = new Text(value.toString() + "-0");
			}
			context.write(v, k);

		}

	}

}
