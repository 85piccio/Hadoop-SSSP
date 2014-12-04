package it.uniroma1.piccioli.tesi.sssp.job1;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MapperInit extends Mapper<Text, Text, Text, Text> {

	@Override
	protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {

		long source = context.getConfiguration().getLong("SOURCE_INDEX", 0);

		if (key.toString().contains("-") && value.toString().contains("-")) {//valori gia inizializzati

			context.write(value, key);

		} else {//1st iteration - inizializzo valori input

			Text k = new Text(key.toString() + "-" + Long.MAX_VALUE);// FORMATO: "id-valore"
			Text v = new Text(value.toString() + "-" + Long.MAX_VALUE);

			if (Long.parseLong(key.toString()) == source) {
				k = new Text(key.toString() + "-0");
			}
			if (Long.parseLong(value.toString()) == source) {
				v = new Text(value.toString() + "-0");
			}
			context.write(v, k);

		}

	}

}
