package it.uniroma1.piccioli.test.sssp;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;


public class ReducerInit extends Reducer<Text, Text, Text, Text> {


	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {

		String[] kTmp = key.toString().split("-");

		long distMin = Long.valueOf(kTmp[1]);

		ArrayList<Text> cache1 = new ArrayList<>();//clone per secondo for su values

		for (Text u : values) {

			String[] uTmp = u.toString().split("-");// FORMATO: "id-valore"

			if (Long.valueOf(uTmp[1]) < distMin) {
				distMin = Long.valueOf(uTmp[1]) + 1;
				context.getCounter(STATE.UPDATED).increment(1);//controllo su stato degli update 
			}

			cache1.add(new Text(uTmp[0] + "-" + uTmp[1]));//clono valori

		}

		for (Text u : cache1) {
			context.write(u, new Text(kTmp[0] + "-" + distMin));
		}

	}

}