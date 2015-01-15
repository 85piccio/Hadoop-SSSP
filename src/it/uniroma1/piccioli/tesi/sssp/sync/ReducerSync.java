package it.uniroma1.piccioli.tesi.sssp.sync;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerSync extends Reducer<Text, Text, Text, Text> {
//	private MultipleOutputs<Text, Text> mos;

//	@Override
//	protected void setup(Context context) throws IOException, InterruptedException {
//		mos = new MultipleOutputs<Text, Text>(context);
//	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		ArrayList<Text> cache1 = new ArrayList<>();


		long distMin = Long.MAX_VALUE;
		for (Text u : values) {
			String[] tmp = u.toString().split("-");// FORMATO: "id-valore"

			long min = Long.parseLong(tmp[3]);
			if (min < distMin)
				distMin = min;

			cache1.add(new Text(u.toString()));
		}

		for (Text u : cache1) {
			String[] tmp = u.toString().split("-");
			if (tmp[0].equals("E"))
				context.write(new Text(key.toString() + "-" + distMin), new Text(tmp[1] + "-" + tmp[2]));
		}



	}

}
