package it.uniroma1.piccioli.tesi.sssp.out;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ReducerOut extends Reducer<Text, Text, Text, Text> {
//	private MultipleOutputs<Text, Text> mos;

//	@Override
//	protected void setup(Context context) throws IOException, InterruptedException {
//		mos = new MultipleOutputs<Text, Text>(context);
//	}

	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

		context.write(key, new Text(""));


	}

}
