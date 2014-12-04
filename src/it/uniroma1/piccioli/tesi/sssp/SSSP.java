package it.uniroma1.piccioli.tesi.sssp;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import it.uniroma1.piccioli.tesi.sssp.STATE;

public class SSSP extends Configured implements Tool {
	public final static String SOURCE_INDEX = "KEEP_ABOVE";
	public final static String VEDIT = "VEDIT";

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		long source = 0;
		if (args.length > 2) {
			source = Long.parseLong(args[2]);
		}
		conf.setLong(SOURCE_INDEX, source);
		conf.setBoolean(VEDIT, false);

		// path job1
		Path in1 = new Path(args[0]);
		Path outTmp = new Path("/result_job");
		Path outSync = new Path("/result_job_sync");
		Path outSync2 = new Path("/result_job_sync2");

		// per entrare
		boolean hasUpdates = true;
		boolean justOne = true;
//		int maxIter = 10;
		long prev = 1;
		while (hasUpdates /*&& (maxIter > 1)*/) {

			//reset cartelle di output
			FileSystem fs = FileSystem.get(conf);			
			fs.delete(outTmp, true);
			fs.delete(outSync, true);

			/*
			 * Primo Job 
			 * Input Lista di Archi nella forma "Sorgente Destinazione"
			 * Mapper - (Sorgente,Destinazione) -> emit (Destinazione,Sorgene) 
			 * Reducer - (v,[k,k,k,...] -> Calcola nodo sorgente con distanza minima e, per ogni k, emit(k,v) con k.val = distMin)
			 * */
			Job jobInit = Job.getInstance(conf);

			jobInit.setJobName("SSCP-step1");

			jobInit.setMapperClass(MapperInit.class);
			jobInit.setReducerClass(ReducerInit.class);

			jobInit.setJarByClass(SSSP.class);

			jobInit.setInputFormatClass(KeyValueTextInputFormat.class);
			jobInit.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(jobInit, in1);
			FileOutputFormat.setOutputPath(jobInit, outTmp);

			jobInit.setMapOutputKeyClass(Text.class);
			jobInit.setMapOutputValueClass(Text.class);

			jobInit.waitForCompletion(true);

			//Contatore degli update della variabile distMin nel reducer 
			long updates = jobInit.getCounters().findCounter(STATE.UPDATED).getValue(); 

			/*
			 * Secondo Job 
			 * Input Lista di Archi nella forma "Sorgente Destinazione"
			 * Mapper - (Sorgente,Destinazione) -> emit (Destinazione,Sorgene) & emit (Sorgente,Destinazione)
			 * Reducer - (v,[k,k,k,...] -> Per ogni nodo calcola il valore minDist in modo da sincronizzare i valori dei nodi sorgenti in input)
			 * */

			Job jobSync = Job.getInstance(conf);
			jobSync.setJobName("SSSP-sync");


			jobSync.setJarByClass(SSSP.class);

			FileInputFormat.addInputPath(jobSync, outTmp);
			jobSync.setInputFormatClass(KeyValueTextInputFormat.class);

			jobSync.setMapperClass(MapperSync.class);
			jobSync.setReducerClass(ReducerSync.class);

			jobSync.setOutputKeyClass(Text.class);
			jobSync.setOutputValueClass(Text.class);

			FileOutputFormat.setOutputPath(jobSync, outSync);
			jobSync.setOutputFormatClass(TextOutputFormat.class);

			jobSync.waitForCompletion(true);

			//Inverto path input con path output per iterazione successiva
			if(justOne){
				in1 =outSync2;//preserva input originale 
				justOne=false;
			}
			Path a = outSync;
			outSync = in1;
			in1 = a;
			
            // Verifico che ci sono stati update altrimeni termino iterazioni
            long diffPrev = updates - prev;
            hasUpdates = (diffPrev != 0);
            prev = updates;
            

		}

		//TODO: presentazione risultati
//		FileSystem fs = FileSystem.get(conf);
//		fs.mo.moveFromLocalFile(in1, new Path(args[0]));
		return 0;

	}
//
//	private void swap(Path p1, Path p2) {
//		Path a;
//		a = p1;
//		p1 = p2;
//		p2 = p1;
//
//	}

	public static void main(String[] args) throws Exception {
		if (args.length < 2) {
			System.exit(printUsage());
		}

		Configuration conf = new Configuration();

		conf.set("mapreduce.input.keyvaluelinerecordreader.key.value.separator", " ");
		conf.set("mapreduce.output.textoutputformat.separator", " ");

		SSSP dc = new SSSP();
		dc.setConf(conf);
		int res = ToolRunner.run(dc, args);

		System.exit(res);

	}

	static int printUsage() {
		System.out.println("DegreeCalculator <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

}