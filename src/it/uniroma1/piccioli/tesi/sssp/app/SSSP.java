package it.uniroma1.piccioli.tesi.sssp.app;

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

import it.uniroma1.piccioli.tesi.sssp.out.MapperOut;
import it.uniroma1.piccioli.tesi.sssp.out.ReducerOut;
import it.uniroma1.piccioli.tesi.sssp.spread.MapperSpread;
import it.uniroma1.piccioli.tesi.sssp.spread.ReducerSpread;
import it.uniroma1.piccioli.tesi.sssp.sync.MapperSync;
import it.uniroma1.piccioli.tesi.sssp.sync.ReducerSync;

public class SSSP extends Configured implements Tool {
	public final static String SOURCE_INDEX = "SOURCE_INDEX";
	public final static String VEDIT = "VEDIT";

	public int run(String[] args) throws Exception {

		Configuration conf = this.getConf();
		String source = "0";
		int maxIter = Integer.MAX_VALUE;//limito risorse
		if (args.length > 2) {
//			source = Long.parseLong(args[2]);
			source = args[2];
			if(args.length > 3){
				maxIter = Integer.parseInt(args[3]);
			}
		}
		//conf.setLong(SOURCE_INDEX, source);
		conf.setStrings(SOURCE_INDEX, source);
		conf.setBoolean(VEDIT, false);

		// path job1
		Path inputIniziale = new Path(args[0]);
		Path outputFinale = new Path(args[1]);
		Path path1 = new Path("/result_job_sync");
		Path path2 = new Path("/result_job_sync2");

		// per entrare
		boolean hasUpdates = true;
		boolean justOne = true;
		
		long prev = 1;
		int nn = 0;
		while (hasUpdates  && (maxIter > 0)) {

			Path outtmp = new Path("/result_outtmp");

			// reset cartelle di output
			FileSystem fs = FileSystem.get(conf);
			fs.delete(outtmp, true);
			fs.delete(path1, true);

			/*
			 * Primo Job Input Lista di Archi nella forma
			 * "Sorgente Destinazione" Mapper - (Sorgente,Destinazione) -> emit
			 * (Destinazione,Sorgene) Reducer - (v,[k,k,k,...] -> Calcola nodo
			 * sorgente con distanza minima e, per ogni k, emit(k,v) con k.val =
			 * distMin)
			 */
			Job jobInit = Job.getInstance(conf);

			jobInit.setJobName("SSCP-step-" + nn);

			jobInit.setMapperClass(MapperSpread.class);
			jobInit.setReducerClass(ReducerSpread.class);

			jobInit.setJarByClass(SSSP.class);

			jobInit.setInputFormatClass(KeyValueTextInputFormat.class);
			jobInit.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.addInputPath(jobInit, inputIniziale);
			FileOutputFormat.setOutputPath(jobInit, outtmp);

			jobInit.setMapOutputKeyClass(Text.class);
			jobInit.setMapOutputValueClass(Text.class);

			jobInit.waitForCompletion(true);

			// Contatore degli update della variabile distMin nel reducer
			long updates = jobInit.getCounters().findCounter(STATE.UPDATED).getValue();

			/*
			 * Secondo Job Input Lista di Archi nella forma
			 * "Sorgente Destinazione" Mapper - (Sorgente,Destinazione) -> emit
			 * (Destinazione,Sorgene) & emit (Sorgente,Destinazione) Reducer -
			 * (v,[k,k,k,...] -> Per ogni nodo calcola il valore minDist in modo
			 * da sincronizzare i valori dei nodi sorgenti in input)
			 */

			Job jobSync = Job.getInstance(conf);
			jobSync.setJobName("SSSP-sync-" + nn);

			jobSync.setJarByClass(SSSP.class);

			FileInputFormat.addInputPath(jobSync, outtmp);
			jobSync.setInputFormatClass(KeyValueTextInputFormat.class);

			jobSync.setMapperClass(MapperSync.class);
			jobSync.setReducerClass(ReducerSync.class);

			jobSync.setOutputKeyClass(Text.class);
			jobSync.setOutputValueClass(Text.class);

			FileOutputFormat.setOutputPath(jobSync, path1);
			jobSync.setOutputFormatClass(TextOutputFormat.class);

			jobSync.waitForCompletion(true);

			// Inverto path input con path output per iterazione successiva
			if (justOne) {
				inputIniziale = path2;// per preservare input originale
				justOne = false;
			}
			Path tmpSwitch = path1;
			path1 = inputIniziale;
			inputIniziale = tmpSwitch;

			// Verifico che ci sono stati update altrimeni termino iterazioni
			long diffPrev = updates - prev;
			hasUpdates = (diffPrev != 0);
			prev = updates;

			System.out.println("Round numero:" + nn);
			nn++;
			maxIter--;

		}

		Job jobOut = Job.getInstance(conf);

		jobOut.setJobName("SSSP-out");

		jobOut.setJarByClass(SSSP.class);

		FileInputFormat.addInputPath(jobOut, inputIniziale);
		jobOut.setInputFormatClass(KeyValueTextInputFormat.class);

		jobOut.setMapperClass(MapperOut.class);
		jobOut.setReducerClass(ReducerOut.class);

		jobOut.setOutputKeyClass(Text.class);
		jobOut.setOutputValueClass(Text.class);

		FileOutputFormat.setOutputPath(jobOut, outputFinale);
		jobOut.setOutputFormatClass(TextOutputFormat.class);

		jobOut.waitForCompletion(true);
		

		// pulisco cartelle temporanee
		FileSystem fs = FileSystem.get(conf);
		fs.delete(path2, true);
		fs.delete(path1, true);

		return 0;

	}

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
		System.out.println("DegreeCalculator <input> <output> [<Source Index>]");
		ToolRunner.printGenericCommandUsage(System.out);
		return -1;
	}

}