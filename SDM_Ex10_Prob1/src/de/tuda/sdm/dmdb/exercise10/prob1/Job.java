package de.tuda.sdm.dmdb.exercise10.prob1;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;

public class Job {
	public static void main(String[] args) throws IOException {
		run("visits_small.csv", "favorites_small.json");
	}

	static String DELIMETER = ";";

	public static void run(String visitDatasetFileName, String outFileName) throws IOException {
		Reader<String, String> reader = new MyReader();
		
		// read devices.csv
		System.out.println("Read devices data...");
		Context<String, String> devicesReaderContext = new Context<String, String>();
		String devicesData = "data/devices.csv";

		File devicesFile = new File(devicesData);
		reader.read(devicesFile, DELIMETER + "10", devicesReaderContext);
		System.out.println("Read devices data FINISHED.");
		
		// read visitData
		System.out.println("Read visit data...");
		String visitDataInput = "data/" + visitDatasetFileName;
		Context<String, String> visitDataReaderContext = new Context<String, String>();

		File visitDataFile = new File(visitDataInput);
		reader.read(visitDataFile, DELIMETER + "01", visitDataReaderContext);
		System.out.println("Read visit data FINISHED.");
		
		/*
		 * Perform mapping and reducing steps (as many as needed)
		 */
		 Context<String, Context<String, Integer>> mapper1Context = new Context<>();
		 Mapper<String, String, String, Context<String, Integer>> mapper1 = new MyMapper<>();
		 mapper1.run(devicesReaderContext, mapper1Context);
		 
		 mapper1Context.display();
		// Context<...> reducer1Context = new Context<>();
		// Reducer<...> reducer1 = new ...;
		// reducer1.run(mapper1Context, reducer1Context);

		// ...

		/*
		 * Write resulting records to file
		 */

		// RecordWriter<String, List<String>> writer = new ...;
		// writer.run(...);
	}
}
