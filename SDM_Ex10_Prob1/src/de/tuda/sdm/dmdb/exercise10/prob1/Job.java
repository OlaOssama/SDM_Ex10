package de.tuda.sdm.dmdb.exercise10.prob1;

import java.io.BufferedReader;
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
		// read visitData
		String visitDataInput = "data/" + visitDatasetFileName;
		Context<String, String> visitDataReaderContext = new Context<String, String>();

		File visitDataFile = new File(visitDataInput);

		Reader<String, String> reader = new Reader<>();
		reader.read(visitDataFile, DELIMETER+"01", visitDataReaderContext);

		// read devices.csv
		Context<String, String> devicesReaderContext = new Context<String, String>();
		String devicesData = "data/devices.csv";

		File devicesFile = new File(devicesData);
		reader.read(devicesFile, DELIMETER+"10", devicesReaderContext);
		
		/*
		 * Perform mapping and reducing steps (as many as needed)
		 */

		// Context<...> mapper1Context = new Context<>();
		// Mapper<...> mapper1 = new ...;
		// mapper1.run(readerContext, mapper1Context);

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
