package de.tuda.sdm.dmdb.exercise10.prob1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class MyReader extends Reader {

	@Override
	public void read(File input, Object key, Context context) throws IOException {
		String line = "";
		String inStr = key.toString();
		String cvsSplitBy = Character.toString(inStr.charAt(0));
		int keyColumn = Character.getNumericValue(inStr.charAt(1));
		int valueColumn = Character.getNumericValue(inStr.charAt(2));
		try (BufferedReader br = new BufferedReader(new FileReader(input))) {
			boolean firstLineRead = false;

			while ((line = br.readLine()) != null) {
				if (!firstLineRead) {	// avoid first line
					firstLineRead = true;
					continue;
				}
				String[] res = line.split(cvsSplitBy);
				context.write(res[keyColumn], res[valueColumn]);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

	}
}
