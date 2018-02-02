package de.tuda.sdm.dmdb.exercise10.prob1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Reader<KEYIN, VALUEIN> {
    /**
     *
     * @param input
     * @param key
     * @param context
     */
    public void read(File input, KEYIN key, Context<KEYIN, VALUEIN> context) throws IOException{
    	 String line = "";
         String cvsSplitBy = (String) key;
         try (BufferedReader br = new BufferedReader(new FileReader(input))) {
             while ((line = br.readLine()) != null) {
                 String[] res = line.split(cvsSplitBy);
                 context.write((KEYIN) res[0], (VALUEIN) res[1]);
             }
         } catch (IOException e) {
             e.printStackTrace();
         }
    	
    }
}
