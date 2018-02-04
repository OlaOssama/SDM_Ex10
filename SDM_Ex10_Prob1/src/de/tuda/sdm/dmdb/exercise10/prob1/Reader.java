package de.tuda.sdm.dmdb.exercise10.prob1;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;

public class Reader<KEYIN, VALUEIN> {
    /**
     *
     * @param input
     * @param key delimiter + keyCol + valueColumn
     * @param context
     */
    public void read(File input, KEYIN key, Context<KEYIN, VALUEIN> context) throws IOException{
    	 String line = "";
         String inStr = key.toString();
         String cvsSplitBy = Character.toString(inStr.charAt(0));
         int keyColumn = inStr.charAt(1);
         int valueColumn = inStr.charAt(2);
         try (BufferedReader br = new BufferedReader(new FileReader(input))) {
             while ((line = br.readLine()) != null) {
                 String[] res = line.split(cvsSplitBy);
                 context.write((KEYIN) res[keyColumn], (VALUEIN) res[valueColumn]);
             }
         } catch (IOException e) {
             e.printStackTrace();
         }
    }
}
