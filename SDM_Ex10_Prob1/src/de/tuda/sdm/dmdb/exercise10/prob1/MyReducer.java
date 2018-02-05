package de.tuda.sdm.dmdb.exercise10.prob1;

public class MyReducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	@Override
	public void reduce(KEYIN key, Iterable<VALUEIN> values, Context<KEYOUT, VALUEOUT> context) {
		// count appearance of each instance in value
		Context<KEYIN, Integer> innerContext = new Context<>(); // write to a new inner context
		for (VALUEIN v : values) {
			if (!innerContext.keyValues.containsKey(v)) {
				
			}
		}
		context.write((KEYOUT) key, (VALUEOUT)innerContext);
	}
}
