package de.tuda.sdm.dmdb.exercise10.prob1;

public class MyMapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> extends Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	@Override
	public void map(KEYIN key, VALUEIN value, Context<KEYOUT, VALUEOUT> context) {
		Context<KEYOUT, Integer> innerContext = new Context<>();
		innerContext.write((KEYOUT) value, 1);
		context.write((KEYOUT) key, (VALUEOUT) innerContext);
	}
}
