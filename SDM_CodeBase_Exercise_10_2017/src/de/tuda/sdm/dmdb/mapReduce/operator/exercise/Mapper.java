package de.tuda.sdm.dmdb.mapReduce.operator.exercise;

import java.util.LinkedList;
import java.util.Queue;

import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.mapReduce.operator.MapperBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

/**
 * similar to:
 * https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Mapper.java
 * 
 * Maps input key/value pairs to a set of intermediate key/value pairs.
 * 
 * Maps are the individual tasks which transform input records into a
 * intermediate records. A given input pair may map to zero or many output
 * pairs. Output pairs are normally written to the nextList member
 * 
 * The class implements the iterator model and works on records. Records are
 * read to retrieve the KEY and VALUE, which are passed to the map() function
 * The map function performs the defined mapping and writes the new output to
 * the passed in record List Use the memeber nextList as input to the map()
 * function to cache produced output accross multiple next() calls.
 * 
 * @author melhindi
 *
 * @param <KEYIN>
 *            SQLValue type of the input key
 * @param <VALUEIN>
 *            SQLValue type of the input value
 * @param <KEYOUT>
 *            SQLValue type of the output key
 * @param <VALUEOUT>
 *            SQLValue type of the output value
 */
public class Mapper<KEYIN extends AbstractSQLValue, VALUEIN extends AbstractSQLValue, KEYOUT extends AbstractSQLValue, VALUEOUT extends AbstractSQLValue>
		extends MapperBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

	@Override
	public void open() {
		// TODO: implement this method
		// make sure to initialize ALL (inherited) member variables
		child.open();
		nextList = new LinkedList<AbstractRecord>();
	}

	@Override
	protected void map(KEYIN key, VALUEIN value, Queue<AbstractRecord> outList) {
		String[] str = value.toString().split("\\s+");
		for (String string : str) {
			AbstractRecord record = MapReduceOperator.keyValueRecordPrototype.clone();
			record.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar(string, 255));
			record.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
			outList.offer(record);
		}
	}

	@Override
	@SuppressWarnings("unchecked")
	public AbstractRecord next() {
		// TODO: implement this method
		// this method has to retrieve and prepare the input to the map function
		// it also returns the result of the map function as next
		// keep in mind that a mapper potentially maps one AbstractRecord to multiple
		// other AbstractRecords

		// get next input

		// prepare input

		// invoke the map function on the input and pass in this.nextList to cache the
		// output pairs there
		AbstractRecord rec = child.next();
		if (rec != null) {
			Queue<AbstractRecord> interResult = new LinkedList<AbstractRecord>();
			this.map((KEYIN) rec.getValue(KEY_COLUMN), (VALUEIN) rec.getValue(VALUE_COLUMN), interResult);
			nextList.addAll(interResult);
		}
		return nextList.poll();
	}

	@Override
	public void close() {
		// TODO: implement this method
		// reverse what was done in open()
		child.close();
		nextList = null;
	}

}
