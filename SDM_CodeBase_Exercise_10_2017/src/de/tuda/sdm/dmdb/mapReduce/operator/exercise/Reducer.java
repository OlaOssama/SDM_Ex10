package de.tuda.sdm.dmdb.mapReduce.operator.exercise;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;

import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.SQLIntegerBase;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

/**
 * similar to https://github.com/apache/hadoop/blob/trunk/hadoop-mapreduce-project/hadoop-mapreduce-client/hadoop-mapreduce-client-core/src/main/java/org/apache/hadoop/mapreduce/Reducer.java
 * 
 * Reduces a set of intermediate values which share a key to a smaller set of
 * values.  
 * 
 * A Reducer performs three primary tasks:
 * 	1) Read sorted input from child
 *  2) Group input from child (ie., prepare input to reduce function)
 *  3) Invoke the reduce() method on the prepared input to generate new output pairs
 * 
 * <p>The output of the <code>Reducer</code> is <b>not re-sorted</b>.</p>
 * 
 * 
 * @author melhindi
 *
 * @param <KEYIN> SQLValue type of the input key
 * @param <VALUEIN> SQLValue type of the input value
 * @param <KEYOUT> SQLValue type of the output key
 * @param <VALUEOUT> SQLValue type of the output value
 */
public class Reducer<KEYIN extends AbstractSQLValue, VALUEIN extends AbstractSQLValue, KEYOUT extends AbstractSQLValue, VALUEOUT extends AbstractSQLValue> extends ReducerBase<KEYIN, VALUEIN, KEYOUT, VALUEOUT>{
	
	@Override
	public void open() {
		child.open();
		lastRecord = null;
		nextList = new LinkedList<AbstractRecord>();
		// TODO: implement this method
		// make sure to initialize ALL (inherited) member variables

	}
//	protected void reduce(KEYIN key, Iterable<VALUEIN> values, Queue<AbstractRecord> nextList) {
//		AbstractRecord newRecord = MapReduceOperator.keyValueRecordPrototype.clone();
//		newRecord.setValue(MapReduceOperator.KEY_COLUMN, (KEYOUT) key);
//		SQLInteger res = new SQLInteger();
//		int counter = 0;
//		for (VALUEIN value: values) {
//			sum all values , put in res
	//		}
//		} 
	@Override
	@SuppressWarnings("unchecked")
	public AbstractRecord next() {
		if(lastRecord == null){
		lastRecord = child.next();
		if(lastRecord == null){
			return null;
		}
		}
		List<AbstractSQLValue> it = new LinkedList<AbstractSQLValue>();
		it.add(lastRecord.getValue(VALUE_COLUMN));
		System.out.println("a "+it);
		AbstractRecord rec;
		while ((rec = child.next()) != null) {
			if(rec.getValue(KEY_COLUMN).equals(lastRecord.getValue(KEY_COLUMN))){
				it.add(rec.getValue(VALUE_COLUMN));
				System.out.println("b "+it);
			}
			else{
				lastRecord = rec;
				break;
			}
		}
		System.out.println("c "+it);
		super.reduce((KEYIN)lastRecord.getValue(KEY_COLUMN), (Iterable<VALUEIN>)it , nextList);
		
		if(rec==null){
			System.out.println("null "+nextList);
			return null;
		}
		System.out.println("nextlist "+nextList);
		return nextList.poll();
		
//		while(true){
//			System.out.println("111");
//			AbstractRecord rec = child.next();
//			System.out.println(rec==null);
//			if(rec==null){
//				super.reduce((KEYIN)lastRecord.getValue(KEY_COLUMN), (Iterable<VALUEIN>)it , nextList);
//				System.out.println("aefsef");
//				return null;
//			}
//			if (lastRecord.getValue(KEY_COLUMN).equals(rec.getValue(KEY_COLUMN))) {
//				System.out.println("22");
//				it.add(rec.getValue(VALUE_COLUMN));
//				System.out.println("it1 :"+it);
//			}
//			else{
//				System.out.println("3333");
//				lastRecord = rec;
//				super.reduce((KEYIN)lastRecord.getValue(KEY_COLUMN), (Iterable<VALUEIN>)it , nextList);
//				System.out.println("it "+it);
//				return nextList.poll();
//			}
//			
//			
//		}
//		System.out.println("it: "+it);
//		System.out.println("recasdfcsdf"+rec);
//		System.out.println("nextlist:"+nextList);
//		return nextList.poll();

		// TODO: implement this method
		// this method has to prepare the input to the reduce function
		
		// it also returns the result of the reduce function as next
		// Implement the grouping of mapper-outputs here 
		// You can assume that the input to the reducer is sorted. This makes the grouping operation easier. Keep this in mind if you write your own tests (make sure that input to reducer is sorted)

		// retrieve next input record

		// prepare input for the reduce function (group by)

		// invoke the reduce function on the input and pass in this.nextList to cache the output pairs there

		
	}

	@Override
	public void close() {
		child.close();
		nextList = null;
		// TODO: implement this method
		// reverse what was done in open()

	}

}
