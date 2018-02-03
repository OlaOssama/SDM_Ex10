package de.tuda.sdm.dmdb.test.mapReduce.operator;

import java.util.ArrayList;
import java.util.List;
import org.junit.Assert;

import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.mapReduce.operator.exercise.Reducer;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;
import de.tuda.sdm.dmdb.test.TestCase;

public class TestReducer extends TestCase{
	

//	public void testReducerIdentity() {
//
//		List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
//		List<AbstractRecord> actualResult = new ArrayList<AbstractRecord>();
//
//		AbstractRecord record = MapReduceOperator.keyValueRecordPrototype.clone();
//		record.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("the", 100));
//		record.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
//		
//		AbstractRecord expectedRecord = record.clone();
//		expectedResult.add(expectedRecord);
//		expectedResult.add(expectedRecord);
//
//		HeapTable table1 = new HeapTable(record);
//		table1.insert(record);
//		table1.insert(record);
//
//		TableScan ts = new TableScan(table1);
//		Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> reducer = new Reducer<>();
//		reducer.setChild(ts);
//		reducer.open();
//		AbstractRecord next;
//		while ((next = reducer.next()) != null) {
//			actualResult.add(next);
//		}
//		reducer.close();
//		System.out.println("actual result: "+actualResult);
//		Assert.assertEquals(expectedResult.size(), actualResult.size());
//		
//		for (int i = 0; i < actualResult.size(); i++) {
//			AbstractRecord value1 = expectedResult.get(i);
//			AbstractRecord value2 = actualResult.get(i);
//			Assert.assertEquals(value1, value2);
//		}
//	}
	public void test2(){

		List<AbstractRecord> expectedResult = new ArrayList<AbstractRecord>();
		List<AbstractRecord> actualResult = new ArrayList<AbstractRecord>();

		AbstractRecord record1 = MapReduceOperator.keyValueRecordPrototype.clone();
		record1.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("bla", 100));
		record1.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
		AbstractRecord record2 = MapReduceOperator.keyValueRecordPrototype.clone();
		record2.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("fox", 100));
		record2.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
		AbstractRecord record3 = MapReduceOperator.keyValueRecordPrototype.clone();
		record3.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("the", 100));
		record3.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
		
		HeapTable table1 = new HeapTable(record1);
		table1.insert(record1);
		table1.insert(record1);
		table1.insert(record1);
		table1.insert(record2);
		table1.insert(record3);
		table1.insert(record3);
		
		AbstractRecord expectedRecord1 = MapReduceOperator.keyValueRecordPrototype.clone();
		expectedRecord1.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("bla", 100));
		expectedRecord1.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(3));
		AbstractRecord expectedRecord2 = MapReduceOperator.keyValueRecordPrototype.clone();
		expectedRecord2.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("fox", 100));
		expectedRecord2.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(1));
		AbstractRecord expectedRecord3 = MapReduceOperator.keyValueRecordPrototype.clone();
		expectedRecord3.setValue(MapReduceOperator.KEY_COLUMN, new SQLVarchar("the", 100));
		expectedRecord3.setValue(MapReduceOperator.VALUE_COLUMN, new SQLInteger(2));
		expectedResult.add(expectedRecord1);
		expectedResult.add(expectedRecord2);
		expectedResult.add(expectedRecord3);
		
		TableScan ts = new TableScan(table1);
		Reducer<SQLVarchar, SQLInteger, SQLVarchar, SQLInteger> reducer = new Reducer<>();
		reducer.setChild(ts);
		reducer.open();
		AbstractRecord next;
		while ((next = reducer.next()) != null) {
			actualResult.add(next);
		}
		reducer.close();
		System.out.println("actual result: "+actualResult);
		System.out.println("expectedresult: "+expectedResult);
		Assert.assertEquals(expectedResult.size(), actualResult.size());
		
		for (int i = 0; i < actualResult.size(); i++) {
			AbstractRecord value1 = expectedResult.get(i);
			AbstractRecord value2 = actualResult.get(i);
			Assert.assertEquals(value1, value2);
		}
		
	}
	
}
