package de.tuda.sdm.dmdb.mapReduce.task.exercise;

import static org.junit.Assume.assumeThat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import de.tuda.sdm.dmdb.access.exercise.HeapTable;
import de.tuda.sdm.dmdb.mapReduce.operator.MapperBase;
import de.tuda.sdm.dmdb.mapReduce.operator.ReducerBase;
import de.tuda.sdm.dmdb.mapReduce.operator.exercise.Mapper;
import de.tuda.sdm.dmdb.mapReduce.task.SinglePhaseTaskBase;
import de.tuda.sdm.dmdb.sql.operator.Shuffle;
import de.tuda.sdm.dmdb.sql.operator.exercise.Sort;
import de.tuda.sdm.dmdb.sql.operator.exercise.TableScan;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.types.AbstractSQLValue;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLInteger;
import de.tuda.sdm.dmdb.storage.types.exercise.SQLVarchar;

/**
 * Defines what happens during the execution a map-reduce task Ie. implements
 * the operator chains for a complete map-reduce task We assume the same number
 * of mappers and reducers (no need to change hashFunction of shuffle) The
 * Operator chain that this task implements is:
 * Scan->Mapper->Shuffle->Sort->Reducer The last operator in the chain writes to
 * the output, ie. is used to populate the output
 * 
 * @author melhindi
 *
 */
public class SinglePhaseTask extends SinglePhaseTaskBase {

	public SinglePhaseTask(HeapTable input, HeapTable output, int nodeId, Map<Integer, String> nodeMap,
			int partitionColumn,
			Class<? extends MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> mapperClass,
			Class<? extends ReducerBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue>> reducerClass) {
		super(input, output, nodeId, nodeMap, partitionColumn, mapperClass, reducerClass);
	}

	@Override
	public void run() {
		// TODO: implement this method
		// read data from input (Remember: There is a special operator to read data from
		// a Table)

		// define/instantiate the required operators

		// Mapper
		TableScan ts = new TableScan(input);
		MapperBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue> mapper;

		try {
			mapper = this.mapperClass.newInstance();
			mapper.setChild(ts);
			mapper.open();
			AbstractRecord next;
			
			HeapTable intermediateResult = null;
			boolean init = false;
			
			while ((next = mapper.next()) != null) {
				if (!init) {
					intermediateResult = new HeapTable(next.clone()); // take the first tuple as sample
					init = true;
				}
				intermediateResult.insert(next);
			}
			mapper.close();
			
			// shuffle
			ts = new TableScan(intermediateResult);
			HeapTable shuffledIntermediateResult = null;
			init = false;
			
			Shuffle shuffleOperator = new Shuffle(ts, nodeId, nodeMap, port, partitionColumn);
			shuffleOperator.open();
			
			while ((next = shuffleOperator.next()) != null) {
				if (!init) {
					shuffledIntermediateResult = new HeapTable(next.clone());
					init = true;
				}
				shuffledIntermediateResult.insert(next);
			}
			shuffleOperator.close();
			
			// sort intermediate result
			ts = new TableScan(shuffledIntermediateResult);
			Sort sort = new Sort(ts, recordComparator);
			sort.open();

			HeapTable sortedIntermediateResult = null;
			init = false;

			while ((next = sort.next()) != null) {
				if (!init) {
					sortedIntermediateResult = new HeapTable(next.clone());
					init = true;
				}
				sortedIntermediateResult.insert(next);
			}
			sort.close();
			
			// Reducer
			ReducerBase<? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue, ? extends AbstractSQLValue> reducer;
			reducer = this.reducerClass.newInstance();
			ts = new TableScan(sortedIntermediateResult);
			reducer.setChild(ts);
			reducer.open();

			// process the input and write to the output
			while ((next = reducer.next()) != null) {
				output.insert(next);
			}
			reducer.close();
			// processing done

		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
