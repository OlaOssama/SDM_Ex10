package de.tuda.sdm.dmdb.sql.operator.exercise;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.PriorityQueue;

import de.tuda.sdm.dmdb.mapReduce.operator.MapReduceOperator;
import de.tuda.sdm.dmdb.sql.operator.Operator;
import de.tuda.sdm.dmdb.sql.operator.SortBase;
import de.tuda.sdm.dmdb.storage.AbstractRecord;
import de.tuda.sdm.dmdb.storage.Record;

public class Sort extends SortBase {

	public Sort(Operator child, Comparator<AbstractRecord> recordComparator) {
		super(child, recordComparator);
	}

	@Override
	public void open() {
		child.open();
		sortedRecords = new PriorityQueue<AbstractRecord>(recordComparator);
		AbstractRecord next;
		do {
			next = child.next();
			if (next != null)
				sortedRecords.offer(next);
		} while (next != null);
		// TODO: implement this method
		// make sure to initialize the required (inherited) member variables
	}

	@Override
	public AbstractRecord next() {
		return sortedRecords.poll();
		// block and sort when required
		// blocking part
		// sort, by adding to PriorityQueue
	}

	@Override
	public void close() {
		child.close();
		sortedRecords = null;
		// TODO: implement this method
		// reverse what was done in open()
	}

}
