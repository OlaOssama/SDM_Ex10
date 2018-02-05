package de.tuda.sdm.dmdb.exercise10.prob1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Context for information transportation/storage
 * 
 * @param <KEY>
 *            type of keys to store
 * @param <VALUE>
 *            type of values to store
 */

public class Context<KEY, VALUE> {
	List<KEY> keys;
	HashMap<KEY, List<VALUE>> keyValues;

	/**
	 * Constructor
	 */
	public Context() {
		keys = new ArrayList<>();
		keyValues = new HashMap<>();
	}

	/**
	 * Add a key-value-pair to context
	 * 
	 * @param k
	 *            key to store
	 * @param v
	 *            value to store
	 */
	public void write(KEY k, VALUE v) {
		if (!keyValues.containsKey(k)) {
			keyValues.put(k, new ArrayList<VALUE>());
			keys.add(k);
		}

		keyValues.get(k).add(v);
	}

	public void display() {
		for (KEY key : keys) {
			for (VALUE value : this.keyValues.get(key)) {
				System.out.print(key + " : ");
				((Context) value).display();
				System.out.println();
			}
		}
	}
}
