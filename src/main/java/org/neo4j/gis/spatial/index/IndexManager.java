/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Spatial.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package org.neo4j.gis.spatial.index;

import java.util.List;
import java.util.concurrent.TimeUnit;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.security.PermissionState;
import org.neo4j.internal.kernel.api.security.PrivilegeAction;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.security.RestrictedAccessMode;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

public class IndexManager {

	private final GraphDatabaseAPI db;
	private final SecurityContext securityContext;

	public static class IndexAccessMode extends RestrictedAccessMode {

		public static SecurityContext withIndexCreate(SecurityContext securityContext) {
			return securityContext.withMode(new IndexAccessMode(securityContext));
		}

		private IndexAccessMode(SecurityContext securityContext) {
			super(securityContext.mode(), Static.SCHEMA);
		}

		@Override
		public PermissionState allowsTokenCreates(PrivilegeAction action) {
			return PermissionState.EXPLICIT_GRANT;
		}

		@Override
		public boolean allowsSchemaWrites() {
			return true;
		}

		@Override
		public PermissionState allowsSchemaWrites(PrivilegeAction action) {
			return PermissionState.EXPLICIT_GRANT;
		}
	}

	public IndexManager(GraphDatabaseAPI db, SecurityContext securityContext) {
		this.db = db;
		this.securityContext = IndexAccessMode.withIndexCreate(securityContext);
	}

	/**
	 * Blocking call that spawns a thread to create an index and then waits for that thread to finish.
	 * This is highly likely to cause deadlocks on index checks, so be careful where it is used.
	 * Best used if you can commit any other outer transaction first, then run this, and after that
	 * start a new transaction. For example, see the OSMImport approaching to batching transactions.
	 * It is possible to use this in procedures with outer transactions if you can ensure the outer
	 * transactions are read-only.
	 */
	public IndexDefinition indexFor(Transaction tx, String indexName, Label label, String propertyKey) {
		return indexFor(tx, indexName, label, propertyKey, true);
	}

	/**
	 * Non-blocking call that spawns a thread to create an index and then waits for that thread to finish.
	 * Use this especially on indexes that are not immediately needed. Also use it if you have an outer
	 * transaction that cannot be committed before making this call.
	 */
	public void makeIndexFor(Transaction tx, String indexName, Label label, String propertyKey) {
		indexFor(tx, indexName, label, propertyKey, false);
	}

	private IndexDefinition indexFor(Transaction tx, String indexName, Label label, String propertyKey,
			boolean waitFor) {
		for (IndexDefinition exists : tx.schema().getIndexes(label)) {
			if (exists.getName().equals(indexName)) {
				return exists;
			}
		}
		String name = "IndexMaker(" + indexName + ")";
		Thread exists = findThread(name);
		if (exists != null) {
			throw new IllegalStateException("Already have thread: " + exists.getName());
		}
		IndexMaker indexMaker = new IndexMaker(indexName, label, propertyKey);
		Thread indexMakerThread = new Thread(indexMaker, name);
		if (waitFor) {
			indexMakerThread.start();
			try {
				indexMakerThread.join();
				if (indexMaker.e != null) {
					throw new RuntimeException("Failed to make index " + indexMaker.description(), indexMaker.e);
				}
				return indexMaker.index;
			} catch (InterruptedException e) {
				throw new RuntimeException("Failed to make index " + indexMaker.description(), e);
			}
		}
		return null;
	}

	public void deleteIndex(IndexDefinition index) {
		String name = "IndexRemover(" + index.getName() + ")";
		Thread exists = findThread(name);
		if (exists != null) {
			System.out.println("Already have thread: " + exists.getName());
		} else {
			IndexRemover indexRemover = new IndexRemover(index);
			Thread indexRemoverThread = new Thread(indexRemover, name);
			indexRemoverThread.start();
		}
	}

	public static void waitForDeletions() {
		waitForThreads("IndexMaker");
		waitForThreads("IndexRemover");
	}

	private static void waitForThreads(String prefix) {
		Thread found;
		while ((found = findThread(prefix)) != null) {
			try {
				found.join();
			} catch (InterruptedException e) {
				throw new RuntimeException("Wait for thread " + found.getName(), e);
			}
		}
	}

	private static Thread findThread(String prefix) {
		ThreadGroup rootGroup = Thread.currentThread().getThreadGroup();
		Thread found = findThread(rootGroup, prefix);
		if (found != null) {
			System.out.println("Found thread in current group[" + rootGroup.getName() + "}: " + prefix);
			return found;
		}
		ThreadGroup parentGroup;
		while ((parentGroup = rootGroup.getParent()) != null) {
			rootGroup = parentGroup;
		}
		found = findThread(rootGroup, prefix);
		if (found != null) {
			System.out.println("Found thread in root group[" + rootGroup.getName() + "}: " + prefix);
			return found;
		}
		return null;
	}

	private static Thread findThread(ThreadGroup group, String prefix) {
		Thread[] threads = new Thread[group.activeCount()];
		while (group.enumerate(threads, true) == threads.length) {
			threads = new Thread[threads.length * 2];
		}
		for (Thread thread : threads) {
			if (thread != null && thread.getName() != null && thread.getName().startsWith(prefix)) {
				return thread;
			}
		}
		return null;
	}

	private class IndexMaker implements Runnable {

		private final String indexName;
		private final Label label;
		private final String propertyKey;
		private Exception e;
		private IndexDefinition index;

		private IndexMaker(String indexName, Label label, String propertyKey) {
			this.indexName = indexName;
			this.label = label;
			this.propertyKey = propertyKey;
			this.e = null;
		}

		@Override
		public void run() {
			try {
				try (Transaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, securityContext)) {
					index = findIndex(tx);
					if (index == null) {
						index = tx.schema().indexFor(label).withName(indexName).on(propertyKey).create();
					}
					tx.commit();
				}
				try (Transaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, securityContext)) {
					tx.schema().awaitIndexOnline(indexName, 30, TimeUnit.SECONDS);
				}
			} catch (Exception e) {
				this.e = e;
			}
		}

		private IndexDefinition findIndex(Transaction tx) {
			for (IndexDefinition index : tx.schema().getIndexes()) {
				if (indexMatches(index)) {
					return index;
				}
			}
			return null;
		}

		private boolean indexMatches(IndexDefinition anIndex) {
			if (anIndex.getName().equals(indexName)) {
				try {
					List<Label> labels = (List<Label>) anIndex.getLabels();
					List<String> propertyKeys = (List<String>) anIndex.getPropertyKeys();
					if (labels.size() == 1 && propertyKeys.size() == 1 && labels.get(0).equals(label)
							&& propertyKeys.get(0).equals(propertyKey)) {
						return true;
					}
					throw new IllegalStateException(
							"Found index with matching name but different specification: " + anIndex);
				} catch (ClassCastException e) {
					throw new RuntimeException(
							"Neo4j API Changed - Failed to retrieve IndexDefinition for index " + description() + ": "
									+ e.getMessage());
				}
			}
			return false;
		}

		private String description() {
			return indexName + " FOR (n:" + label.name() + ") ON (n." + propertyKey + ")";
		}
	}

	private class IndexRemover implements Runnable {

		private final IndexDefinition index;

		private IndexRemover(IndexDefinition index) {
			this.index = index;
		}

		@Override
		public void run() {
			try {
				try (Transaction tx = db.beginTransaction(KernelTransaction.Type.EXPLICIT, securityContext)) {
					// Need to find and drop in the same transaction due to saved state in the index definition implementation
					IndexDefinition found = tx.schema().getIndexByName(index.getName());
					if (found != null) {
						found.drop();
					}
					tx.commit();
				}
			} catch (Exception ignored) {
			}
		}
	}
}
