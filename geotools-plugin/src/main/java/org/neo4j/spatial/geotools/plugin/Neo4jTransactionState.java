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

package org.neo4j.spatial.geotools.plugin;

import java.io.IOException;
import org.geotools.api.data.Transaction;
import org.geotools.api.data.Transaction.State;
import org.neo4j.driver.Session;

final class Neo4jTransactionState implements State {

	private final Session session;
	private org.neo4j.driver.Transaction neo4jTransaction;
	private boolean closed = false;
	private boolean autoCommit = false;

	public Neo4jTransactionState(Session session) {
		this.session = session;
	}

	@Override
	public void setTransaction(Transaction tx) {
		if (tx == null) {
			this.neo4jTransaction = null;
		} else {
			autoCommit = tx == Transaction.AUTO_COMMIT;
			if (!autoCommit) {
				if (neo4jTransaction == null) {
					this.neo4jTransaction = session.beginTransaction();
				}
			} else {
				this.neo4jTransaction = null;
			}
		}
	}

	@Override
	public void addAuthorization(String authID) {
	}

	@Override
	public void commit() throws IOException {
		if (neo4jTransaction != null && !closed) {
			try {
				neo4jTransaction.commit();
				neo4jTransaction.close();
				if (!autoCommit) {
					neo4jTransaction = session.beginTransaction();
				}
			} catch (Exception e) {
				throw new IOException("Failed to commit Neo4j transaction", e);
			}
		}
	}

	@Override
	public void rollback() throws IOException {
		if (neo4jTransaction != null && !closed) {
			try {
				neo4jTransaction.rollback();
				neo4jTransaction.close();
				if (!autoCommit) {
					neo4jTransaction = session.beginTransaction();
				}
			} catch (Exception e) {
				throw new IOException("Failed to rollback Neo4j transaction", e);
			}
		}
	}

	public void close() {
		try {
			if (neo4jTransaction != null) {
				neo4jTransaction.close();
			}
			session.close();
		} finally {
			closed = true;
		}
	}

	public Session getSession() {
		return session;
	}

	public org.neo4j.driver.Transaction getNeo4jTransaction() {
		return neo4jTransaction;
	}
}

