package org.geotools.data.neo4j;

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

