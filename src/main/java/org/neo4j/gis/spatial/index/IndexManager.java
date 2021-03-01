package org.neo4j.gis.spatial.index;

import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.schema.IndexDefinition;
import org.neo4j.internal.kernel.api.security.SecurityContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.internal.GraphDatabaseAPI;

import java.util.List;
import java.util.concurrent.TimeUnit;

public class IndexManager {

    private final GraphDatabaseAPI db;
    private final SecurityContext securityContext;

    public IndexManager(GraphDatabaseAPI db, SecurityContext securityContext) {
        this.db = db;
        this.securityContext = securityContext;
    }

    public IndexDefinition indexFor(String indexName, Label label, String propertyKey) {
        IndexMaker indexMaker = new IndexMaker(indexName, label, propertyKey);
        Thread indexMakerThread = new Thread(indexMaker, "IndexMaker(" + indexName + ")");
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

    public void deleteIndex(IndexDefinition index) {
        IndexRemover indexRemover = new IndexRemover(index);
        Thread indexRemoverThread = new Thread(indexRemover, "IndexRemover(" + index.getName() + ")");
        indexRemoverThread.start();
        try {
            indexRemoverThread.join();
            if (indexRemover.e != null) {
                throw new RuntimeException("Failed to remove index " + index.getName(), indexRemover.e);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException("Failed to remove index " + index.getName(), e);
        }
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
                try (Transaction tx = db.beginTransaction(KernelTransaction.Type.explicit, securityContext)) {
                    index = findIndex(tx);
                    if (index == null) {
                        index = tx.schema().indexFor(label).withName(indexName).on(propertyKey).create();
                    }
                    tx.commit();
                }
                try (Transaction tx = db.beginTransaction(KernelTransaction.Type.explicit, securityContext)) {
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
                    if (labels.size() == 1 && propertyKeys.size() == 1 && labels.get(0).equals(label) && propertyKeys.get(0).equals(propertyKey)) {
                        return true;
                    } else {
                        throw new IllegalStateException("Found index with matching name but different specification: " + anIndex);
                    }
                } catch (ClassCastException e) {
                    throw new RuntimeException("Neo4j API Changed - Failed to retrieve IndexDefinition for index " + description() + ": " + e.getMessage());
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
        private Exception e;

        private IndexRemover(IndexDefinition index) {
            this.index = index;
            this.e = null;
        }

        @Override
        public void run() {
            try {
                try (Transaction tx = db.beginTransaction(KernelTransaction.Type.explicit, securityContext)) {
                    // Need to find and drop in the same transaction due to saved state in the index definition implementation
                    IndexDefinition found = tx.schema().getIndexByName(index.getName());
                    if (found != null) {
                        found.drop();
                    }
                    tx.commit();
                }
            } catch (Exception e) {
                this.e = e;
            }
        }
    }
}
