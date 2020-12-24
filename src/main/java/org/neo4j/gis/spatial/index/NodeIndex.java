package org.neo4j.gis.spatial.index;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;
import org.neo4j.graphdb.Node;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class NodeIndex<E> {
    Directory indexDir = new RAMDirectory();
    private final String indexName;

    public NodeIndex(String indexName) {
        this.indexName = indexName;
        // TODO create Lucene index
    }

    public void add(Node geomNode, String indexTypeName, E indexValueFor) {
        // TODO add to lucene index
    }

    public void remove(Node geomNode) {
        // TODO remove from lucene index
    }

    public IndexHits<Node> query(String type, String query) {
        // TODO query lucene index
        return new IndexHits<>();
    }

    public IndexHits<Node> get(String key, Object value) {
        return new IndexHits<>();
    }

    public void delete() {
        // TODO delete lucene index
    }

    public class IndexHits<E> implements Iterable<E> {
        private final List<E> hits = new ArrayList<>();

        @Override
        public Iterator<E> iterator() {
            return hits.iterator();
        }

        public E getSingle() {
            if (hits.size() == 1) return hits.get(0);
            else throw new IllegalStateException("Expected exactly one entry in IndexHits for '" + indexName + "', but got " + hits.size());
        }

        public int size() {
            return hits.size();
        }

        public void close() {
        }
    }

}
