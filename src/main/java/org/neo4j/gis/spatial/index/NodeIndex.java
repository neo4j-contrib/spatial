package org.neo4j.gis.spatial.index;

import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.*;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.NIOFSDirectory;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static java.lang.String.format;

/**
 * The NodeIndex is a mapper onto Lucene for indexing a 1D encoding of 2D space.
 * Geohash indexes use Strings for the encoding, while ZOrder and Hilbert space
 * filling curves use Long values.
 *
 * @param <E> either a String or a Long depending on whether the index is geohash or space-filling curve.
 */
public class NodeIndex<E> {
    private final Directory indexDir;
    private final StandardAnalyzer analyzer;
    private final String indexName;

    public NodeIndex(String indexName, IndexManager indexManager) {
        this.indexName = indexName;
        Path path = indexManager.makePathFor(indexName);
        try {
            indexDir = new NIOFSDirectory(path);
            analyzer = new StandardAnalyzer();
        } catch (IOException e) {
            throw new NodeIndexException(indexName, "initialize", e);
        }
    }

    public void add(Node geomNode, String indexKey, E indexValueFor) {
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        try (IndexWriter writer = new IndexWriter(indexDir, config)) {
            addNodeAsDoc(writer, indexKey, geomNode, indexValueFor);
            writer.commit();
        } catch (IOException e) {
            throw new NodeIndexException(indexName, "add to", e);
        }
    }

    private void addNodeAsDoc(IndexWriter w, String indexKey, Node node, E value) throws IOException {
        String valueAsString = String.valueOf(value);
        Document doc = new Document();
        // use a string field for nodeId because we don't want it tokenized
        doc.add(new StringField("node", String.valueOf(node.getId()), Field.Store.YES));
        // use a string field for value because we don't want it tokenized
        doc.add(new StringField(indexKey, valueAsString, Field.Store.YES));
        w.addDocument(doc);
    }

    public void remove(Node geomNode) {
        // TODO remove from lucene index
    }

    public IndexHits queryAll() {
        try {
            if (DirectoryReader.indexExists(indexDir)) {
                try (IndexReader reader = DirectoryReader.open(indexDir)) {
                    return new IndexHits(reader).readAll();
                }
            } else {
                return new IndexHits();
            }
        } catch (IOException e) {
            e.printStackTrace();
            throw new NodeIndexException(indexName, "query", e);
        }
    }

    public IndexHits query(String indexKey, String query) {
        try {
            // the "indexKey" arg specifies the default field to use
            // when no field is explicitly specified in the query.
            return query(new QueryParser(indexKey, analyzer).parse(query));
        } catch (ParseException e) {
            throw new NodeIndexException(indexName, "query", e);
        }
    }

    public IndexHits query(Query query) {
        try {
            if (DirectoryReader.indexExists(indexDir)) {
                try (IndexReader reader = DirectoryReader.open(indexDir)) {
                    IndexSearcher searcher = new IndexSearcher(reader);
                    IndexHits hits = new IndexHits(reader);
                    searcher.search(query, hits);
                    return hits;
                }
            } else {
                return new IndexHits();
            }
        } catch (IOException e) {
            throw new NodeIndexException(indexName, "query", e);
        }
    }

    public IndexHits get(String key, Object value) {
        return query("string", String.valueOf(value));
    }

    public void delete() {
        // TODO delete lucene index
    }

    /**
     * When searching an index with value type <code>E</code> which can be string or long,
     * we still expect the results returned to be of type <code>Long</code> containing the Node ID.
     */
    public class IndexHits extends SimpleCollector implements Iterable<Long> {
        private IndexReader reader;
        private final List<Long> hits = new ArrayList<>();

        public IndexHits() {
        }

        public IndexHits(IndexReader reader) {
            this.reader = reader;
        }

        @Override
        protected void doSetNextReader(LeafReaderContext context) {
            this.reader = context.reader();
        }

        @Override
        public Iterator<Long> iterator() {
            return hits.iterator();
        }

        public Long getSingle() {
            if (hits.size() == 1) return hits.get(0);
            else throw new IllegalStateException("Expected exactly one entry in IndexHits for '" + indexName + "', but got " + hits.size());
        }

        public int size() {
            return hits.size();
        }

        public void close() {
        }

        public Iterable<Node> asNodes(Transaction tx) {
            List<Node> nodes = new ArrayList<>();
            for (long nodeId : hits) {
                nodes.add(tx.getNodeById(nodeId));
            }
            return nodes;
        }

        @Override
        public ScoreMode scoreMode() {
            return ScoreMode.COMPLETE;
        }

        public IndexHits readAll() throws IOException {
            for (int i = 0; i < reader.numDocs(); i++) {
                Document d = reader.document(i);
                String node = d.get("node");
                hits.add(Long.valueOf(node));
            }
            return this;
        }

        @Override
        public void collect(int doc) throws IOException {
            add(reader.document(doc));
        }

        private void add(Document d) {
            String node = d.get("node");
            hits.add(Long.valueOf(node));
        }
    }

    /**
     * To avoid having to add IOException to all call stacks that reach the lucene index,
     * we wrap in a RuntimeException.
     */
    public static class NodeIndexException extends RuntimeException {
        public NodeIndexException(String name, String verb, Exception e) {
            super(format("Failed to %s explicit node index '%s': %s", verb, name, e.getMessage(), e));
        }
    }
}
