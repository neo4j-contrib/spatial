package org.neo4j.gis.spatial;

import org.geotools.data.neo4j.Neo4jSpatialDataStore;
import org.geotools.data.simple.SimpleFeatureCollection;
import org.geotools.data.simple.SimpleFeatureSource;
import org.geotools.geometry.jts.ReferencedEnvelope;
import org.geotools.referencing.crs.DefaultGeographicCRS;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.gis.spatial.osm.OSMImporter;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.test.TestGraphDatabaseFactory;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;

import javax.xml.stream.XMLStreamException;
import java.io.IOException;
import java.nio.charset.Charset;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertThat;

public class Neo4jSpatialDataStoreTest {

    public GraphDatabaseService graph;

    @Before
    public void setup() throws IOException, XMLStreamException {
        this.graph = new TestGraphDatabaseFactory().newImpermanentDatabase();
        OSMImporter importer = new OSMImporter("map", new ConsoleListener());
        importer.setCharset(Charset.forName("UTF-8"));
        importer.setVerbose(false);
        importer.importFile(graph, "map.osm");
        importer.reIndex(graph);
    }

    @Test
    public void shouldOpenDataStore() {
        Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
        ReferencedEnvelope bounds = store.getBounds("map");
        assertThat(bounds, equalTo(new ReferencedEnvelope(12.7856667, 13.2873561, 55.9254241, 56.2179056, DefaultGeographicCRS.WGS84)));
    }

    @Test
    public void shouldOpenDataStoreOnNonSpatialDatabase() {
        GraphDatabaseService db = new TestGraphDatabaseFactory().newImpermanentDatabase();
        Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(db);
        ReferencedEnvelope bounds = store.getBounds("map");
        // TODO: rather should throw a descriptive exception
        assertThat(bounds, equalTo(null));
    }

    @Test
    public void shouldBeAbleToListLayers() throws IOException {
        Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
        String[] layers = store.getTypeNames();
        assertThat("Expected one layer", layers.length, equalTo(1));
        assertThat(layers[0], equalTo("map"));
    }

    @Test
    public void shouldBeAbleToGetSchemaForLayer() throws IOException {
        Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
        SimpleFeatureType schema = store.getSchema("map");
        assertThat("Expected 25 attributes", schema.getAttributeCount(), equalTo(25));
        assertThat("Expected geometry attribute to be called 'the_geom'", schema.getAttributeDescriptors().get(0).getLocalName(), equalTo("the_geom"));
    }

    @Test
    public void shouldBeAbleToGetFeatureSourceForLayer() throws IOException {
        Neo4jSpatialDataStore store = new Neo4jSpatialDataStore(graph);
        SimpleFeatureSource source = store.getFeatureSource("map");
        SimpleFeatureCollection features = source.getFeatures();
        assertThat("Expected 217 features", features.size(), equalTo(217));
        SimpleFeature feature = features.features().next();
        assertThat("Expected first feature to have name 'Nybrodalsvägen'", feature.getAttribute("name").toString(), equalTo("Nybrodalsvägen"));
    }


}
