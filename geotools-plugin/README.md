# Neo4j Spatial GeoTools Plugin

This module provides integration between Neo4j Spatial and GeoTools. It enables GeoTools-based applications to connect to Neo4j as a data source using the standard GeoTools DataStore API.

## Key Features

- Connect to Neo4j using the Neo4j driver
- Access Neo4j Spatial layers as GeoTools FeatureSources
- Read and write spatial data using the GeoTools API
- Uses Neo4j Spatial Procedures and Functions instead of direct database access

## Usage

```java
Map<String, Object> params = new HashMap<>();
params.put("dbtype", "neo4j-driver");
params.put("uri", "bolt://localhost:7687");
params.put("database", "neo4j");
params.put("username", "neo4j");
params.put("password", "password");

DataStore dataStore = DataStoreFinder.getDataStore(params);

// Access layer as a feature source
SimpleFeatureSource featureSource = dataStore.getFeatureSource("layerName");

// Read features
SimpleFeatureCollection features = featureSource.getFeatures();

// Write features using a transaction
SimpleFeatureStore featureStore = (SimpleFeatureStore) featureSource;
featureStore.addFeatures(featureCollection);
```

## Implementation Details

This implementation uses the Neo4j Spatial Procedures and Functions API to access spatial data.

The main components are:

- `Neo4jSpatialDataStoreFactory`: Creates connections to Neo4j using the Neo4j Java Driver
- `Neo4jSpatialDataStore`: Manages the GeoTools DataStore functionality
- `Neo4jSpatialFeatureSource`: Reads features from Neo4j Spatial layers
- `Neo4jSpatialFeatureStore`: Adds write capabilities to FeatureSource

## Requirements

- Neo4j (with Spatial plugin installed)
- Neo4j Java Driver
- GeoTools libraries
