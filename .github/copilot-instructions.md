# Neo4j Spatial - Copilot Instructions

## Repository Overview

Neo4j Spatial is a Java library that facilitates the import, storage, and querying of spatial data in the Neo4j graph database. This is a mature library (started 2010) that provides GIS capabilities for Neo4j, supporting various spatial formats like Shapefiles and OpenStreetMap data, with an in-graph RTree index for spatial queries.

**Key Statistics:**
- **Size**: ~24MB repository with 239 Java files (179 main source, 60 test files)
- **Language**: Java 17 (required)
- **Build System**: Maven 3.9+ with wrapper (`./mvnw`)
- **Target**: Neo4j 5.26.5, GeoTools 32.2, JTS spatial libraries
- **Architecture**: Multi-module Maven project with primary `server-plugin` module

## Build Instructions

### Prerequisites
- **Java 17** (required - check with `java -version`)
- **Maven 3.9+** (use provided wrapper `./mvnw`)
- **Network access** to Maven Central and OSGeo repositories

### Essential Build Commands

**Always use Maven wrapper for consistency:**

```bash
# Clean and compile (basic build)
./mvnw clean compile

# Full build with tests 
./mvnw clean install

# Quick compilation without tests
./mvnw clean compile -DskipTests

# Run tests only
./mvnw test

# Package for deployment
./mvnw clean package
```

### Test Execution Profiles

The project supports different test modes via Maven profiles:

```bash
# Short tests (recommended for development)
./mvnw test -Denv=short

# Default test suite
./mvnw test -Denv=default  

# Development tests
./mvnw test -Denv=dev

# Long-running tests (CI)
./mvnw test -Denv=long
```

### Known Build Issues & Workarounds

**OSGeo Repository Connectivity Issues:**
If you encounter errors like "Could not transfer artifact... from/to osgeo", this indicates network issues with the OSGeo repository (https://repo.osgeo.org/repository/release/). 

Workarounds:
1. Retry the build command 2-3 times
2. Check network connectivity
3. Use `-o` flag for offline builds if dependencies are cached
4. The CI environment typically resolves these automatically

**Memory Configuration:**
Tests require significant memory. The project is configured for:
- `-Xms1024m -Xmx2048m` for tests
- Single thread execution (`threadCount=1`, `forkCount=1`)
- No test reuse (`reuseForks=false`)

**Dependency Resolution:**
```bash
# Force dependency download
./mvnw dependency:resolve

# Copy dependencies for manual classpath setup
./mvnw dependency:copy-dependencies

# View dependency tree
./mvnw dependency:tree
```

## Project Layout & Architecture

### Core Directory Structure
```
├── pom.xml                 # Root Maven configuration
├── server-plugin/          # Main module containing all spatial functionality
│   ├── pom.xml            # Module-specific Maven config
│   ├── src/main/java/     # Core spatial library code
│   └── src/test/java/     # Test suite
├── .github/workflows/     # GitHub Actions CI/CD
├── docs/                  # Documentation (Antora-based)
└── utils/                 # Utility scripts (Ruby, shell)
```

### Key Source Packages
```
org.neo4j.gis.spatial/
├── SpatialDatabaseService.java    # Main API entry point
├── Layer.java                     # Core spatial layer interface
├── DefaultLayer.java              # Standard layer implementation  
├── procedures/                    # Neo4j stored procedures
├── rtree/                         # Spatial index implementation
├── osm/                          # OpenStreetMap import/export
├── encoders/                     # Geometry encoding (WKT, WKB)
└── filter/                       # Spatial query filters
```

### Configuration Files
- **Maven**: `pom.xml` (root), `server-plugin/pom.xml`
- **CI**: `.github/workflows/pr-build.yaml`, `.github/actions/`
- **Code Style**: `.editorconfig` (tab indentation, 120 char lines)
- **Git**: `.gitignore`, `.travis.yml` (legacy)

### GitHub Actions Workflow

The CI pipeline (`.github/workflows/pr-build.yaml`) runs:
1. JDK 17 setup via `.github/actions/setup-jdk/action.yaml`
2. Maven cache setup via `.github/actions/setup-maven-cache/action.yaml`  
3. Build and test via `.github/actions/run-tests/action.yaml`
   - Command: `./mvnw --no-transfer-progress clean compile test`
4. Test result publishing with surefire reports

**Critical CI Requirements:**
- Must use JDK 17 (project requirement)
- Maven cache recommended for performance
- Test reports expected in `**/target/surefire-reports/*.xml`

### Dependencies & External Libraries

**Core Dependencies:**
- Neo4j 5.26.5 (provided scope - not packaged)
- GeoTools 32.2 for spatial operations
- JTS 1.20.0 for geometry handling
- Neo4j Java Driver 5.28.4

**Test Dependencies:**
- JUnit 5.11.3 (Jupiter engine)
- Neo4j test utilities
- Spatial test data artifacts (OSM, Shapefile)

## Development Guidelines

### Making Changes
1. **Always run** `./mvnw clean compile` before making changes to verify baseline
2. **Test frequently** with `./mvnw test -Denv=short` during development
3. **Full validation** with `./mvnw clean install` before committing
4. **Check style** - follow `.editorconfig` (tabs, 120 chars)

### Common Development Tasks

**Adding new spatial functionality:**
- Core logic: `server-plugin/src/main/java/org/neo4j/gis/spatial/`
- Procedures: `server-plugin/src/main/java/org/neo4j/gis/spatial/procedures/`
- Tests: `server-plugin/src/test/java/org/neo4j/gis/spatial/`

**Running specific tests:**
```bash
# Single test class
./mvnw test -Dtest=TestClassName

# Test pattern
./mvnw test -Dtest="*Spatial*"
```

**Command-line utilities:**
```bash
# OSM import example
./mvnw dependency:copy-dependencies
java -cp target/classes:target/dependency/* org.neo4j.gis.spatial.osm.OSMImporter osm-db two-street.osm

# Using Maven exec
./mvnw exec:java -Dexec.mainClass=org.neo4j.gis.spatial.osm.OSMImporter -Dexec.args="osm-db two-street.osm"
```

### Validation Steps
1. **Build**: `./mvnw clean compile` (must succeed)
2. **Test**: `./mvnw test -Denv=short` (recommended subset)  
3. **Integration**: `./mvnw clean install` (full validation)
4. **Style**: Verify `.editorconfig` compliance

### Performance Notes
- Tests can be slow due to spatial data processing
- Use `-Denv=short` for faster feedback during development
- CI uses `-Denv=default` or longer test suites
- Memory requirements: 1-2GB for test execution

---

**Trust these instructions** - they are validated against the current codebase. Only search for additional information if these instructions are incomplete or incorrect for your specific use case.