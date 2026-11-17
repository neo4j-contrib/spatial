#!/usr/bin/env bash
# Creates an upgrade-compatibility fixture: a store written by a released plugin version, read by
# UpgradeCompatibilityTest. Needs docker, curl, zip.
# One fixture per store format, not per release: add one only if a release writes the store differently
# (layer/metadata node properties, stored ids or class names), i.e. if the property sections of expected.txt
# differ from the newest fixture. Keep the oldest release of each format; it covers the longest upgrade path.
#   usage: generate-fixture.sh <plugin-version> <neo4j-version>     e.g. 2026.10.0 2026.10.2
# The store is built by create.cypher and populate.cypher in the fixture directory, written in that release's
# procedure syntax. For a new version they are copied from the newest fixture first: adjust them, then rerun.
# Afterwards: mvn -pl compat-tests -am test -Dtest=UpgradeCompatibilityTest -Dsurefire.failIfNoSpecifiedTests=false -Dcompat.update=true
# to write expected.txt, then review it.
set -euo pipefail
plugin=$1
neo4j=$2
here=$(cd "$(dirname "$0")" && pwd)
fixtures="$here/../resources/compat"
target="$fixtures/$plugin"
if [[ ! -f "$target/create.cypher" ]]; then
	latest=$(ls -d "$fixtures"/*/ 2>/dev/null | sort -V | tail -1)
	mkdir -p "$target"
	cp "$latest/create.cypher" "$latest/populate.cypher" "$target/"
	echo "copied create.cypher and populate.cypher from $(basename "$latest") to $target:"
	echo "adapt them to $plugin (e.g. config maps instead of strings), then rerun"
	exit 1
fi
work=$(mktemp -d)
container="spatial-compat-$plugin"
trap 'docker rm -f "$container" >/dev/null 2>&1 || true; rm -rf "$work"' EXIT

mkdir -p "$work/plugins" "$work/data"
release="https://github.com/neo4j-contrib/spatial/releases/download/$plugin"
# jar names changed with 2025.07
curl -fsSL -o "$work/plugins/server-plugin.jar" "$release/neo4j-spatial-server-plugin-$plugin-with-dependencies.jar" 2>/dev/null \
	|| curl -fsSL -o "$work/plugins/server-plugin.jar" "$release/neo4j-spatial-$plugin-server-plugin.jar"
# separate OSM plugin since 2026.10.0; part of the server plugin before
curl -fsSL -o "$work/plugins/osm-server-plugin.jar" \
	"$release/neo4j-spatial-osm-server-plugin-$plugin-with-dependencies.jar" 2>/dev/null \
	|| { rm -f "$work/plugins/osm-server-plugin.jar"; echo "no separate OSM plugin in $plugin"; }

docker pull -q "neo4j:$neo4j-community" >/dev/null
# no tx log preallocation: keeps the fixture small
docker run -d --name "$container" --user "$(id -u):$(id -g)" \
	-v "$work/data:/data" -v "$work/plugins:/plugins" \
	-e NEO4J_AUTH=none -e 'NEO4J_dbms_security_procedures_unrestricted=spatial.*' \
	-e NEO4J_db_tx__log_preallocate=false \
	"neo4j:$neo4j-community" >/dev/null
for _ in $(seq 1 60); do
	docker exec "$container" cypher-shell "RETURN 1" >/dev/null 2>&1 && break
	sleep 2
done

docker cp "$here/../../../../osm-server-plugin/two-street.osm" "$container:/var/lib/neo4j/two-street.osm"
for script in create populate; do
	docker cp "$target/$script.cypher" "$container:/tmp/$script.cypher"
	docker exec "$container" cypher-shell -f "/tmp/$script.cypher" >/dev/null
done
docker stop "$container" >/dev/null

rm -f "$target/store.zip"
(cd "$work/data" && zip -qr "$target/store.zip" databases/neo4j transactions/neo4j -x '*_lock')
printf 'plugin=%s\nneo4j=%s\n' "$plugin" "$neo4j" > "$target/fixture.properties"
echo "fixture written to $target ($(du -h "$target/store.zip" | cut -f1))"
