#!/usr/bin/env bash
# Whether the store written by <plugin-version> has a new format: compares the stored properties (the sections of
# expected.txt before the first per-layer query) with every other fixture. Exit 0: new format; 1: same as a fixture.
#   usage: format-changed.sh <plugin-version>
set -euo pipefail
version=$1
fixtures="$(cd "$(dirname "$0")" && pwd)/../resources/compat"
stored() {
	awk '/^[^ ].* \| /{exit} {print}' "$fixtures/$1/expected.txt" | sed -E 's/(k=ctime, v=)[0-9]+/\1T/'
}
for other in $(ls "$fixtures" | grep -vxF "$version" | sort -V); do
	if diff -q <(stored "$other") <(stored "$version") >/dev/null; then
		echo "same store format as $other"
		exit 1
	fi
done
echo "new store format"
exit 0
