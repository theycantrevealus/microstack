#!/bin/bash
# convert.sh

# Input file (raw JSON with ObjectId/ISODate)
INPUT="input.json"
OUTPUT="output.json"

sed -E \
  -e 's/ObjectId\("([0-9a-fA-F]+)"\)/{"$oid": "\1"}/g' \
  -e 's/ISODate\("([^"]+)"\)/{"$date": "\1"}/g' \
  "$INPUT" > "$OUTPUT"

echo "✅ Conversion complete! Saved to $OUTPUT"
