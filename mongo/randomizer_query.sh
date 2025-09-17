#!/bin/bash

mongosh mongodb://127.0.0.1:27117,127.0.0.1:27118/HORAS \
  --quiet \
  --eval 'db.test.insertOne({ abc: 1}).comment("Data creation")'

# MongoDB command to run
CMD="mongosh mongodb://127.0.0.1:27117,127.0.0.1:27118/HORAS \
  --quiet \
  --eval 'db.test.find({ abc: 1}, { abc: 1}).sort({ created_at: -1 }).comment(\"This is from where the query is called\")'"

t=0
while true; do
  ((t++))

  # Pick random number 1–10 (adjust as needed)
  N=$(( (RANDOM % 10) + 1 ))
  echo "Second $t → running $N sequentially"

  for ((i=1; i<=N; i++)); do
    eval $CMD   # no "&" → wait for each mongosh to complete
  done

  sleep 1
done
