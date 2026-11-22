#!/bin/sh
# Run the maelstrom g-counter test N times and report PASS/FAIL for each.
n=${1:-3}
for i in $(seq 1 "$n"); do
  out=$(timeout 240 maelstrom test --workload g-counter --bin ./node \
        --nemesis partition --node-count 3 --rate 100 --time-limit 20 2>&1)
  if echo "$out" | grep -q "Everything looks good"; then
    finals=$(echo "$out" | grep -o 'final-reads ([^)]*)' | head -1)
    acc=$(echo "$out" | grep -o 'acceptable (\[[^]]*\])' | head -1)
    okc=$(echo "$out" | grep -o 'ok-count [0-9]*' | head -1)
    echo "run $i: PASS  $finals $acc $okc"
  else
    echo "run $i: FAIL"
    echo "$out" | grep -v "WARNING\|Warning" | tail -50
  fi
done
