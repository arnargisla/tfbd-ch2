#!/bin/bash

for arg in 1 10 50 100 150 200 250 300 350 400 450 500 750 1000 2500 5000; do
  t=$(/usr/bin/time --format="%U %S" python3 main.py $arg  2>&1 >/dev/null)
  t1=$(echo $t | cut -d' ' -f1)
  t2=$(echo $t | cut -d' ' -f2)
  total=$(echo $t1 + $t2 | bc)
  echo $arg $total
done;
