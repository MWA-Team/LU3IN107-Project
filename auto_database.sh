#!/bin/bash

while getopt ":c:i:s:h" arg; do
  case $arg in
    c)
      curl "http://localhost:8080/table"
      ;;
    i)
      i=0
      while [ $i -ge $OPTARG ]; do
        curl -d @./src/main/resources/parquet_files/yellow_tripdata_2009-01.parquet "http://localhost:8080/insert"
        ((i++))
      done
      ;;
    s)
      echo "SELECT TODO"
      # curl "http://localhost:8080/select"
      ;;
    h)
      echo "usage TODO"
      ;;
  esac
done