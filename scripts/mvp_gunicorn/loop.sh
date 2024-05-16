#!/bin/bash

for i in {1..10}; do
    curl -X POST 'http://localhost:8000/add-status/?id='$i'' -H 'accept: application/json' -d ''
    echo
done
