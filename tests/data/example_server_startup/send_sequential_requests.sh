for i in {1..4000}; do curl localhost:8000/api/alive/ >> pippo.txt; echo -n "," >> pippo.txt; done
