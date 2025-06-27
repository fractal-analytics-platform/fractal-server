#!/bin/bash

set -e

curl http://localhost:8000/api/db-sync-dependency/
echo
curl http://localhost:8000/api/db-async-dependency/
echo
curl http://localhost:8000/api/db-sync-context/
echo
curl http://localhost:8000/api/db-async-context/
echo
