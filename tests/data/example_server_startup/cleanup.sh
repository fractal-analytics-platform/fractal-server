#!/bin/bash

rm test.db
rm -r Tasks
rm -r Artifacts
rm logs*
rm test.db-shm test.db-wal

dropdb --if-exists fractal-examples-test
