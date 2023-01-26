#!/usr/bin/env python3
import os
import pickle
import sys

with open(sys.argv[1], "rb") as f:
    content = pickle.load(f)
    assert content == {"key": "value"}
    print(f"User {os.getuid()} successfully read data from {sys.argv[1]}")
