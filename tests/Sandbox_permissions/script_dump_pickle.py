#!/usr/bin/env python3
import os
import pickle
import sys

with open(sys.argv[1], "wb") as f:
    pickle.dump({"key": "value"}, f)
    print(f"User {os.getuid()} successfully wrote data to {sys.argv[1]}")
