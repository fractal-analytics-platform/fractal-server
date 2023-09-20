import sys
from pathlib import Path

test_dir = Path(__file__).parent
repo_root_dir = str(test_dir.parent)

sys.path.append(repo_root_dir)
