import json
from collections import Counter


def analyze_pids(file_path):
    with open(file_path, "r") as file:
        data = file.read()

    if data.startswith(","):
        data = data[1:]

    pid_list = []
    for item in data.split(","):
        try:
            pid_list.append(json.loads(item)["pid"])
        except json.JSONDecodeError:
            print("Invalid")
            continue

    pid_counts = Counter(pid_list)

    print("PID Statistics:")
    for pid, count in pid_counts.items():
        print(f"PID: {pid}, Count: {count}")


file_path = "pippo.txt"
analyze_pids(file_path)
