import os
import subprocess  # nosec
from pathlib import Path

BASE_CMD = "fractalctl"


os.environ["JWT_SECRET_KEY"] = "fake-jwt-secret-key"  # nosec
os.environ["POSTGRES_DB"] = "fake-postgres-db"


def log_help(cmd: list[str]) -> str:
    out = subprocess.check_output(
        cmd + ["--help"],
        encoding="utf-8",
    )  # nosec
    return out.strip()


output_file = Path(__file__).parents[1] / "cli_reference.md"
print(output_file)
with output_file.open("w") as f:
    f.write("# CLI Reference\n\n")
    f.write(
        "This page shows the help screens for the `fractalctl` command "
        "and its subcommands.\n\n"
    )

    f.write(f"## `{BASE_CMD}`\n\n")
    main_help = log_help([BASE_CMD])
    f.write("```\n")
    f.write(main_help)
    f.write("\n```\n\n")

    subcommands = main_help[
        main_help.index("{") + 1 : main_help.index("}")
    ].split(",")

    for sub in subcommands:
        f.write(f"## `{BASE_CMD} {sub}`\n\n")
        help_text = log_help([BASE_CMD, sub])
        f.write("```\n")
        f.write(help_text)
        f.write("\n```\n\n")
