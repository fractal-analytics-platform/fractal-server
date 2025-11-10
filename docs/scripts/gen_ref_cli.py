import subprocess  # nosec
from pathlib import Path

import mkdocs_gen_files


output_path = Path("cli_reference.md")

COMMAND = "fractalctl"


def log_help(cmd: list[str]) -> str:
    out = subprocess.check_output(cmd + ["--help"])  # nosec
    return out.decode()


with mkdocs_gen_files.open(output_path, "w") as fd:
    fd.write("# CLI Reference\n\n")
    fd.write(
        "This page shows the help screens for the `fractalctl` command "
        "and its subcommands.\n\n"
    )

    fd.write(f"## `{COMMAND}`\n\n")
    main_help = log_help([COMMAND])
    fd.write("```\n")
    fd.write(main_help)
    fd.write("\n```\n\n")

    subcommands = main_help[
        main_help.index("{") + 1 : main_help.index("}")
    ].split(",")

    for sub in subcommands:
        fd.write(f"## `{COMMAND} {sub}`\n\n")
        help_text = log_help([COMMAND, sub])
        fd.write("```\n")
        fd.write(help_text)
        fd.write("\n```\n\n")
