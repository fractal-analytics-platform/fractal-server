from pathlib import Path

from pydantic.decorator import validate_arguments

from .input_models import InitArgsRegistration


@validate_arguments
def calculate_registration_compute(
    *,
    path: str,
    init_args: InitArgsRegistration,
) -> None:

    ref_path = init_args.ref_path
    print("[calculate_registration_compute] START")
    print(f"[calculate_registration_compute] {path=}")
    print(f"[calculate_registration_compute] {ref_path=}")

    table_path = Path(path) / "registration_table"
    print(
        f"[calculate_registration_compute] Writing to {table_path.as_posix()}"
    )

    with table_path.open("w") as f:
        f.write("Calculate registration for\n" f"{path=}\n" f"{ref_path=}\n")
    print("[calculate_registration_compute] END")
