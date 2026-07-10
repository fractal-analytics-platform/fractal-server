from pathlib import Path

from pydantic import RootModel

from fractal_server.types import Field
from fractal_server.types import NonEmptyStr


class CoreTaskList(RootModel):
    """
    List of valid items, with unique names.
    """

    root: set[
        tuple[
            NonEmptyStr,
            NonEmptyStr,
            NonEmptyStr,
        ]
    ] = Field(default_factory=list)
    """
    List of tuples like `(pkg_name, version, task_name)`.
    """


def _read_file_or_empty_list(path: Path | None) -> set[tuple[str, str, str]]:
    if path is None or not path.exists():
        return set()
    else:
        return CoreTaskList.model_validate_json(path.read_text()).root


def _get_final_list(
    *,
    main_list_path: Path | None = None,
    custom_list_path: Path | None = None,
    ignore_list_path: Path | None = None,
) -> set[tuple[str, str, str]]:
    main_list = _read_file_or_empty_list(main_list_path)
    custom_list = _read_file_or_empty_list(custom_list_path)
    ignore_list = _read_file_or_empty_list(ignore_list_path)

    final_list = (main_list.union(custom_list)).difference(ignore_list)
    return final_list


def sync_core_tasks(
    *,
    main_list_path: Path | None = None,
    custom_list_path: Path | None = None,
    ignore_list_path: Path | None = None,
):
    print(
        _get_final_list(
            main_list_path=main_list_path,
            custom_list_path=custom_list_path,
            ignore_list_path=ignore_list_path,
        )
    )
