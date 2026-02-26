from pathlib import Path

import mkdocs_gen_files

nav = mkdocs_gen_files.Nav()


for path in sorted(Path("fractal_server").rglob("*.py")):
    module_path = path.relative_to("fractal_server").with_suffix("")
    doc_path = path.relative_to("fractal_server").with_suffix(".md")
    parts = list(module_path.parts)
    full_doc_path = "reference" / doc_path

    # Skip some subpackages and modules
    if (
        "migrations" in parts
        or "data_migrations" in parts
        or "json_schemas" in parts
        or parts == ["__init__"]
        or parts[-1] == "__main__"
    ):
        continue

    # Create index.md pages based on `__init__.py` files
    if parts[-1] == "__init__":
        parts = parts[:-1]
        doc_path = doc_path.with_name("index.md")
        full_doc_path = full_doc_path.with_name("index.md")

    # Add item to the navigation
    nav[parts] = doc_path.as_posix()

    # Write md file with entries like `::: module.submodule`
    with mkdocs_gen_files.open(full_doc_path, "w") as fd:
        identifier = ".".join(["fractal_server"] + parts)
        fd.write(f"::: {identifier}")

    # mkdocs_gen_files.set_edit_path(doc_path, path)


with mkdocs_gen_files.open("reference/SUMMARY.md", "w") as nav_file:
    nav_file.writelines(nav.build_literate_nav())
