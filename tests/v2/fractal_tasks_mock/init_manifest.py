import json

from devtools import debug

from fractal_server.app.schemas.v2.manifest import ManifestV2
from fractal_server.app.schemas.v2.manifest import TaskManifestV2


TASK_LIST_DICT = {
    "create_ome_zarr_compound": dict(
        name="create_ome_zarr_compound",
        executable_non_parallel="create_cellvoyager_ome_zarr.py",
        executable_parallel="fill_cellvoyager_ome_zarr.py",
    ),
    "create_ome_zarr_multiplex_compound": dict(
        name="create_ome_zarr_multiplex_compound",
        executable_non_parallel="create_cellvoyager_ome_zarr_multiplex.py",
        executable_parallel="fill_cellvoyager_ome_zarr.py",
    ),
    "MIP_compound": dict(
        name="MIP_compound",
        input_types={"3D": True},
        executable_non_parallel="new_ome_zarr.py",
        executable_parallel="maximum_intensity_projection.py",
        output_types={"3D": False},
    ),
    "illumination_correction": dict(
        name="illumination_correction",
        input_types=dict(illumination_correction=False),
        executable_parallel="illumination_correction.py",
        output_types=dict(illumination_correction=True),
    ),
    "illumination_correction_compound": dict(
        name="illumination_correction_compound",
        input_types=dict(illumination_correction=False),
        executable_non_parallel="illumination_correction_init.py",
        executable_parallel="illumination_correction_compute.py",
        output_types=dict(illumination_correction=True),
    ),
    "cellpose_segmentation": dict(
        name="cellpose_segmentation",
        executable_parallel="cellpose_segmentation.py",
    ),
    "calculate_registration_compound": dict(
        name="calculate_registration_compound",
        executable_non_parallel="calculate_registration_init.py",
        executable_parallel="calculate_registration_compute.py",
    ),
    "find_registration_consensus": dict(
        name="find_registration_consensus",
        executable_non_parallel="find_registration_consensus.py",
    ),
    "apply_registration_to_image": dict(
        name="apply_registration_to_image",
        input_types=dict(registration=False),
        executable_parallel="apply_registration_to_image.py",
        output_types=dict(registration=True),
    ),
}


manifest = ManifestV2(
    manifest_version="2",
    task_list=[],
    has_args_schemas=False,
)
for key, value in TASK_LIST_DICT.items():
    t = TaskManifestV2(**value)
    manifest.task_list.append(t)

debug(manifest.dict())
with open("src/fractal_tasks_mock/__FRACTAL_MANIFEST__.json", "w") as f:
    json.dump(manifest.dict(), f, indent=2, sort_keys=True)
