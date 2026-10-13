from fractal_server.runner.v2.resume.plan import _outputs_reflected
from fractal_server.runner.v2.resume.plan import _task_output_from_files
from fractal_server.runner.v2.resume.plan import last_resumed_task_index
from fractal_server.runner.v2.resume.slurm_evidence import (
    collect_fractal_slurm_jobs,
)
from fractal_server.runner.v2.resume.slurm_evidence import parse_stdout_path
from fractal_server.runner.v2.task_interface import TaskOutput

ROOT = "/remote/jobs/proj_v2_0000001_wf_0000001_job_0000001_20261012_100000"
OTHER_ROOT = ROOT.replace("job_0000001", "job_0000011")


def test_last_resumed_task_index():
    boot = "2026-10-13T10:00:00.000001+00:00"
    assert last_resumed_task_index(None) is None
    assert last_resumed_task_index("some log") is None
    log = (
        "some log\n"
        f"RESUME boot={boot} k=2 C6_units_in_progress -> resume at x\n"
        "\n"
        f"RESUME boot={boot} k=13 C4_next_task_not_started -> resume at y\n"
    )
    assert last_resumed_task_index(log) == 13
    # Mark-only resumes do not relaunch the job
    log += f"\nRESUME boot={boot} C2_all_tasks_done -> mark_done at z\n"
    assert last_resumed_task_index(log) == 13
    assert last_resumed_task_index(f"x RESUME boot={boot} k=1 C4 -> ") is None


def test_parse_stdout_path():
    assert parse_stdout_path(f"{ROOT}/0_task/non_par-slurm-%j.out", ROOT) == (
        "0_task",
        "non_par",
    )
    assert parse_stdout_path(
        f"{ROOT}/1_t/par-000001-slurm-%j.out", f"{ROOT}/"
    ) == ("1_t", "par-000001")
    # Other fractal job, other files, or not a fractal job
    assert (
        parse_stdout_path(f"{OTHER_ROOT}/0_t/non_par-slurm-%j.out", ROOT)
        is None
    )
    assert parse_stdout_path(f"{ROOT}_x/0_t/non_par-slurm-%j.out", ROOT) is None
    assert parse_stdout_path(f"{ROOT}/0_t/non_par-slurm-%j.err", ROOT) is None
    assert parse_stdout_path(f"{ROOT}/non_par-slurm-%j.out", ROOT) is None
    assert parse_stdout_path(f"{ROOT}/0_t/x/p-slurm-%j.out", ROOT) is None
    assert parse_stdout_path("/home/user/slurm-%j.out", ROOT) is None
    assert parse_stdout_path(None, ROOT) is None


class _FakeClient:
    def list_accounting_jobs(self, *, start_time):
        return [
            {
                "job_id": 10,
                "stdout": f"{ROOT}/0_a/non_par-slurm-%j.out",
                "state": {"current": ["COMPLETED"]},
                "time": {"end": 1000},
            },
            {
                "job_id": 11,
                "stdout": f"{ROOT}/1_b/par-000000-slurm-%j.out",
                "state": {"current": ["RUNNING"]},
                "time": {"end": 0},
            },
            {
                "job_id": 12,
                "stdout": f"{OTHER_ROOT}/0_a/non_par-slurm-%j.out",
                "state": {"current": ["COMPLETED"]},
                "time": {"end": 1000},
            },
            {
                "job_id": 13,
                "stdout": f"{ROOT}/0_a/par-000000-slurm-%j.out",
                "state": {"current": ["COMPLETED"]},
                "time": {"end": 1005},
            },
        ]

    def list_jobs(self):
        return [
            {
                "job_id": 11,
                "standard_output": f"{ROOT}/1_b/par-000000-slurm-%j.out",
                "job_state": ["RUNNING"],
                "end_time": {"set": True, "number": 9999},
            },
            {
                "job_id": 10,
                "standard_output": f"{ROOT}/0_a/non_par-slurm-%j.out",
                "job_state": ["COMPLETED", "COMPLETING"],
                "end_time": {"set": True, "number": 1001},
            },
        ]


def test_collect_fractal_slurm_jobs():
    jobs = collect_fractal_slurm_jobs(
        client=_FakeClient(), job_root=ROOT, start_time=0
    )
    assert [
        (j.slurm_job_id, j.state, j.source, j.end_time, j.subfolder, j.prefix)
        for j in jobs
    ] == [
        ("10", "COMPLETED", "slurm", 1001, "0_a", "non_par"),
        ("11", "RUNNING", "slurm", None, "1_b", "par-000000"),
        ("13", "COMPLETED", "slurmdb", 1005, "0_a", "par-000000"),
    ]


def test_task_output_from_files():
    outputs = {
        "non_par-init-output.json": [True, {"parallelization_list": []}],
        "par-000000-0-output.json": [
            True,
            {
                "image_list_updates": [
                    {"zarr_url": "/z/a", "types": {"x": True}}
                ]
            },
        ],
        "par-000000-1-output.json": [False, {"exc_type_name": "E"}],
        "par-000001-2-output.json": [True, None],
    }
    out = _task_output_from_files(
        outputs=outputs, task_type="compound", unit_selection=["/z/a"]
    )
    assert [img.zarr_url for img in out.image_list_updates] == ["/z/a"]

    # No image changes: all input images are flagged as modified
    out = _task_output_from_files(
        outputs={"non_par-0-output.json": [True, None]},
        task_type="non_parallel",
        unit_selection=["/z/a", "/z/b"],
    )
    assert [img.zarr_url for img in out.image_list_updates] == [
        "/z/a",
        "/z/b",
    ]

    # All units failed
    out = _task_output_from_files(
        outputs={"par-000000-0-output.json": [False, {}]},
        task_type="parallel",
        unit_selection=["/z/a"],
    )
    assert out == TaskOutput()


def test_outputs_reflected():
    before = [
        dict(zarr_url="/z/a", attributes={"well": "A01"}, types={}),
        dict(zarr_url="/z/old", attributes={}, types={}),
    ]
    after = [
        dict(
            zarr_url="/z/a",
            attributes={"well": "A01", "done": True},
            types={"is_3D": False},
        ),
        dict(
            zarr_url="/z/a_mip",
            origin="/z/a",
            attributes={"well": "A01"},
            types={"is_3D": False},
        ),
    ]
    task_output = TaskOutput(
        image_list_updates=[
            dict(zarr_url="/z/a", attributes={"done": True}),
            dict(zarr_url="/z/a_mip", origin="/z/a"),
        ],
        image_list_removals=["/z/old"],
    )
    output_types = {"is_3D": False}
    assert not _outputs_reflected(
        task_output=task_output, images=before, output_types=output_types
    )
    assert _outputs_reflected(
        task_output=task_output, images=after, output_types=output_types
    )

    # Attributes set to `None` are dropped by the engine
    task_output = TaskOutput(
        image_list_updates=[dict(zarr_url="/z/a", attributes={"well": None})]
    )
    assert not _outputs_reflected(
        task_output=task_output, images=before, output_types={}
    )
    assert _outputs_reflected(
        task_output=task_output,
        images=[dict(zarr_url="/z/a", attributes={}, types={})],
        output_types={},
    )
