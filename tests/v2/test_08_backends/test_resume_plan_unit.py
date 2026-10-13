from fractal_server.runner.v2.resume.plan import last_resumed_task_index
from fractal_server.runner.v2.resume.slurm_evidence import (
    collect_fractal_slurm_jobs,
)
from fractal_server.runner.v2.resume.slurm_evidence import parse_stdout_path

ROOT = "/remote/jobs/proj_v2_0000001_wf_0000001_job_0000001_20261012_100000"
OTHER_ROOT = ROOT.replace("job_0000001", "job_0000011")


def test_last_resumed_task_index():
    boot = "2026-10-13T10:00:00.000001+00:00"
    assert last_resumed_task_index(None) is None
    assert last_resumed_task_index("some log") is None
    log = (
        "some log\n"
        f"RESUME boot={boot} k=2 C6_run_with_units -> resume at x\n"
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
