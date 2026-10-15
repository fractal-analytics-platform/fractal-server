import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

from asgi_lifespan import LifespanManager
from fastapi import FastAPI

from fractal_server.app.routes.admin.v2 import resume as resume_module
from fractal_server.app.routes.admin.v2 import router_admin
from fractal_server.runner.v2.resume.actions import ResumeError
from fractal_server.runner.v2.resume.plan import ResumeAction
from fractal_server.runner.v2.resume.plan import ResumePlan
from fractal_server.utils import get_timestamp


class _FakeSSHList:
    def get(self, *, host, user, key_path):
        return f"ssh:{user}@{host}"


def _fake_app() -> SimpleNamespace:
    return SimpleNamespace(
        state=SimpleNamespace(jobs=[], fractal_ssh_list=_FakeSSHList())
    )


async def test_resume_orphans_on_startup(monkeypatch):
    actions = {
        1: ResumeAction.RESUME,
        2: ResumeAction.REFUSE,
        3: ResumeAction.MARK_DONE,
        4: ResumeAction.RESUME,
        6: ResumeAction.NONE,
    }

    def fake_plan_resume(*, job_id, fractal_ssh_list, started_before):
        if job_id == 5:
            raise RuntimeError("SSH down")
        return ResumePlan(
            job_id=job_id,
            job_status="submitted",
            start_timestamp=get_timestamp(),
            action=actions[job_id],
        )

    applied = []

    def fake_apply_resume_plan(plan):
        applied.append(plan.job_id)
        if plan.job_id == 4:
            raise ResumeError("Job was already resumed by this server.")
        if plan.action != ResumeAction.RESUME:
            return None
        return SimpleNamespace(
            workflow_id=10,
            dataset_id=20,
            job_id=plan.job_id,
            user_id=30,
            worker_init=None,
            user_cache_dir="/cache",
            resource=SimpleNamespace(host="host"),
            profile=SimpleNamespace(username="user", ssh_key_path="/key"),
            resume="spec",
        )

    submitted = []
    monkeypatch.setattr(
        resume_module,
        "list_orphan_candidates",
        lambda *, started_before: [1, 2, 3, 4, 5, 6],
    )
    monkeypatch.setattr(resume_module, "plan_resume", fake_plan_resume)
    monkeypatch.setattr(
        resume_module, "apply_resume_plan", fake_apply_resume_plan
    )
    monkeypatch.setattr(
        resume_module,
        "submit_workflow",
        lambda **kwargs: submitted.append(kwargs),
    )

    app = _fake_app()
    await resume_module.resume_orphans_on_startup(app)
    await asyncio.gather(*resume_module._startup_tasks)

    # Refused, failing and no-op plans are not applied; errors do not stop
    # the loop
    assert applied == [1, 3, 4]
    assert app.state.jobs == [1]
    assert len(submitted) == 1
    assert submitted[0]["job_id"] == 1
    assert submitted[0]["fractal_ssh"] == "ssh:user@host"
    assert submitted[0]["resume"] == "spec"


async def test_resume_orphans_on_startup_listing_fails(monkeypatch):
    def fail(*, started_before):
        raise RuntimeError("DB down")

    monkeypatch.setattr(resume_module, "list_orphan_candidates", fail)
    await resume_module.resume_orphans_on_startup(_fake_app())


async def test_startup_hook_in_app_lifespan(monkeypatch, tmp_path):
    monkeypatch.chdir(tmp_path)
    called = []

    async def fake_hook(app):
        called.append(app)

    monkeypatch.setattr(resume_module, "resume_orphans_on_startup", fake_hook)

    @asynccontextmanager
    async def app_lifespan(app):
        app.state.jobs = []
        app.state.fractal_ssh_list = _FakeSSHList()
        yield

    for enabled, expected_calls in (("false", 0), ("true", 1)):
        monkeypatch.setenv("FRACTAL_RESUME_ON_STARTUP", enabled)
        app = FastAPI(lifespan=app_lifespan)
        app.include_router(router_admin, prefix="/admin/v2")
        async with LifespanManager(app):
            await asyncio.sleep(0)
        assert len(called) == expected_calls
