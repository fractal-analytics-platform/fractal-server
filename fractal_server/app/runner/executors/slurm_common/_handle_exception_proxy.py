from fractal_server.app.runner.exceptions import JobExecutionError
from fractal_server.app.runner.exceptions import TaskExecutionError


def _handle_exception_proxy(proxy):  # FIXME
    if proxy.exc_type_name == "JobExecutionError":
        return JobExecutionError(str(proxy))
    else:
        kwargs = {}
        for key in [
            "workflow_task_id",
            "workflow_task_order",
            "task_name",
        ]:
            if key in proxy.kwargs.keys():
                kwargs[key] = proxy.kwargs[key]
        return TaskExecutionError(proxy.tb, **kwargs)
