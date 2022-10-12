RUNNER_BACKEND = None
if RUNNER_BACKEND == "PARSL":
    from .parsl import auto_output_dataset
    from .parsl import submit_workflow
    from .parsl import validate_workflow_compatibility
else:

    def no_function(*args, **kwarsg):
        raise NotImplementedError(
            f"Runner backend {RUNNER_BACKEND} not implemented"
        )

    auto_output_dataset = no_function
    submit_workflow = no_function
    validate_workflow_compatibility = no_function
