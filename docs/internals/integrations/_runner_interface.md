# Fractal runner interface

This page describes how `fractal-server` runs Fractal tasks and processes the metadata they produce.

> NOTE: The process of defining a single full specification for this interface is still ongoing.

The description below is based on concepts and definitions which are part of `fractal-server`. For the specific case of the **Fractal image list**, a more detailed description is available at https://fractal-analytics-platform.github.io/image_list. For clarifications about other terms or definitions, the starting point is the [`execute_tasks` function in the `runner.py` Python module](../../../reference/runner/v2/runner/#fractal_server.runner.v2.runner.execute_tasks).

Within `fractal-server`, a Fractal task is associated to a [`TaskV2` object](../../../reference/app/models/v2/task#fractal_server.app.models.v2.task.TaskV2), which has either one or both _non-parallel_ and _parallel_ components (where "both" corresponds to compound tasks).
The `command_non_parallel` and `command_parallel` attributes, when set, represent a command-line executables which are used to run the task. As an example, if `command_non_parallel = "/path/to/python /path/to/my_task.py`, then the command that is executed will look like
```bash
/path/to/python /path/to/my_task.py --args-json /path/to/args.json --out-json /path/to/out.json
```
For Fractal tasks that are developed in Python, the `fractal-task-tools` exposes [a helper tool to implement this command-line interface](https://fractal-analytics-platform.github.io/fractal-task-tools/usage/run_task).

The main entrypoint for task execution in `fractal-server` is the `execute_tasks` function. Its input arguments include:

* a Fractal dataset (which also contains an [image list](https://fractal-analytics-platform.github.io/image_list)),
* a list of workflow tasks (each one associated to a [`TaskV2` object](../../../reference/app/models/v2/task#fractal_server.app.models.v2.task.TaskV2)),
* filters based on image types or attributes, set by the user upon job submission.

In the following parts of this page we provide a high-level description of the `execute_tasks` flow. Some aspects which are not covered here are:

* Validation procedures and error handling.
* Fractal-job statuses and history tracking.
* Advanced status-based image filtering.


## Initialization phase

Before starting the execution of the tasks, `fractal-server` initializes some relevant variables.

* Variables that are extracted from the current dataset state:
    * `zarr_dir`
    * The current image list
* Variables that are extracted from user-provided job-submission parameters:
    * Image-type filters to apply to the image list.


After this preliminary phase the following three steps (pre-execution, execution, post-execution) are repeated for all tasks in the list.

## Pre-task-execution phase

If the task is a converter, it does not receive any OME-Zarr image as input.
For non-converter tasks, however, `fractal-server` prepares a list of images that will be part of either `zarr_urls` (for non-parallel or compound tasks) or of the individual `zarr_url` arguments (for parallel tasks).

The input image list is constructed by applying two sets of filters to the current dataset image list:

* Image-type filters obtained as a combination of current type filters, the task input types and the user-specified workflow-task type filters.
* Image-attribute filters specified by the user upon job submission.

This procedure leads to a `filtered_images` list, with all OME-Zarr images that should be used as input for the task.

## Task execution

Call one of `run_task_non_parallel`, `run_task_parallel` or `run_task_compound`, which returns a dictionary of task outcomes and their number.

Each computational unit optionally returns a [`TaskOutput` object](../../../reference/runner/v2/runner/#fractal_server.runner.v2.task_interface.TaskOutput).

FIXME

## Post-task-execution phase

* Metadata outputs from all units are merged into a single [`TaskOutput` object](../../../reference/runner/v2/runner/#fractal_server.runner.v2.task_interface.TaskOutput).
* If there are no images to be created or updated, all input images in `filtered_images` are flagged as "to be updated", so that they will be updated e.g. with the new types set by the task.
* For each image that should be created or updated, the image `attributes`, `types` and `origin` properties are updated as appropriate.
* All images marked as "to be removed" are removed from the image list.
* The current type filters are updated based on task output_types
* The existing dataset image list is replaced with the new one, in the database.
