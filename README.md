# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_aux\_functions.py                    |       69 |        0 |       30 |        0 |    100% |           |
| fractal\_server/app/api/v1/dataset.py                             |      103 |        0 |       11 |        0 |    100% |           |
| fractal\_server/app/api/v1/job.py                                 |       73 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/api/v1/project.py                             |      134 |        6 |       24 |        0 |     96% |     94-99 |
| fractal\_server/app/api/v1/task.py                                |       86 |        1 |       24 |        1 |     98% |        95 |
| fractal\_server/app/api/v1/task\_collection.py                    |      168 |        5 |       22 |        2 |     96% |202, 242-243, 316-317 |
| fractal\_server/app/api/v1/workflow.py                            |      112 |        0 |       28 |        1 |     99% |  268->266 |
| fractal\_server/app/api/v1/workflowtask.py                        |       63 |        1 |       16 |        2 |     96% |131->134, 142 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       57 |       11 |        8 |        2 |     80% |31-35, 50, 58-59, 95-97 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       32 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       36 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       45 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/models/workflow.py                            |       74 |        3 |       16 |        2 |     94% |78, 165->168, 172, 176 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      147 |        5 |       25 |        1 |     97% |49-50, 57-58, 181 |
| fractal\_server/app/runner/\_common.py                            |      150 |        6 |       34 |        5 |     94% |114, 173-174, 177->exit, 184, 344, 346 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       23 |        1 |        4 |        1 |     93% |       150 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       26 |        2 |        6 |        2 |     88% |    61, 66 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       52 |        3 |       22 |        1 |     95% |98->exit, 127-130 |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      209 |        9 |       82 |        8 |     94% |165-166, 183->187, 311, 329, 335, 350-357, 437-438, 514->518, 545->549 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       47 |        1 |       14 |        1 |     97% |        92 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      391 |       40 |      131 |        9 |     90% |128, 140, 214, 437, 531, 538, 760-769, 811-821, 833-838, 895, 912-918, 980-985, 988-995, 1052->1051, 1117-1123 |
| fractal\_server/app/runner/common.py                              |      109 |        6 |       40 |        8 |     91% |120, 131, 136, 141, 144->147, 148, 161, 241->243 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      147 |       34 |       22 |        1 |     71% |111-124, 149-158, 163-171, 309-350 |
| fractal\_server/common/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_\_init\_\_.py                    |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_validators.py                    |       29 |        1 |       14 |        1 |     95% |        47 |
| fractal\_server/common/schemas/applyworkflow.py                   |       49 |        0 |        8 |        0 |    100% |           |
| fractal\_server/common/schemas/manifest.py                        |       38 |        0 |        8 |        0 |    100% |           |
| fractal\_server/common/schemas/project.py                         |       50 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/state.py                           |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/task.py                            |       55 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/task\_collection.py                |       39 |        1 |        6 |        1 |     96% |        74 |
| fractal\_server/common/schemas/user.py                            |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/workflow.py                        |       56 |        0 |        7 |        0 |    100% |           |
| fractal\_server/config.py                                         |      137 |       25 |       34 |        9 |     74% |140-158, 178-185, 186->exit, 200-202, 203->exit, 331-333, 335, 342, 354-380 |
| fractal\_server/logger.py                                         |       50 |        0 |       12 |        0 |    100% |           |
| fractal\_server/main.py                                           |       60 |       13 |       14 |        3 |     76% |68-69, 79, 122, 124, 126, 130-137, 183-190 |
| fractal\_server/syringe.py                                        |       29 |        2 |        2 |        0 |     94% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      203 |        5 |       70 |       10 |     95% |81-82, 143, 235->exit, 289, 307->exit, 312->exit, 317->exit, 509->exit, 540->exit, 549 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3458** |  **183** |  **802** |   **72** | **93%** |           |


## Setup coverage badge

Below are examples of the badges you can use in your main branch `README` file.

### Direct image

[![Coverage badge](https://raw.githubusercontent.com/fractal-analytics-platform/fractal-server/python-coverage-comment-action-data/badge.svg)](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

This is the one to use if your repository is private or if you don't want to customize anything.

### [Shields.io](https://shields.io) Json Endpoint

[![Coverage badge](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/fractal-analytics-platform/fractal-server/python-coverage-comment-action-data/endpoint.json)](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

Using this one will allow you to [customize](https://shields.io/endpoint) the look of your badge.
It won't work with private repositories. It won't be refreshed more than once per five minutes.

### [Shields.io](https://shields.io) Dynamic Badge

[![Coverage badge](https://img.shields.io/badge/dynamic/json?color=brightgreen&label=coverage&query=%24.message&url=https%3A%2F%2Fraw.githubusercontent.com%2Ffractal-analytics-platform%2Ffractal-server%2Fpython-coverage-comment-action-data%2Fendpoint.json)](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

This one will always be the same color. It won't work for private repos. I'm not even sure why we included it.

## What is that?

This branch is part of the
[python-coverage-comment-action](https://github.com/marketplace/actions/python-coverage-comment)
GitHub Action. All the files in this branch are automatically generated and may be
overwritten at any moment.