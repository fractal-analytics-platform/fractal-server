# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_aux\_functions.py                    |       69 |        0 |       30 |        0 |    100% |           |
| fractal\_server/app/api/v1/dataset.py                             |      103 |        0 |       11 |        0 |    100% |           |
| fractal\_server/app/api/v1/job.py                                 |       74 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/api/v1/project.py                             |      121 |        6 |       20 |        0 |     96% |     92-97 |
| fractal\_server/app/api/v1/task.py                                |      238 |        6 |       44 |        3 |     97% |211, 251-252, 325-326, 398 |
| fractal\_server/app/api/v1/workflow.py                            |      154 |        1 |       42 |        3 |     98% |272->270, 378->381, 389 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       57 |       11 |        8 |        2 |     80% |31-35, 50, 58-59, 95-97 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       32 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       32 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       36 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       45 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/models/workflow.py                            |       74 |        1 |       16 |        2 |     97% |78, 165->168 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      134 |       14 |       22 |        2 |     90% |49-50, 57-58, 161, 265-275 |
| fractal\_server/app/runner/\_common.py                            |      146 |        6 |       30 |        5 |     94% |113, 163-164, 167->exit, 174, 334, 336 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       21 |        1 |        4 |        1 |     92% |       138 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       24 |        2 |        6 |        2 |     87% |    58, 63 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       52 |        0 |       22 |        1 |     99% |  98->exit |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      201 |        9 |       74 |        8 |     94% |165-166, 304, 322, 328, 343-350, 430-431, 534->538, 538->543, 543->549 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       47 |        1 |       14 |        1 |     97% |        92 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      390 |       40 |      131 |        9 |     90% |127, 139, 213, 434, 528, 535, 757-766, 808-818, 830-835, 892, 909-915, 977-982, 985-992, 1049->1048, 1114-1120 |
| fractal\_server/app/runner/common.py                              |       93 |        7 |       28 |        9 |     87% |120, 131, 136, 141, 144->147, 148, 161, 215, 237->239 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      147 |       34 |       22 |        1 |     71% |111-124, 149-158, 163-171, 309-350 |
| fractal\_server/common/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_\_init\_\_.py                    |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_validators.py                    |       29 |        1 |       14 |        1 |     95% |        47 |
| fractal\_server/common/schemas/applyworkflow.py                   |       28 |        0 |        0 |        0 |    100% |           |
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
|                                                         **TOTAL** | **3331** |  **188** |  **759** |   **74** | **93%** |           |


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