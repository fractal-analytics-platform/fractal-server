# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_aux\_functions.py                    |       56 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/api/v1/dataset.py                             |       96 |        0 |        9 |        1 |     99% | 248->exit |
| fractal\_server/app/api/v1/job.py                                 |       56 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/api/v1/project.py                             |      113 |        8 |       14 |        0 |     94% |92-97, 234-235 |
| fractal\_server/app/api/v1/task.py                                |      209 |       10 |       28 |        3 |     95% |208, 231-233, 306-307, 338-340, 369 |
| fractal\_server/app/api/v1/workflow.py                            |      142 |        1 |       32 |        2 |     98% |247->244, 368 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       57 |       11 |        8 |        2 |     80% |31-35, 50, 58-59, 95-97 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       31 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       35 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       22 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/workflow.py                            |       74 |        1 |       10 |        3 |     95% |78, 121->123, 179->182 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      122 |       15 |       20 |        7 |     85% |49-50, 57-58, 112, 115, 118, 121, 139->145, 157, 249-256 |
| fractal\_server/app/runner/\_common.py                            |      146 |        6 |       30 |        5 |     94% |112, 162-163, 166->exit, 173, 332, 334 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       21 |        1 |        4 |        1 |     92% |       138 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       24 |        2 |        6 |        2 |     87% |    58, 63 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       33 |        3 |       14 |        0 |     94% |     93-96 |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      201 |        9 |       74 |        8 |     94% |165-166, 304, 322, 328, 343-350, 430-431, 534->538, 538->543, 543->549 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       47 |        1 |       14 |        1 |     97% |        92 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      361 |       19 |      112 |       11 |     94% |128, 140, 211, 428, 523, 530, 753, 803->806, 822, 882, 899-905, 968-973, 976-983, 1041->1040 |
| fractal\_server/app/runner/common.py                              |      102 |       14 |       34 |       11 |     79% |120, 131, 136, 141, 144->147, 148, 161, 203, 210-221, 225, 248->250 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      147 |       34 |       22 |        1 |     71% |111-124, 149-158, 163-171, 309-350 |
| fractal\_server/common/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_\_init\_\_.py                    |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_validators.py                    |       29 |        6 |       14 |        6 |     72% |12, 15, 29, 31, 47, 50 |
| fractal\_server/common/schemas/applyworkflow.py                   |       28 |        3 |        0 |        0 |     89% |     52-54 |
| fractal\_server/common/schemas/manifest.py                        |       29 |        1 |        2 |        1 |     94% |        95 |
| fractal\_server/common/schemas/project.py                         |       52 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/state.py                           |       15 |        3 |        0 |        0 |     80% |     32-34 |
| fractal\_server/common/schemas/task.py                            |       76 |        1 |        4 |        1 |     98% |       137 |
| fractal\_server/common/schemas/user.py                            |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/workflow.py                        |       58 |        1 |        7 |        1 |     97% |        94 |
| fractal\_server/config.py                                         |      135 |       25 |       34 |        9 |     74% |140-158, 178-185, 186->exit, 200-202, 203->exit, 318-320, 322, 329, 341-367 |
| fractal\_server/logger.py                                         |       50 |        0 |       12 |        0 |    100% |           |
| fractal\_server/main.py                                           |       57 |       12 |       12 |        2 |     77% |68-69, 79, 121, 123, 127-134, 185-191 |
| fractal\_server/syringe.py                                        |       29 |        2 |        2 |        0 |     94% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      166 |        9 |       52 |       11 |     91% |81-82, 135, 164, 171-173, 209->exit, 228->252, 234->exit, 237->exit, 243->exit, 303, 426->exit, 435 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3096** |  **200** |  **650** |   **90** | **91%** |           |


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