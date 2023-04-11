# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/job.py                                 |       50 |        4 |       10 |        0 |     87% |37-38, 68-69 |
| fractal\_server/app/api/v1/project.py                             |      261 |       70 |       57 |       12 |     70% |107, 111, 120, 144-145, 166-176, 179-187, 259, 281-282, 291-302, 337, 361, 378-379, 396-397, 415, 462, 485-486, 488, 512, 521, 537-543, 560-573, 597, 602, 614, 647-648, 658-663, 669-684, 689->690, 690->689, 692-696, 707 |
| fractal\_server/app/api/v1/task.py                                |      190 |       35 |       26 |        1 |     78% |189, 212-213, 240-256, 272-284, 297, 311-312, 332-341, 348, 362-368, 371 |
| fractal\_server/app/api/v1/workflow.py                            |      120 |       18 |       30 |        4 |     81% |62-63, 72, 76, 107-108, 152-153, 183-184, 187, 210, 251, 295, 333, 341, 369, 372 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       49 |        9 |        6 |        1 |     82% |26-30, 45, 82-84 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       32 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       34 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       22 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/workflow.py                            |       74 |        1 |       10 |        3 |     95% |78, 121->123, 179->182 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      107 |       12 |       12 |        4 |     87% |51-52, 59-60, 114, 132->138, 150, 225-232 |
| fractal\_server/app/runner/\_common.py                            |      160 |        6 |       38 |        5 |     94% |131, 181-182, 185->exit, 192, 351, 353 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       19 |        1 |        4 |        1 |     91% |       118 |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       24 |        2 |        6 |        2 |     87% |    57, 62 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       68 |        0 |       28 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       33 |        3 |       14 |        0 |     94% |     93-96 |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      189 |        9 |       68 |        8 |     93% |153-154, 285, 295, 301, 316-323, 394-395, 493->497, 497->502, 502->508 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       40 |        1 |       14 |        1 |     96% |        86 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      357 |       24 |      118 |       13 |     92% |124, 136, 202-203, 206-209, 213, 429, 523, 530, 751, 801->804, 820, 879, 896-902, 964-969, 972-979, 1034->1033 |
| fractal\_server/app/runner/common.py                              |      111 |       19 |       38 |       12 |     75% |121, 132, 137, 142, 145->148, 149, 162, 225-231, 238, 254, 261-272, 276, 299->301 |
| fractal\_server/app/security/\_\_init\_\_.py                      |       78 |       13 |        8 |        1 |     79% |175-176, 193-234 |
| fractal\_server/common/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_\_init\_\_.py                    |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/\_validators.py                    |       29 |        2 |       14 |        2 |     91% |    12, 47 |
| fractal\_server/common/schemas/applyworkflow.py                   |       33 |        3 |        0 |        0 |     91% |     70-72 |
| fractal\_server/common/schemas/manifest.py                        |       29 |        0 |        2 |        0 |    100% |           |
| fractal\_server/common/schemas/project.py                         |       52 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/state.py                           |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/task.py                            |       78 |        2 |        6 |        2 |     95% |  137, 141 |
| fractal\_server/common/schemas/user.py                            |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/common/schemas/workflow.py                        |       62 |        0 |        7 |        0 |    100% |           |
| fractal\_server/config.py                                         |      138 |       25 |       38 |       10 |     74% |139-157, 177-184, 185->exit, 197-199, 200->exit, 328-330, 332, 339, 355, 363-389 |
| fractal\_server/main.py                                           |       51 |        9 |        8 |        1 |     83% |64-65, 75, 110, 114-115, 161-167 |
| fractal\_server/syringe.py                                        |       29 |        2 |        2 |        0 |     94% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      165 |        7 |       52 |       11 |     92% |135, 136->143, 162, 169-171, 208->exit, 227->251, 233->exit, 236->exit, 242->exit, 302, 426->exit, 435 |
| fractal\_server/utils.py                                          |       45 |        4 |       12 |        1 |     88% |34->33, 55-59 |
|                                                         **TOTAL** | **2841** |  **281** |  **630** |   **95** | **87%** |           |


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