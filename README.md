# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/job.py                                 |       50 |        4 |       10 |        0 |     87% |37-38, 68-69 |
| fractal\_server/app/api/v1/project.py                             |      262 |       70 |       57 |       12 |     70% |108, 112, 121, 145-146, 167-177, 180-188, 260, 282-283, 292-303, 338, 362, 379-380, 397-398, 416, 463, 486-487, 489, 513, 522, 538-544, 561-574, 598, 603, 615, 648-649, 659-664, 670-685, 690->691, 691->690, 693-697, 708 |
| fractal\_server/app/api/v1/task.py                                |      191 |       35 |       26 |        1 |     78% |190, 213-214, 241-257, 273-285, 298, 312-313, 333-342, 349, 363-369, 372 |
| fractal\_server/app/api/v1/workflow.py                            |      121 |       18 |       30 |        4 |     81% |63-64, 73, 77, 108-109, 153-154, 184-185, 188, 211, 252, 296, 334, 342, 370, 373 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       49 |        9 |        6 |        1 |     82% |26-30, 45, 82-84 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       32 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       37 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       29 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/workflow.py                            |       82 |        2 |       12 |        4 |     94% |82, 111, 126->128, 184->187 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      108 |       12 |       12 |        4 |     87% |52-53, 60-61, 115, 133->139, 151, 226-233 |
| fractal\_server/app/runner/\_common.py                            |      158 |       11 |       40 |        5 |     92% |120, 170-171, 174->exit, 181, 340, 342, 369-373 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       21 |        1 |        4 |        1 |     92% |       120 |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       75 |        8 |       18 |        3 |     86% |117->120, 129-132, 180-181, 234, 239 |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       34 |        1 |       12 |        1 |     96% |        86 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      254 |       21 |       66 |        7 |     91% |114-115, 118-121, 230-238, 314, 347, 600-604, 642, 652-658, 695->697 |
| fractal\_server/app/runner/\_slurm/wait\_thread.py                |       31 |        3 |       12 |        0 |     93% |     61-64 |
| fractal\_server/app/runner/common.py                              |      113 |       19 |       38 |       12 |     75% |123, 134, 139, 144, 147->150, 151, 164, 227-233, 240, 256, 263-274, 278, 301->303 |
| fractal\_server/app/security/\_\_init\_\_.py                      |       79 |       13 |        8 |        1 |     79% |176-177, 194-235 |
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
| fractal\_server/config.py                                         |      134 |       22 |       36 |        9 |     76% |139-157, 177-184, 185->exit, 197-199, 200->exit, 335-337, 339, 346, 359-374 |
| fractal\_server/main.py                                           |       51 |        9 |        8 |        1 |     83% |64-65, 75, 110, 114-115, 161-167 |
| fractal\_server/syringe.py                                        |       30 |        2 |        2 |        0 |     94% |     94-95 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      167 |        7 |       52 |       11 |     92% |137, 138->145, 164, 171-173, 210->exit, 229->253, 235->exit, 238->exit, 244->exit, 304, 428->exit, 437 |
| fractal\_server/utils.py                                          |       45 |        4 |       12 |        1 |     88% |34->33, 55-59 |
|                                                         **TOTAL** | **2538** |  **278** |  **492** |   **82** | **86%** |           |


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