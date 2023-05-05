# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/job.py                                 |       52 |        6 |       10 |        0 |     84% |37-38, 56, 69-70, 91 |
| fractal\_server/app/api/v1/project.py                             |      287 |       81 |       61 |       11 |     69% |108, 112, 121, 145, 147, 168-178, 182-192, 292-293, 302, 314-316, 335, 352-353, 379, 396, 398, 415, 417, 436, 457, 485, 508-509, 511-512, 536, 546, 562-569, 586-600, 624, 629, 642, 675-676, 686-691, 693-699, 705-720, 725->726, 726->725, 728-732, 743-744 |
| fractal\_server/app/api/v1/task.py                                |      202 |       39 |       26 |        1 |     77% |196, 219-221, 249-265, 267, 283, 285-296, 298, 311, 314, 326-328, 348-357, 365, 379-385, 389 |
| fractal\_server/app/api/v1/workflow.py                            |      125 |       20 |       30 |        4 |     81% |62-63, 72, 76, 107-108, 152-153, 183-184, 188, 211, 253, 297-298, 336, 345, 373, 376, 395 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       51 |        9 |        6 |        1 |     82% |30-34, 49, 84-86 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       35 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       22 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/workflow.py                            |       74 |        1 |       10 |        3 |     95% |78, 121->123, 179->182 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      123 |       15 |       20 |        7 |     85% |50-51, 58-59, 113, 116, 119, 122, 140->146, 158, 250-257 |
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
| fractal\_server/app/runner/\_slurm/executor.py                    |      368 |       24 |      118 |       13 |     92% |128, 140, 208-209, 212-215, 219, 436, 531, 538, 761, 811->814, 830, 890, 907-913, 976-981, 984-991, 1049->1048 |
| fractal\_server/app/runner/common.py                              |      111 |       19 |       38 |       12 |     75% |121, 132, 137, 142, 145->148, 149, 162, 225-231, 238, 254, 261-272, 276, 299->301 |
| fractal\_server/app/security/\_\_init\_\_.py                      |       79 |       13 |        8 |        1 |     79% |175, 177, 194-235 |
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
| fractal\_server/config.py                                         |      135 |       25 |       34 |        9 |     74% |140-158, 178-185, 186->exit, 200-202, 203->exit, 318-320, 322, 329, 341-367 |
| fractal\_server/logger.py                                         |       50 |        0 |       12 |        0 |    100% |           |
| fractal\_server/main.py                                           |       57 |       12 |       12 |        2 |     77% |68-69, 79, 121, 123, 127-134, 185-191 |
| fractal\_server/syringe.py                                        |       29 |        2 |        2 |        0 |     94% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      165 |        9 |       52 |       12 |     90% |81-82, 135, 136->143, 162, 169-171, 207->exit, 226->250, 232->exit, 235->exit, 241->exit, 301, 425->exit, 434 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3023** |  **306** |  **662** |   **98** | **87%** |           |


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