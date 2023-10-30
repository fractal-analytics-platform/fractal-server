# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       23 |        0 |        2 |        1 |     96% |    32->31 |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_aux\_functions.py                    |       75 |        0 |       32 |        0 |    100% |           |
| fractal\_server/app/api/v1/dataset.py                             |      177 |        0 |       61 |       10 |     96% |44->39, 69->65, 93->89, 131->127, 177->172, 206->202, 232->228, 273->269, 304->300, 386->382 |
| fractal\_server/app/api/v1/job.py                                 |       63 |        0 |       16 |        4 |     95% |34->30, 60->56, 102->98, 124->120 |
| fractal\_server/app/api/v1/project.py                             |      128 |        6 |       34 |        6 |     93% |44->43, 63->62, 84-89, 98->97, 114->113, 140->139, 162->157 |
| fractal\_server/app/api/v1/task.py                                |       86 |        1 |       34 |        6 |     94% |30->29, 46->45, 64->63, 97, 109->108, 155->154 |
| fractal\_server/app/api/v1/task\_collection.py                    |      177 |        5 |       30 |        4 |     96% |171->154, 204, 244-245, 311->310, 334-335 |
| fractal\_server/app/api/v1/workflow.py                            |      114 |        0 |       44 |        8 |     95% |49->45, 72->67, 102->98, 123->119, 175->171, 212->208, 245->240, 274->272 |
| fractal\_server/app/api/v1/workflowtask.py                        |       63 |        1 |       24 |        6 |     92% |42->37, 81->77, 102->98, 131->134, 142, 158->154 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       65 |        8 |       22 |        6 |     84% |31->30, 32-36, 39->38, 47->46, 91->90, 98->97, 104-106, 111->110 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/dataset.py                             |       23 |        0 |        4 |        1 |     96% |    54->53 |
| fractal\_server/app/models/job.py                                 |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       48 |        0 |       12 |        3 |     95% |52->51, 59->58, 63->62 |
| fractal\_server/app/models/workflow.py                            |       74 |        3 |       26 |        7 |     90% |60->59, 78, 85->84, 89->88, 165->168, 171->170, 172, 175->174, 176 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      161 |        5 |       25 |        1 |     97% |53-54, 61-62, 182 |
| fractal\_server/app/runner/\_common.py                            |      181 |        6 |       48 |        6 |     95% |116, 127->126, 175-176, 179->exit, 186, 376, 378 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       23 |        1 |        4 |        1 |     93% |       155 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       26 |        2 |        6 |        2 |     88% |    63, 68 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       47 |        0 |       18 |        1 |     98% |  94->exit |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      207 |        9 |       82 |        8 |     94% |164-165, 182->186, 310, 328, 334, 349-356, 436-437, 512->516, 543->547 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       34 |        1 |       12 |        1 |     96% |        88 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      388 |       33 |      131 |       10 |     91% |128, 140, 214, 437, 531, 538, 753-762, 810->813, 826-831, 888, 905-911, 973-978, 981-988, 1045->1044, 1110-1116 |
| fractal\_server/app/runner/common.py                              |      110 |        6 |       42 |        9 |     90% |120, 131, 136, 141, 144->147, 148, 161, 244->243, 245->247 |
| fractal\_server/app/runner/handle\_failed\_job.py                 |       49 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                       |       35 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                       |       31 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/schemas/applyworkflow.py                      |       48 |        0 |       12 |        2 |     97% |46->45, 57->56 |
| fractal\_server/app/schemas/dataset.py                            |       46 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/manifest.py                           |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/project.py                            |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                              |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task.py                               |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task\_collection.py                   |       39 |        0 |        8 |        1 |     98% |    70->69 |
| fractal\_server/app/schemas/user.py                               |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/workflow.py                           |       61 |        0 |       11 |        2 |     97% |100->99, 159->158 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      147 |       33 |       26 |        3 |     71% |113-126, 151-160, 165-173, 283->282, 293->292, 321-344 |
| fractal\_server/config.py                                         |      162 |        4 |       67 |       11 |     93% |71->70, 121->120, 198->197, 201, 210->exit, 222->221, 225, 229->exit, 270->269, 392-393, 398->exit |
| fractal\_server/logger.py                                         |       50 |       11 |       14 |        0 |     80% |   138-153 |
| fractal\_server/main.py                                           |       60 |       13 |       16 |        4 |     75% |68-69, 79, 122, 124, 126, 130-137, 177->176, 183-190 |
| fractal\_server/syringe.py                                        |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      205 |        6 |       76 |       14 |     93% |63, 85-86, 132->131, 136->135, 147, 160->159, 239->exit, 293, 311->exit, 316->exit, 321->exit, 513->exit, 544->exit, 553 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3704** |  **158** | **1039** |  **144** | **93%** |           |


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