# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                            |       76 |        0 |       24 |        7 |     93% |37->36, 45->44, 53->52, 80->79, 109->108, 116->115, 129->128 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/dataset.py                             |       29 |        0 |        4 |        1 |     97% |    70->69 |
| fractal\_server/app/models/job.py                                 |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       37 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       48 |        0 |       12 |        3 |     95% |52->51, 59->58, 63->62 |
| fractal\_server/app/models/workflow.py                            |       55 |        3 |       14 |        6 |     87% |62->61, 80, 87->86, 91->90, 128->127, 129, 132->131, 133 |
| fractal\_server/app/routes/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin.py                               |      181 |        0 |       94 |        8 |     97% |61->60, 99->98, 148->147, 201->200, 282->281, 311->307, 345->344, 374->370 |
| fractal\_server/app/routes/api/\_\_init\_\_.py                    |        8 |        0 |        2 |        1 |     90% |    14->13 |
| fractal\_server/app/routes/api/v1/\_\_init\_\_.py                 |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_aux\_functions.py             |      111 |        0 |       46 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/dataset.py                      |      213 |        0 |       76 |       12 |     96% |45->40, 70->66, 101->97, 125->121, 163->159, 235->230, 264->260, 290->286, 331->327, 365->361, 433->429, 524->523 |
| fractal\_server/app/routes/api/v1/job.py                          |       80 |        0 |       24 |        6 |     94% |31->30, 55->51, 77->73, 111->107, 145->141, 173->169 |
| fractal\_server/app/routes/api/v1/project.py                      |      188 |        6 |       59 |        6 |     95% |52->51, 71->70, 92-97, 106->105, 122->121, 148->147, 246->241 |
| fractal\_server/app/routes/api/v1/task.py                         |       88 |        1 |       36 |        6 |     94% |30->29, 50->49, 68->67, 101, 113->112, 159->158 |
| fractal\_server/app/routes/api/v1/task\_collection.py             |      113 |        5 |       20 |        4 |     93% |59->42, 92, 132-133, 199->198, 222-223 |
| fractal\_server/app/routes/api/v1/workflow.py                     |      130 |        0 |       50 |        9 |     95% |52->48, 78->73, 108->104, 129->125, 181->177, 233->229, 266->261, 295->293, 339->338 |
| fractal\_server/app/routes/api/v1/workflowtask.py                 |       64 |        1 |       24 |        6 |     92% |43->38, 83->79, 104->100, 133->136, 144, 160->156 |
| fractal\_server/app/routes/auth.py                                |       64 |       12 |       18 |        4 |     76% |68->67, 78-79, 90->89, 98->97, 126-149 |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                           |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                        |        9 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      165 |        5 |       25 |        1 |     97% |54-55, 62-63, 183 |
| fractal\_server/app/runner/\_common.py                            |      182 |        6 |       48 |        6 |     95% |117, 128->127, 176-177, 180->exit, 187, 376, 378 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       23 |        1 |        4 |        1 |     93% |       160 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       26 |        2 |        6 |        2 |     88% |    64, 69 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       47 |        3 |       18 |        1 |     94% |94->exit, 125-128 |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      212 |        9 |       84 |        8 |     94% |164-165, 182->186, 310, 328, 334, 349-356, 436-437, 512->516, 543->547 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       34 |        1 |       12 |        1 |     96% |        88 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      402 |       26 |      135 |        9 |     93% |161, 173, 490, 588, 595, 817, 835-839, 861-871, 886, 945, 1025-1032, 1093->1092, 1158-1164 |
| fractal\_server/app/runner/common.py                              |      110 |        7 |       42 |       10 |     89% |118-120, 131, 136, 141, 144->147, 148, 161, 244->243, 245->247 |
| fractal\_server/app/runner/handle\_failed\_job.py                 |       49 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                       |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                       |       46 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/schemas/applyworkflow.py                      |       62 |        0 |       12 |        2 |     97% |75->74, 86->85 |
| fractal\_server/app/schemas/dataset.py                            |       52 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/dumps.py                              |       40 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/manifest.py                           |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/project.py                            |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                              |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task.py                               |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task\_collection.py                   |       42 |        0 |       12 |        2 |     96% |59->58, 73->72 |
| fractal\_server/app/schemas/user.py                               |       49 |        0 |        8 |        2 |     96% |76->68, 122->121 |
| fractal\_server/app/schemas/workflow.py                           |       67 |        0 |       11 |        2 |     97% |102->101, 168->167 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      144 |       28 |       32 |        3 |     77% |114-127, 146-147, 152-161, 166-174, 188, 192, 316 |
| fractal\_server/config.py                                         |      172 |        6 |       67 |       11 |     92% |76->75, 126->125, 204->203, 207, 216->exit, 228->227, 231, 235->exit, 276->275, 387-388, 411-412, 417->exit |
| fractal\_server/logger.py                                         |       35 |        0 |       12 |        0 |    100% |           |
| fractal\_server/main.py                                           |       29 |        6 |        2 |        1 |     77% |54-55, 65, 85->84, 91-99 |
| fractal\_server/syringe.py                                        |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_TaskCollectPip.py                         |       43 |        0 |       24 |        3 |     96% |29->28, 33->32, 57->56 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/background\_operations.py                   |      155 |        1 |       32 |        3 |     98% |124->exit, 155->exit, 177 |
| fractal\_server/tasks/endpoint\_operations.py                     |       80 |        0 |       26 |        4 |     96% |47->exit, 119->exit, 124->exit, 129->exit |
| fractal\_server/tasks/utils.py                                    |       35 |        0 |        6 |        0 |    100% |           |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **4281** |  **133** | **1235** |  **157** | **94%** |           |


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