# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                            |       70 |       12 |       22 |        6 |     80% |31->30, 34-36, 39->38, 42-44, 47->46, 99->98, 106->105, 112-114, 119->118, 125-127 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/dataset.py                             |       25 |        0 |        4 |        1 |     97% |    62->61 |
| fractal\_server/app/models/job.py                                 |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       37 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       48 |        0 |       12 |        3 |     95% |52->51, 59->58, 63->62 |
| fractal\_server/app/models/workflow.py                            |       75 |        3 |       26 |        7 |     90% |60->59, 78, 85->84, 89->88, 168->171, 174->173, 175, 178->177, 179 |
| fractal\_server/app/routes/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin.py                               |      132 |        0 |       66 |        7 |     96% |40->39, 70->69, 111->110, 156->155, 230->226, 264->263, 293->289 |
| fractal\_server/app/routes/api/\_\_init\_\_.py                    |        8 |        0 |        2 |        1 |     90% |    14->13 |
| fractal\_server/app/routes/api/v1/\_\_init\_\_.py                 |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_aux\_functions.py             |       82 |        0 |       32 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/dataset.py                      |      204 |        0 |       68 |       12 |     96% |45->40, 70->66, 95->91, 119->115, 157->153, 229->224, 258->254, 284->280, 325->321, 359->355, 426->422, 517->516 |
| fractal\_server/app/routes/api/v1/job.py                          |       64 |        0 |       12 |        6 |     92% |30->29, 50->46, 72->68, 98->94, 132->128, 155->151 |
| fractal\_server/app/routes/api/v1/project.py                      |      182 |        6 |       56 |        6 |     95% |49->48, 68->67, 89-94, 103->102, 119->118, 145->144, 243->238 |
| fractal\_server/app/routes/api/v1/task.py                         |       85 |        1 |       32 |        6 |     94% |30->29, 45->44, 63->62, 96, 108->107, 154->153 |
| fractal\_server/app/routes/api/v1/task\_collection.py             |      178 |        5 |       30 |        4 |     96% |172->155, 205, 245-246, 312->311, 335-336 |
| fractal\_server/app/routes/api/v1/workflow.py                     |      129 |        0 |       50 |        9 |     95% |51->47, 77->72, 107->103, 128->124, 180->176, 232->228, 265->260, 294->292, 337->336 |
| fractal\_server/app/routes/api/v1/workflowtask.py                 |       63 |        1 |       24 |        6 |     92% |42->37, 81->77, 102->98, 131->134, 142, 158->154 |
| fractal\_server/app/routes/auth.py                                |       64 |       12 |       18 |        4 |     76% |68->67, 78-79, 90->89, 98->97, 126-149 |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                    |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                           |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                        |        9 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      164 |        5 |       25 |        1 |     97% |53-54, 61-62, 182 |
| fractal\_server/app/runner/\_common.py                            |      181 |        6 |       48 |        6 |     95% |116, 127->126, 175-176, 179->exit, 186, 376, 378 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       23 |        1 |        4 |        1 |     93% |       155 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       26 |        2 |        6 |        2 |     88% |    63, 68 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       47 |        3 |       18 |        1 |     94% |94->exit, 125-128 |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      207 |        9 |       82 |        8 |     94% |164-165, 182->186, 310, 328, 334, 349-356, 436-437, 512->516, 543->547 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       34 |        1 |       12 |        1 |     96% |        88 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      389 |       30 |      131 |       10 |     92% |128, 140, 214, 437, 531, 538, 760, 778-782, 804-814, 829, 888, 973-978, 981-988, 1049->1048, 1114-1120 |
| fractal\_server/app/runner/common.py                              |      110 |        7 |       42 |       10 |     89% |118-120, 131, 136, 141, 144->147, 148, 161, 244->243, 245->247 |
| fractal\_server/app/runner/handle\_failed\_job.py                 |       49 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                       |       38 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                       |       38 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/schemas/applyworkflow.py                      |       60 |        0 |       12 |        2 |     97% |78->77, 89->88 |
| fractal\_server/app/schemas/dataset.py                            |       48 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/dumps.py                              |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/manifest.py                           |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/project.py                            |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                              |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task.py                               |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task\_collection.py                   |       44 |        0 |       12 |        2 |     96% |67->66, 81->80 |
| fractal\_server/app/schemas/user.py                               |       44 |        0 |        4 |        1 |     98% |    76->68 |
| fractal\_server/app/schemas/workflow.py                           |       63 |        0 |       11 |        2 |     97% |100->99, 161->160 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      113 |       27 |       18 |        2 |     70% |106-119, 138-139, 144-153, 158-166, 180, 184 |
| fractal\_server/config.py                                         |      163 |        4 |       67 |       11 |     93% |76->75, 126->125, 204->203, 207, 216->exit, 228->227, 231, 235->exit, 276->275, 398-399, 404->exit |
| fractal\_server/logger.py                                         |       50 |       11 |       14 |        0 |     80% |   138-153 |
| fractal\_server/main.py                                           |       66 |       13 |       14 |        3 |     78% |72-73, 83, 126-130, 141, 145-152, 192->191, 198-206 |
| fractal\_server/syringe.py                                        |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      205 |        6 |       76 |       14 |     93% |63, 85-86, 132->131, 136->135, 147, 160->159, 239->exit, 293, 311->exit, 316->exit, 321->exit, 513->exit, 544->exit, 553 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **4098** |  **169** | **1162** |  **161** | **93%** |           |


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