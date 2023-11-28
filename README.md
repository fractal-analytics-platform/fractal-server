# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                            |       70 |       14 |       22 |        6 |     78% |31->30, 32-36, 39->38, 42-44, 47->46, 91->90, 98->97, 104-106, 111->110, 117-119 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/dataset.py                             |       27 |        0 |        4 |        1 |     97% |    72->71 |
| fractal\_server/app/models/job.py                                 |       31 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       48 |        0 |       12 |        3 |     95% |52->51, 59->58, 63->62 |
| fractal\_server/app/models/workflow.py                            |       76 |        3 |       26 |        7 |     90% |61->60, 79, 86->85, 90->89, 169->172, 175->174, 176, 179->178, 180 |
| fractal\_server/app/routes/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin.py                               |       92 |        0 |       52 |        4 |     97% |31->30, 61->60, 102->101, 147->146 |
| fractal\_server/app/routes/api/\_\_init\_\_.py                    |        8 |        0 |        2 |        1 |     90% |    14->13 |
| fractal\_server/app/routes/api/v1/\_\_init\_\_.py                 |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_aux\_functions.py             |       80 |        0 |       32 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/dataset.py                      |      185 |        0 |       66 |       12 |     95% |44->39, 69->65, 87->83, 111->107, 149->145, 197->192, 226->222, 252->248, 293->289, 327->323, 394->390, 485->484 |
| fractal\_server/app/routes/api/v1/job.py                          |       69 |        0 |       22 |        6 |     93% |31->30, 49->45, 72->68, 98->94, 140->136, 158->154 |
| fractal\_server/app/routes/api/v1/project.py                      |      135 |        6 |       38 |        6 |     93% |45->44, 64->63, 85-90, 99->98, 115->114, 141->140, 181->176 |
| fractal\_server/app/routes/api/v1/task.py                         |       84 |        1 |       32 |        6 |     94% |29->28, 44->43, 62->61, 95, 107->106, 153->152 |
| fractal\_server/app/routes/api/v1/task\_collection.py             |      177 |        5 |       30 |        4 |     96% |171->154, 204, 244-245, 311->310, 334-335 |
| fractal\_server/app/routes/api/v1/workflow.py                     |      116 |        0 |       50 |        9 |     95% |50->46, 69->64, 99->95, 120->116, 172->168, 213->209, 246->241, 275->273, 318->317 |
| fractal\_server/app/routes/api/v1/workflowtask.py                 |       63 |        1 |       24 |        6 |     92% |42->37, 81->77, 102->98, 131->134, 142, 158->154 |
| fractal\_server/app/routes/auth.py                                |       66 |       10 |       18 |        4 |     79% |74->73, 96->95, 104->103, 132-155 |
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
| fractal\_server/app/runner/\_slurm/executor.py                    |      388 |       30 |      131 |       10 |     92% |128, 140, 214, 437, 531, 538, 760, 778-782, 804-814, 829, 888, 973-978, 981-988, 1045->1044, 1110-1116 |
| fractal\_server/app/runner/common.py                              |      110 |        6 |       42 |        9 |     90% |120, 131, 136, 141, 144->147, 148, 161, 244->243, 245->247 |
| fractal\_server/app/runner/handle\_failed\_job.py                 |       49 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                       |       36 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                       |       31 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/schemas/applyworkflow.py                      |       57 |        0 |       12 |        2 |     97% |72->71, 83->82 |
| fractal\_server/app/schemas/dataset.py                            |       46 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/manifest.py                           |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/project.py                            |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                              |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task.py                               |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task\_collection.py                   |       39 |        0 |        8 |        1 |     98% |    70->69 |
| fractal\_server/app/schemas/user.py                               |       36 |        0 |        4 |        1 |     98% |    65->57 |
| fractal\_server/app/schemas/workflow.py                           |       61 |        0 |       11 |        2 |     97% |100->99, 159->158 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      117 |       25 |       20 |        0 |     74% |108-121, 140-141, 146-155, 160-168 |
| fractal\_server/config.py                                         |      163 |        4 |       67 |       11 |     93% |76->75, 126->125, 204->203, 207, 216->exit, 228->227, 231, 235->exit, 276->275, 398-399, 404->exit |
| fractal\_server/logger.py                                         |       50 |       11 |       14 |        0 |     80% |   138-153 |
| fractal\_server/main.py                                           |       70 |       15 |       18 |        5 |     75% |72-73, 83, 126-130, 140, 142, 144, 148-155, 195->194, 201-208 |
| fractal\_server/syringe.py                                        |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      205 |        6 |       76 |       14 |     93% |63, 85-86, 132->131, 136->135, 147, 160->159, 239->exit, 293, 311->exit, 316->exit, 321->exit, 513->exit, 544->exit, 553 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3896** |  **168** | **1128** |  **156** | **93%** |           |


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