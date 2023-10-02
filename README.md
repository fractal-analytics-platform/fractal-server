# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       23 |        0 |        2 |        1 |     96% |    32->31 |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_aux\_functions.py                    |       75 |        0 |       32 |        0 |    100% |           |
| fractal\_server/app/api/v1/dataset.py                             |      175 |        0 |       59 |       10 |     96% |44->39, 69->65, 93->89, 124->120, 170->165, 199->195, 225->221, 266->262, 297->293, 379->375 |
| fractal\_server/app/api/v1/job.py                                 |       73 |        0 |       18 |        4 |     96% |36->32, 73->69, 115->111, 137->133 |
| fractal\_server/app/api/v1/project.py                             |      130 |        6 |       34 |        6 |     93% |45->44, 64->63, 86-91, 100->99, 116->115, 142->141, 164->159 |
| fractal\_server/app/api/v1/task.py                                |       86 |        1 |       34 |        6 |     94% |30->29, 46->45, 64->63, 97, 109->108, 155->154 |
| fractal\_server/app/api/v1/task\_collection.py                    |      177 |        5 |       30 |        4 |     96% |171->154, 204, 244-245, 311->310, 334-335 |
| fractal\_server/app/api/v1/workflow.py                            |      114 |        0 |       44 |        8 |     95% |49->45, 72->67, 102->98, 123->119, 175->171, 212->208, 245->240, 274->272 |
| fractal\_server/app/api/v1/workflowtask.py                        |       63 |        1 |       24 |        6 |     92% |42->37, 81->77, 102->98, 131->134, 142, 158->154 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       58 |       11 |       18 |        7 |     76% |30->29, 31-35, 38->37, 46->45, 51, 59-60, 90->89, 96-98, 103->102 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/dataset.py                             |       26 |        0 |        4 |        1 |     97% |    66->65 |
| fractal\_server/app/models/job.py                                 |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                            |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       48 |        0 |       12 |        3 |     95% |52->51, 59->58, 63->62 |
| fractal\_server/app/models/workflow.py                            |       79 |        3 |       26 |        7 |     90% |61->60, 79, 86->85, 90->89, 166->169, 172->171, 173, 176->175, 177 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      171 |        5 |       29 |        1 |     97% |52-53, 60-61, 181 |
| fractal\_server/app/runner/\_common.py                            |      193 |        6 |       46 |        6 |     95% |116, 127->126, 175-176, 179->exit, 186, 380, 382 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       23 |        1 |        4 |        1 |     93% |       150 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       26 |        2 |        6 |        2 |     88% |    61, 66 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       52 |        3 |       22 |        1 |     95% |98->exit, 127-130 |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      209 |        9 |       84 |        9 |     94% |165-166, 183->187, 311, 329, 335, 350-357, 437-438, 466->465, 514->518, 545->549 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       47 |        1 |       24 |        6 |     90% |30->29, 79->78, 92, 99->98, 122->121, 138->137 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      391 |       43 |      131 |       10 |     89% |128, 140, 214, 437, 531, 538, 760-769, 785-789, 811-821, 833-838, 895, 912-918, 980-985, 988-995, 1052->1051, 1117-1123 |
| fractal\_server/app/runner/common.py                              |      109 |        6 |       42 |        9 |     90% |120, 131, 136, 141, 144->147, 148, 161, 240->239, 241->243 |
| fractal\_server/app/runner/handle\_failed\_job.py                 |       49 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                       |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                       |       31 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/schemas/applyworkflow.py                      |       49 |        0 |       12 |        2 |     97% |46->45, 57->56 |
| fractal\_server/app/schemas/dataset.py                            |       36 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/manifest.py                           |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/project.py                            |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                              |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task.py                               |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/task\_collection.py                   |       39 |        0 |        8 |        1 |     98% |    70->69 |
| fractal\_server/app/schemas/user.py                               |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/workflow.py                           |       56 |        0 |       11 |        2 |     97% |98->97, 157->156 |
| fractal\_server/app/security/\_\_init\_\_.py                      |      146 |       33 |       26 |        3 |     71% |112-125, 150-159, 164-172, 279->278, 289->288, 317-340 |
| fractal\_server/config.py                                         |      159 |       10 |       62 |       11 |     88% |71->70, 121->120, 198->197, 200-209, 210->exit, 222->221, 224-228, 229->exit, 270->269, 400-401, 406->exit |
| fractal\_server/logger.py                                         |       50 |        0 |       14 |        1 |     98% |  139->138 |
| fractal\_server/main.py                                           |       60 |       13 |       16 |        4 |     75% |68-69, 79, 122, 124, 126, 130-137, 177->176, 183-190 |
| fractal\_server/syringe.py                                        |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      205 |        5 |       76 |       13 |     94% |85-86, 132->131, 136->135, 147, 160->159, 239->exit, 293, 311->exit, 316->exit, 321->exit, 513->exit, 544->exit, 553 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3741** |  **168** | **1050** |  **151** | **93%** |           |


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