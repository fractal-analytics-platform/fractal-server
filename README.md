# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                              |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------ | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                   |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/\_\_init\_\_.py                           |       23 |        0 |        2 |        1 |     96% |    32->31 |
| fractal\_server/app/api/v1/\_\_init\_\_.py                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/api/v1/\_aux\_functions.py                    |       69 |        0 |       30 |        0 |    100% |           |
| fractal\_server/app/api/v1/dataset.py                             |      175 |        0 |       59 |       10 |     96% |44->39, 69->65, 93->89, 124->120, 170->165, 199->195, 225->221, 266->262, 297->293, 379->375 |
| fractal\_server/app/api/v1/job.py                                 |       73 |        0 |       18 |        4 |     96% |36->32, 73->69, 115->111, 137->133 |
| fractal\_server/app/api/v1/project.py                             |      134 |        6 |       36 |        6 |     93% |44->43, 63->62, 94-99, 108->107, 124->123, 158->157, 180->175 |
| fractal\_server/app/api/v1/task.py                                |       86 |        1 |       34 |        6 |     94% |30->29, 46->45, 64->63, 97, 109->108, 155->154 |
| fractal\_server/app/api/v1/task\_collection.py                    |      168 |        5 |       26 |        4 |     95% |169->152, 202, 242-243, 293->292, 316-317 |
| fractal\_server/app/api/v1/workflow.py                            |      112 |        0 |       42 |        8 |     95% |49->45, 72->67, 102->98, 123->119, 169->165, 206->202, 239->234, 268->266 |
| fractal\_server/app/api/v1/workflowtask.py                        |       63 |        1 |       24 |        6 |     92% |42->37, 81->77, 102->98, 131->134, 142, 158->154 |
| fractal\_server/app/db/\_\_init\_\_.py                            |       58 |       11 |       18 |        7 |     76% |30->29, 31-35, 38->37, 46->45, 51, 59-60, 90->89, 96-98, 103->102 |
| fractal\_server/app/models/\_\_init\_\_.py                        |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/job.py                                 |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/project.py                             |       36 |        0 |        4 |        1 |     98% |    65->64 |
| fractal\_server/app/models/security.py                            |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                               |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/task.py                                |       48 |        0 |       12 |        3 |     95% |52->51, 59->58, 63->62 |
| fractal\_server/app/models/workflow.py                            |       79 |        3 |       26 |        7 |     90% |61->60, 79, 86->85, 90->89, 166->169, 172->171, 173, 176->175, 177 |
| fractal\_server/app/runner/\_\_init\_\_.py                        |      171 |        5 |       29 |        1 |     97% |52-53, 60-61, 181 |
| fractal\_server/app/runner/\_common.py                            |      189 |        6 |       42 |        7 |     94% |116, 127->126, 175-176, 179->exit, 186, 375, 377, 512->527 |
| fractal\_server/app/runner/\_local/\_\_init\_\_.py                |       23 |        1 |        4 |        1 |     93% |       150 |
| fractal\_server/app/runner/\_local/\_local\_config.py             |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/\_submit\_setup.py             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_local/executor.py                    |       27 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_\_init\_\_.py                |       26 |        2 |        6 |        2 |     88% |    61, 66 |
| fractal\_server/app/runner/\_slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/\_slurm/\_executor\_wait\_thread.py    |       52 |        0 |       22 |        1 |     99% |  98->exit |
| fractal\_server/app/runner/\_slurm/\_slurm\_config.py             |      209 |        9 |       84 |        9 |     94% |165-166, 183->187, 311, 329, 335, 350-357, 437-438, 466->465, 514->518, 545->549 |
| fractal\_server/app/runner/\_slurm/\_submit\_setup.py             |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/\_slurm/\_subprocess\_run\_as\_user.py |       47 |        1 |       24 |        6 |     90% |30->29, 79->78, 92, 99->98, 122->121, 138->137 |
| fractal\_server/app/runner/\_slurm/executor.py                    |      391 |       40 |      131 |        9 |     90% |128, 140, 214, 437, 531, 538, 760-769, 811-821, 833-838, 895, 912-918, 980-985, 988-995, 1052->1051, 1117-1123 |
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
| fractal\_server/config.py                                         |      158 |       28 |       54 |       14 |     74% |71->70, 72-78, 115->114, 124-125, 132->131, 159-168, 209->208, 211-218, 219->exit, 231->230, 233-235, 236->exit, 352, 383, 395-421 |
| fractal\_server/logger.py                                         |       50 |        0 |       14 |        1 |     98% |  139->138 |
| fractal\_server/main.py                                           |       60 |       13 |       16 |        4 |     75% |68-69, 79, 122, 124, 126, 130-137, 177->176, 183-190 |
| fractal\_server/syringe.py                                        |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/collection.py                               |      203 |        5 |       76 |       13 |     94% |81-82, 128->127, 132->131, 143, 156->155, 235->exit, 289, 307->exit, 312->exit, 317->exit, 509->exit, 540->exit, 549 |
| fractal\_server/utils.py                                          |       22 |        0 |        2 |        0 |    100% |           |
|                                                         **TOTAL** | **3714** |  **180** | **1032** |  **154** | **92%** |           |


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