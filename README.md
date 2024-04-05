# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|-------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                           |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                    |       75 |        0 |       24 |        7 |     93% |36->35, 44->43, 52->51, 79->78, 108->107, 115->114, 128->127 |
| fractal\_server/app/models/\_\_init\_\_.py                                |        4 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                             |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                    |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/state.py                                       |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/\_\_init\_\_.py                             |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/dataset.py                                  |       29 |        0 |        2 |        1 |     97% |    70->69 |
| fractal\_server/app/models/v1/job.py                                      |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/project.py                                  |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/task.py                                     |       50 |        0 |       12 |        3 |     95% |57->56, 64->63, 68->67 |
| fractal\_server/app/models/v1/workflow.py                                 |       55 |        3 |       14 |        6 |     87% |62->61, 80, 87->86, 91->90, 128->127, 129, 132->131, 133 |
| fractal\_server/app/models/v2/\_\_init\_\_.py                             |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                  |       27 |        0 |        2 |        1 |     97% |    54->53 |
| fractal\_server/app/models/v2/job.py                                      |       31 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                  |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                     |       57 |        7 |       16 |        3 |     86% |46->45, 62-68, 71->70, 85, 87-93 |
| fractal\_server/app/models/v2/workflow.py                                 |       23 |        2 |        4 |        2 |     85% |38->37, 39, 42->41, 43 |
| fractal\_server/app/models/v2/workflowtask.py                             |       49 |        7 |       12 |        4 |     79% |53->52, 66, 73->72, 79-90 |
| fractal\_server/app/routes/\_\_init\_\_.py                                |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v1.py                                    |      181 |        0 |       94 |        8 |     97% |61->60, 99->98, 148->147, 201->200, 282->281, 311->307, 345->344, 374->370 |
| fractal\_server/app/routes/admin/v2.py                                    |      122 |        0 |       56 |        6 |     97% |56->55, 132->131, 161->157, 195->194, 224->220, 256->252 |
| fractal\_server/app/routes/api/\_\_init\_\_.py                            |        8 |        0 |        2 |        1 |     90% |    14->13 |
| fractal\_server/app/routes/api/v1/\_\_init\_\_.py                         |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_aux\_functions.py                     |      111 |        0 |       46 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/dataset.py                              |      216 |        0 |       72 |       12 |     96% |46->41, 71->67, 102->98, 126->122, 164->160, 236->231, 265->261, 291->287, 332->328, 366->362, 434->430, 531->530 |
| fractal\_server/app/routes/api/v1/job.py                                  |       80 |        0 |       24 |        6 |     94% |31->30, 55->51, 77->73, 111->107, 145->141, 173->169 |
| fractal\_server/app/routes/api/v1/project.py                              |      188 |        7 |       55 |        7 |     94% |54->53, 73->72, 94-99, 108->107, 124->123, 150->149, 248->243, 451 |
| fractal\_server/app/routes/api/v1/task.py                                 |       93 |        1 |       36 |        6 |     95% |31->30, 51->50, 69->68, 102, 116->113, 169->168 |
| fractal\_server/app/routes/api/v1/task\_collection.py                     |      113 |        5 |       20 |        4 |     93% |61->44, 94, 134-135, 201->200, 224-225 |
| fractal\_server/app/routes/api/v1/workflow.py                             |      130 |        0 |       44 |        9 |     95% |52->48, 78->73, 108->104, 129->125, 181->177, 233->229, 266->261, 295->293, 339->338 |
| fractal\_server/app/routes/api/v1/workflowtask.py                         |       64 |        1 |       24 |        6 |     92% |43->38, 83->79, 104->100, 133->136, 144, 160->156 |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                         |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                     |      135 |        9 |       60 |        9 |     90% |270, 315, 319, 355, 416, 426, 437, 451-452, 453->456 |
| fractal\_server/app/routes/api/v2/dataset.py                              |      130 |        0 |       42 |        7 |     96% |38->33, 63->59, 94->90, 118->114, 150->146, 199->198, 225->221 |
| fractal\_server/app/routes/api/v2/images.py                               |       87 |        3 |       42 |        8 |     91% |46->42, 81->76, 105, 125->exit, 130, 180->176, 193->exit, 197 |
| fractal\_server/app/routes/api/v2/job.py                                  |       79 |       31 |       24 |        8 |     54% |31->30, 39-51, 58->54, 67-73, 80->76, 101-105, 114->110, 123-137, 148->144, 166-167, 176->172, 187-200 |
| fractal\_server/app/routes/api/v2/project.py                              |      106 |        6 |       26 |        5 |     92% |33->32, 52->51, 73-78, 87->86, 103->102, 129->128 |
| fractal\_server/app/routes/api/v2/submit.py                               |       72 |        1 |       23 |        2 |     97% |43->38, 192 |
| fractal\_server/app/routes/api/v2/task.py                                 |       99 |        0 |       44 |        5 |     97% |31->30, 55->54, 73->72, 111->108, 193->192 |
| fractal\_server/app/routes/api/v2/task\_collection.py                     |      113 |        5 |       20 |        4 |     93% |61->44, 94, 134-135, 202->201, 225-226 |
| fractal\_server/app/routes/api/v2/workflow.py                             |      147 |       48 |       56 |        8 |     60% |42->38, 68->63, 96->92, 120->116, 175->171, 229->225, 238-267, 275->270, 289-380, 384->383 |
| fractal\_server/app/routes/api/v2/workflowtask.py                         |       97 |       16 |       54 |        8 |     77% |31->26, 59, 117->113, 138->134, 186-201, 209->212, 219-227, 243->239 |
| fractal\_server/app/routes/auth.py                                        |       64 |       12 |       16 |        4 |     75% |68->67, 78-79, 90->89, 98->97, 126-149 |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                   |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                |        9 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/runner/async\_wrap.py                                 |       12 |        0 |        4 |        2 |     88% |21->20, 22->24 |
| fractal\_server/app/runner/components.py                                  |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/exceptions.py                                  |       50 |        6 |       16 |        7 |     80% |97-99, 110, 115, 120, 123->126, 127 |
| fractal\_server/app/runner/executors/slurm/\_\_init\_\_.py                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_batching.py                  |       69 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/executors/slurm/\_check\_jobs\_status.py       |       24 |        1 |       10 |        2 |     91% |25->31, 62 |
| fractal\_server/app/runner/executors/slurm/\_executor\_wait\_thread.py    |       47 |        3 |       16 |        1 |     94% |93->exit, 124-127 |
| fractal\_server/app/runner/executors/slurm/\_slurm\_config.py             |      155 |        9 |       54 |        6 |     93% |163-164, 181->185, 309, 327, 333, 348-355, 435-436 |
| fractal\_server/app/runner/executors/slurm/\_subprocess\_run\_as\_user.py |       45 |        1 |       16 |        1 |     97% |        88 |
| fractal\_server/app/runner/executors/slurm/executor.py                    |      413 |       27 |      141 |       10 |     93% |163, 175, 492, 585, 594, 601, 831, 849-853, 875-885, 900, 967, 1046-1053, 1114->1113, 1179-1185 |
| fractal\_server/app/runner/filenames.py                                   |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/set\_start\_and\_last\_task\_index.py          |       15 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/task\_files.py                                 |       35 |        1 |        4 |        1 |     95% |        75 |
| fractal\_server/app/runner/v1/\_\_init\_\_.py                             |      158 |        2 |       29 |        2 |     98% |   98, 170 |
| fractal\_server/app/runner/v1/\_common.py                                 |      166 |        8 |       48 |        4 |     94% |96-97, 100->exit, 107, 296, 298, 430-432 |
| fractal\_server/app/runner/v1/\_local/\_\_init\_\_.py                     |       23 |        1 |        4 |        1 |     93% |       160 |
| fractal\_server/app/runner/v1/\_local/\_local\_config.py                  |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_local/\_submit\_setup.py                  |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_local/executor.py                         |       27 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_slurm/\_\_init\_\_.py                     |       88 |        9 |       36 |       13 |     82% |76, 81, 213->217, 237, 239->248, 244->248, 248->253, 253->259, 263->278, 266-273, 281, 283->289, 298-299 |
| fractal\_server/app/runner/v1/\_slurm/\_submit\_setup.py                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_slurm/get\_slurm\_config.py               |       64 |        7 |       30 |        4 |     84% |66->70, 93-98, 130, 137-141 |
| fractal\_server/app/runner/v1/common.py                                   |       34 |        1 |       10 |        1 |     95% |        28 |
| fractal\_server/app/runner/v1/handle\_failed\_job.py                      |       49 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_\_init\_\_.py                             |      164 |       50 |       41 |        7 |     65% |84, 86->exit, 90, 95-108, 122, 143, 275-333 |
| fractal\_server/app/runner/v2/\_local/\_\_init\_\_.py                     |       21 |        1 |        4 |        1 |     92% |       146 |
| fractal\_server/app/runner/v2/\_local/\_local\_config.py                  |       40 |        9 |       12 |        4 |     71% |93, 99, 101->104, 107-117 |
| fractal\_server/app/runner/v2/\_local/\_submit\_setup.py                  |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local/executor.py                         |       27 |        1 |        8 |        2 |     91% |78, 87->91 |
| fractal\_server/app/runner/v2/\_slurm/\_\_init\_\_.py                     |       25 |        2 |        6 |        2 |     87% |    66, 71 |
| fractal\_server/app/runner/v2/\_slurm/\_submit\_setup.py                  |       11 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm/get\_slurm\_config.py               |       70 |       16 |       34 |        8 |     73% |69, 82->86, 109-114, 118-119, 123-125, 135-142, 146, 153-157 |
| fractal\_server/app/runner/v2/deduplicate\_list.py                        |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/handle\_failed\_job.py                      |       54 |        8 |       12 |        2 |     82% |77-93, 96->106 |
| fractal\_server/app/runner/v2/merge\_outputs.py                           |       22 |        1 |        8 |        2 |     90% |23, 29->32 |
| fractal\_server/app/runner/v2/runner.py                                   |      113 |        9 |       48 |       10 |     88% |68, 111-114, 135, 144, 177->182, 210->216, 217, 223, 228-229 |
| fractal\_server/app/runner/v2/runner\_functions.py                        |       99 |       18 |       22 |        3 |     81% |63-65, 74, 98-102, 243, 302-341 |
| fractal\_server/app/runner/v2/runner\_functions\_low\_level.py            |       59 |        6 |       22 |        5 |     86% |44-45, 47->exit, 54, 76, 99, 133 |
| fractal\_server/app/runner/v2/task\_interface.py                          |       29 |        5 |        4 |        1 |     76% |     22-28 |
| fractal\_server/app/runner/v2/v1\_compat.py                               |       10 |        6 |        0 |        0 |     40% |      8-21 |
| fractal\_server/app/schemas/\_\_init\_\_.py                               |        4 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                               |       46 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                                      |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user.py                                       |       49 |        0 |        8 |        2 |     96% |76->68, 122->121 |
| fractal\_server/app/schemas/v1/\_\_init\_\_.py                            |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/applyworkflow.py                           |       62 |        0 |       12 |        2 |     97% |75->74, 86->85 |
| fractal\_server/app/schemas/v1/dataset.py                                 |       52 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/dumps.py                                   |       40 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/manifest.py                                |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/v1/project.py                                 |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/task.py                                    |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/task\_collection.py                        |       42 |        0 |       12 |        2 |     96% |59->58, 73->72 |
| fractal\_server/app/schemas/v1/workflow.py                                |       67 |        0 |       11 |        2 |     97% |102->101, 168->167 |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                            |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                 |       43 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                   |       52 |        2 |        4 |        2 |     93% |58->57, 64-65 |
| fractal\_server/app/schemas/v2/job.py                                     |       60 |        1 |       12 |        3 |     94% |51->50, 62->61, 68 |
| fractal\_server/app/schemas/v2/manifest.py                                |       63 |        0 |       34 |        3 |     97% |54->53, 134->133, 156->155 |
| fractal\_server/app/schemas/v2/project.py                                 |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                    |       78 |        2 |        8 |        4 |     93% |37->36, 41, 94->93, 96 |
| fractal\_server/app/schemas/v2/task\_collection.py                        |       41 |        3 |       12 |        5 |     85% |54->53, 57, 62, 68->67, 73 |
| fractal\_server/app/schemas/v2/workflow.py                                |       40 |        1 |        7 |        2 |     94% |47->46, 49 |
| fractal\_server/app/schemas/v2/workflowtask.py                            |       70 |        0 |        4 |        1 |     99% |    89->88 |
| fractal\_server/app/security/\_\_init\_\_.py                              |      144 |       28 |       32 |        3 |     77% |114-127, 146-147, 152-161, 166-174, 188, 192, 316 |
| fractal\_server/config.py                                                 |      170 |        4 |       63 |       11 |     93% |76->75, 126->125, 204->203, 207, 216->exit, 228->227, 231, 235->exit, 276->275, 414-415, 420->exit |
| fractal\_server/images/\_\_init\_\_.py                                    |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                          |       28 |        0 |       12 |        2 |     95% |19->18, 40->39 |
| fractal\_server/images/tools.py                                           |       29 |        0 |       12 |        0 |    100% |           |
| fractal\_server/logger.py                                                 |       35 |        0 |        8 |        0 |    100% |           |
| fractal\_server/main.py                                                   |       33 |        6 |        2 |        1 |     80% |62-63, 73, 93->92, 99-107 |
| fractal\_server/syringe.py                                                |       29 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/endpoint\_operations.py                             |       76 |        0 |       26 |        4 |     96% |38->exit, 113->exit, 118->exit, 123->exit |
| fractal\_server/tasks/utils.py                                            |       46 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v1/\_TaskCollectPip.py                              |       43 |        0 |       24 |        3 |     96% |29->28, 33->32, 57->56 |
| fractal\_server/tasks/v1/background\_operations.py                        |      145 |        1 |       28 |        3 |     98% |90->exit, 121->exit, 143 |
| fractal\_server/tasks/v1/get\_collection\_data.py                         |       11 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_TaskCollectPip.py                              |       43 |        0 |       24 |        3 |     96% |29->28, 33->32, 57->56 |
| fractal\_server/tasks/v2/background\_operations.py                        |      158 |        2 |       38 |        5 |     96% |90->exit, 121->exit, 143, 226, 245->250 |
| fractal\_server/tasks/v2/get\_collection\_data.py                         |       11 |        0 |        2 |        0 |    100% |           |
| fractal\_server/utils.py                                                  |       22 |        0 |        2 |        0 |    100% |           |
|                                                                 **TOTAL** | **7406** |  **426** | **2132** |  **331** | **91%** |           |


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