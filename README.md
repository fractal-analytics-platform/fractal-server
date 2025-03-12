# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                           |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                                |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                         |       60 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/\_\_init\_\_.py                                     |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkusergroup.py                                    |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                                  |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                         |       42 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/user\_settings.py                                   |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/\_\_init\_\_.py                                  |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/accounting.py                                    |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                       |       27 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/job.py                                           |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                       |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                          |       28 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task\_group.py                                   |       56 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflow.py                                      |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                                  |       22 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_\_init\_\_.py                            |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/accounting.py                              |       65 |        0 |       16 |        3 |     96% |98->100, 100->102, 102->105 |
| fractal\_server/app/routes/admin/v2/impersonate.py                             |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/job.py                                     |      101 |        0 |       36 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/project.py                                 |       22 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task.py                                    |       68 |        0 |       18 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group.py                             |      116 |        0 |       50 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group\_lifecycle.py                  |       93 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/\_\_init\_\_.py                                 |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                              |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                          |       99 |        1 |       32 |        1 |     98% |       357 |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_lifecycle.py         |       70 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_tasks.py                   |      115 |        0 |       40 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/dataset.py                                   |      122 |        4 |       24 |        0 |     97% |   271-281 |
| fractal\_server/app/routes/api/v2/images.py                                    |      114 |        2 |       36 |        2 |     97% |  163, 232 |
| fractal\_server/app/routes/api/v2/job.py                                       |       82 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/project.py                                   |      111 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/status.py                                    |       74 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/submit.py                                    |      102 |        0 |       24 |        1 |     99% |  211->217 |
| fractal\_server/app/routes/api/v2/task.py                                      |       90 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_collection.py                          |      152 |        2 |       32 |        1 |     98% |233->243, 275-276 |
| fractal\_server/app/routes/api/v2/task\_collection\_custom.py                  |       65 |        0 |       12 |        1 |     99% |    67->93 |
| fractal\_server/app/routes/api/v2/task\_group.py                               |      108 |        0 |       36 |        1 |     99% |  233->237 |
| fractal\_server/app/routes/api/v2/task\_group\_lifecycle.py                    |       93 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow.py                                  |      141 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow\_import.py                          |      114 |        2 |       28 |        1 |     98% |   248-251 |
| fractal\_server/app/routes/api/v2/workflowtask.py                              |      107 |        2 |       42 |        3 |     97% |84->98, 291, 301 |
| fractal\_server/app/routes/auth/\_\_init\_\_.py                                |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/\_aux\_auth.py                                 |       60 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/auth/current\_user.py                               |       74 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/auth/group.py                                       |      114 |        0 |       24 |        1 |     99% |  129->134 |
| fractal\_server/app/routes/auth/login.py                                       |       10 |        0 |        4 |        1 |     93% |    24->23 |
| fractal\_server/app/routes/auth/oauth.py                                       |       21 |       12 |       10 |        2 |     35% |24-47, 62-63 |
| fractal\_server/app/routes/auth/register.py                                    |       11 |        0 |        4 |        1 |     93% |    22->21 |
| fractal\_server/app/routes/auth/router.py                                      |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/users.py                                       |      105 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                                 |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                        |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                     |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/validate\_user\_settings.py                     |       28 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/runner/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/components.py                                       |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/compress\_folder.py                                 |       57 |        2 |       10 |        2 |     94% |  126, 132 |
| fractal\_server/app/runner/exceptions.py                                       |       50 |        2 |       14 |        3 |     92% |99, 123->126, 127 |
| fractal\_server/app/runner/executors/\_\_init\_\_.py                           |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/\_job\_states.py                          |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_\_init\_\_.py                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_batching.py                       |       68 |       36 |       28 |        5 |     43% |50, 126-131, 133-138, 140-145, 150-199 |
| fractal\_server/app/runner/executors/slurm/\_slurm\_config.py                  |      159 |       34 |       52 |       12 |     72% |171-172, 189->193, 299-305, 325, 343, 348-349, 374, 383-384, 387-393, 439-440, 442, 446-447, 452-453, 455-463 |
| fractal\_server/app/runner/executors/slurm/ssh/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/ssh/\_executor\_wait\_thread.py     |       61 |        8 |       14 |        3 |     85% |75-78, 94-96, 112->exit, 117-118, 120->126, 124-125 |
| fractal\_server/app/runner/executors/slurm/ssh/\_slurm\_job.py                 |       35 |        3 |        4 |        2 |     87% |95, 107, 116 |
| fractal\_server/app/runner/executors/slurm/ssh/executor.py                     |      531 |      104 |      118 |       23 |     77% |121, 144, 380, 444, 490, 495, 504, 513, 550-561, 567, 704, 787-796, 840-853, 856-875, 887-898, 928->932, 941, 946-954, 972-1005, 1019-1052, 1054-1068, 1087-1088, 1108, 1153->1157, 1209->1208, 1236-1246, 1250-1253, 1300-1304, 1322-1331, 1371-1379 |
| fractal\_server/app/runner/executors/slurm/sudo/\_\_init\_\_.py                |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/sudo/\_check\_jobs\_status.py       |       24 |       11 |       10 |        1 |     47% |11-30, 54-61 |
| fractal\_server/app/runner/executors/slurm/sudo/\_executor\_wait\_thread.py    |       59 |        5 |       14 |        3 |     89% |88-91, 106->exit, 116->exit, 119-122 |
| fractal\_server/app/runner/executors/slurm/sudo/\_subprocess\_run\_as\_user.py |       46 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/sudo/executor.py                    |      453 |       62 |       98 |       17 |     85% |477, 579, 584, 593, 602, 637-648, 654, 793-794, 893-897, 912->907, 918-927, 938-943, 976-981, 1019, 1037-1043, 1089, 1108-1115, 1142-1144, 1177->1176, 1201-1211, 1215-1235 |
| fractal\_server/app/runner/executors/slurm/utils\_executors.py                 |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/runner/extract\_archive.py                                 |       32 |        2 |        8 |        2 |     90% |    25, 85 |
| fractal\_server/app/runner/filenames.py                                        |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/run\_subprocess.py                                  |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/set\_start\_and\_last\_task\_index.py               |       15 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/shutdown.py                                         |       34 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/runner/task\_files.py                                      |       45 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_\_init\_\_.py                                  |      178 |        6 |       36 |        4 |     95% |120-125, 133->135, 135->139, 199, 298 |
| fractal\_server/app/runner/v2/\_local/\_\_init\_\_.py                          |       16 |        1 |        2 |        1 |     89% |       106 |
| fractal\_server/app/runner/v2/\_local/\_local\_config.py                       |       40 |        9 |       10 |        4 |     74% |94, 100, 102->105, 108-118 |
| fractal\_server/app/runner/v2/\_local/\_submit\_setup.py                       |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local/executor.py                              |       26 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_common/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_common/get\_slurm\_config.py            |       70 |        1 |       34 |        3 |     96% |60, 73->77, 104->108 |
| fractal\_server/app/runner/v2/\_slurm\_ssh/\_\_init\_\_.py                     |       27 |        1 |        2 |        1 |     93% |        67 |
| fractal\_server/app/runner/v2/\_slurm\_ssh/\_submit\_setup.py                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_sudo/\_\_init\_\_.py                    |       18 |        2 |        4 |        2 |     82% |    61, 66 |
| fractal\_server/app/runner/v2/\_slurm\_sudo/\_submit\_setup.py                 |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/deduplicate\_list.py                             |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/handle\_failed\_job.py                           |       22 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/merge\_outputs.py                                |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/runner.py                                        |      119 |        4 |       34 |        4 |     95% |45-49, 125, 168, 226->231 |
| fractal\_server/app/runner/v2/runner\_functions.py                             |      105 |        7 |       24 |        2 |     93% |91-93, 102, 125-129 |
| fractal\_server/app/runner/v2/runner\_functions\_low\_level.py                 |       60 |        5 |       10 |        3 |     89% |49-50, 57, 78, 124 |
| fractal\_server/app/runner/v2/task\_interface.py                               |       35 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/versions.py                                         |       11 |        2 |        2 |        1 |     77% |     29-30 |
| fractal\_server/app/schemas/\_\_init\_\_.py                                    |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_filter\_validators.py                            |       21 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                                    |       51 |        0 |       30 |        1 |     99% |    81->84 |
| fractal\_server/app/schemas/user.py                                            |       37 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_group.py                                     |       43 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_settings.py                                  |       60 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                                 |       51 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/accounting.py                                   |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                      |       99 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                        |       41 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/job.py                                          |       81 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/manifest.py                                     |       83 |        0 |       32 |        1 |     99% |  108->110 |
| fractal\_server/app/schemas/v2/project.py                                      |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status.py                                       |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                         |      138 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_collection.py                             |      101 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_group.py                                  |       97 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflow.py                                     |       50 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflowtask.py                                 |      136 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/security/\_\_init\_\_.py                                   |      178 |       30 |       34 |        1 |     81% |117-130, 149-150, 155-164, 169-177, 210, 268, 350-354 |
| fractal\_server/app/security/signup\_email.py                                  |       20 |        8 |        4 |        0 |     50% |     33-44 |
| fractal\_server/app/user\_settings.py                                          |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config.py                                                      |      307 |        6 |       92 |        6 |     96% |720-721, 726, 735, 740, 747, 752->exit |
| fractal\_server/images/\_\_init\_\_.py                                         |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                               |       59 |        0 |       16 |        1 |     99% |  104->115 |
| fractal\_server/images/tools.py                                                |       37 |        0 |       14 |        0 |    100% |           |
| fractal\_server/logger.py                                                      |       44 |        3 |       12 |        3 |     89% |112, 161, 165 |
| fractal\_server/main.py                                                        |       66 |        1 |       10 |        1 |     97% |       132 |
| fractal\_server/ssh/\_\_init\_\_.py                                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/ssh/\_fabric.py                                                |      264 |        0 |       46 |        3 |     99% |150->152, 257->exit, 299->353 |
| fractal\_server/string\_tools.py                                               |       17 |        0 |        8 |        0 |    100% |           |
| fractal\_server/syringe.py                                                     |       28 |        2 |        2 |        0 |     93% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/utils.py                                                 |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_\_init\_\_.py                                 |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_utils.py                                      |       28 |        0 |       12 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect.py                                      |      133 |        0 |       10 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/deactivate.py                                   |       93 |        1 |       20 |        1 |     98% |       167 |
| fractal\_server/tasks/v2/local/reactivate.py                                   |       73 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_\_init\_\_.py                                   |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_utils.py                                        |       31 |        8 |        2 |        0 |     76% |     78-87 |
| fractal\_server/tasks/v2/ssh/collect.py                                        |      133 |        1 |       10 |        0 |     99% |       306 |
| fractal\_server/tasks/v2/ssh/deactivate.py                                     |      101 |       11 |       20 |        1 |     88% |   192-221 |
| fractal\_server/tasks/v2/ssh/reactivate.py                                     |       85 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_background.py                                  |       62 |        0 |       14 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_database.py                                    |       18 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_package\_names.py                              |       23 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_python\_interpreter.py                         |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_templates.py                                   |       37 |        0 |        8 |        0 |    100% |           |
| fractal\_server/urls.py                                                        |        8 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                       |       42 |        0 |        4 |        0 |    100% |           |
| fractal\_server/zip\_tools.py                                                  |       67 |        0 |       18 |        0 |    100% |           |
|                                                                      **TOTAL** | **8779** |  **403** | **1820** |  **136** | **94%** |           |


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