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
| fractal\_server/app/models/security.py                                         |       43 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/user\_settings.py                                   |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/\_\_init\_\_.py                                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                       |       27 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/job.py                                           |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                       |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                          |       29 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task\_group.py                                   |       56 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflow.py                                      |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                                  |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_\_init\_\_.py                            |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/job.py                                     |      103 |        0 |       36 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/project.py                                 |       22 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task.py                                    |       68 |        0 |       18 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group.py                             |      119 |        0 |       50 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group\_lifecycle.py                  |       93 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/\_\_init\_\_.py                                 |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                              |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                          |       99 |        1 |       32 |        1 |     98% |       357 |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_lifecycle.py         |       70 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_tasks.py                   |      115 |        0 |       40 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/dataset.py                                   |      122 |        4 |       24 |        0 |     97% |   271-281 |
| fractal\_server/app/routes/api/v2/images.py                                    |      116 |        3 |       38 |        3 |     96% |142, 175, 244 |
| fractal\_server/app/routes/api/v2/job.py                                       |       82 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/project.py                                   |      111 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/status.py                                    |       74 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/submit.py                                    |      104 |        0 |       26 |        1 |     99% |  209->215 |
| fractal\_server/app/routes/api/v2/task.py                                      |       90 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_collection.py                          |      151 |        2 |       32 |        1 |     98% |232->242, 274-275 |
| fractal\_server/app/routes/api/v2/task\_collection\_custom.py                  |       65 |        0 |       12 |        1 |     99% |    67->93 |
| fractal\_server/app/routes/api/v2/task\_group.py                               |      110 |        0 |       36 |        1 |     99% |  236->240 |
| fractal\_server/app/routes/api/v2/task\_group\_lifecycle.py                    |       93 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow.py                                  |      141 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow\_import.py                          |      114 |        2 |       28 |        1 |     98% |   248-251 |
| fractal\_server/app/routes/api/v2/workflowtask.py                              |      107 |        2 |       42 |        3 |     97% |84->98, 289, 299 |
| fractal\_server/app/routes/auth/\_\_init\_\_.py                                |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/\_aux\_auth.py                                 |       59 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/auth/current\_user.py                               |       74 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/auth/group.py                                       |      114 |        0 |       24 |        1 |     99% |  129->134 |
| fractal\_server/app/routes/auth/login.py                                       |       10 |        0 |        4 |        1 |     93% |    24->23 |
| fractal\_server/app/routes/auth/oauth.py                                       |       21 |       12 |       10 |        2 |     35% |24-47, 62-63 |
| fractal\_server/app/routes/auth/register.py                                    |       11 |        0 |        4 |        1 |     93% |    22->21 |
| fractal\_server/app/routes/auth/router.py                                      |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/users.py                                       |      105 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                                 |        8 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                        |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                     |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/validate\_user\_settings.py                     |       29 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/runner/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/components.py                                       |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/compress\_folder.py                                 |       57 |        2 |       10 |        2 |     94% |  126, 132 |
| fractal\_server/app/runner/exceptions.py                                       |       50 |        3 |       14 |        4 |     89% |97-99, 123->126, 127 |
| fractal\_server/app/runner/executors/\_\_init\_\_.py                           |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_\_init\_\_.py                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_batching.py                       |       68 |       38 |       28 |        6 |     40% |50, 126-131, 133-138, 140-145, 150-199, 211-212 |
| fractal\_server/app/runner/executors/slurm/\_slurm\_config.py                  |      155 |       34 |       52 |       12 |     71% |165-166, 183->187, 291-297, 317, 335, 340-341, 366, 375-376, 379-385, 431-432, 434, 438-439, 444-445, 447-455 |
| fractal\_server/app/runner/executors/slurm/ssh/\_\_init\_\_.py                 |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/ssh/\_executor\_wait\_thread.py     |       56 |        8 |       14 |        3 |     84% |66-69, 85-87, 103->exit, 108-109, 111->117, 115-116 |
| fractal\_server/app/runner/executors/slurm/ssh/\_slurm\_job.py                 |       35 |        3 |        4 |        2 |     87% |97, 109, 120 |
| fractal\_server/app/runner/executors/slurm/ssh/executor.py                     |      538 |      107 |      128 |       25 |     76% |130, 153, 390, 454, 500, 505, 514, 523, 560-571, 577, 714, 797-806, 850-863, 866-885, 897-909, 939->943, 952, 957-965, 983-1017, 1031-1064, 1065->1085, 1067-1082, 1085->977, 1102-1103, 1123, 1168->1172, 1222->1221, 1249-1259, 1263-1266, 1315-1319, 1337-1346, 1386-1394 |
| fractal\_server/app/runner/executors/slurm/sudo/\_\_init\_\_.py                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/sudo/\_check\_jobs\_status.py       |       24 |       11 |       10 |        1 |     47% |12-31, 55-62 |
| fractal\_server/app/runner/executors/slurm/sudo/\_executor\_wait\_thread.py    |       52 |        5 |       14 |        3 |     88% |80-83, 98->exit, 126->exit, 129-132 |
| fractal\_server/app/runner/executors/slurm/sudo/\_subprocess\_run\_as\_user.py |       46 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/sudo/executor.py                    |      435 |       68 |      102 |       23 |     82% |170, 182, 245, 269-270, 472, 574, 579, 588, 597, 625-636, 642, 781-782, 863->865, 882-886, 901->896, 906->921, 908-918, 921->824, 930-935, 968-973, 1011, 1029-1035, 1081, 1100-1107, 1134-1136, 1167->1166, 1191-1201, 1205-1225 |
| fractal\_server/app/runner/executors/slurm/utils\_executors.py                 |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/runner/extract\_archive.py                                 |       32 |        2 |        8 |        2 |     90% |    25, 85 |
| fractal\_server/app/runner/filenames.py                                        |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/run\_subprocess.py                                  |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/set\_start\_and\_last\_task\_index.py               |       15 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/shutdown.py                                         |       34 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/runner/task\_files.py                                      |       45 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_\_init\_\_.py                                  |      185 |        6 |       40 |        4 |     96% |121-126, 134->136, 136->140, 202, 304 |
| fractal\_server/app/runner/v2/\_local/\_\_init\_\_.py                          |       16 |        1 |        2 |        1 |     89% |       104 |
| fractal\_server/app/runner/v2/\_local/\_local\_config.py                       |       39 |        9 |       10 |        4 |     73% |93, 99, 101->104, 107-117 |
| fractal\_server/app/runner/v2/\_local/\_submit\_setup.py                       |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local/executor.py                              |       26 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/\_\_init\_\_.py            |       22 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/\_local\_config.py         |       39 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/\_submit\_setup.py         |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/executor.py                |       74 |        0 |       16 |        2 |     98% |72->80, 140->144 |
| fractal\_server/app/runner/v2/\_slurm\_common/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_common/get\_slurm\_config.py            |       70 |        1 |       34 |        3 |     96% |60, 73->77, 104->108 |
| fractal\_server/app/runner/v2/\_slurm\_ssh/\_\_init\_\_.py                     |       27 |        1 |        2 |        1 |     93% |        66 |
| fractal\_server/app/runner/v2/\_slurm\_ssh/\_submit\_setup.py                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_sudo/\_\_init\_\_.py                    |       18 |        2 |        4 |        2 |     82% |    60, 65 |
| fractal\_server/app/runner/v2/\_slurm\_sudo/\_submit\_setup.py                 |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/deduplicate\_list.py                             |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/handle\_failed\_job.py                           |       22 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/merge\_outputs.py                                |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/runner.py                                        |      113 |        4 |       34 |        4 |     95% |43-47, 123, 163, 221->226 |
| fractal\_server/app/runner/v2/runner\_functions.py                             |      102 |        7 |       24 |        2 |     93% |91-93, 102, 126-130 |
| fractal\_server/app/runner/v2/runner\_functions\_low\_level.py                 |       60 |        5 |       10 |        3 |     89% |49-50, 57, 78, 124 |
| fractal\_server/app/runner/v2/task\_interface.py                               |       30 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/versions.py                                         |       11 |        2 |        2 |        1 |     77% |     29-30 |
| fractal\_server/app/schemas/\_\_init\_\_.py                                    |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_filter\_validators.py                            |       21 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                                    |       51 |        0 |       30 |        1 |     99% |    79->82 |
| fractal\_server/app/schemas/user.py                                            |       33 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_group.py                                     |       32 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_settings.py                                  |       56 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                                 |       50 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                      |       87 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                        |       38 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/job.py                                          |       66 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/manifest.py                                     |       76 |        0 |       30 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/project.py                                      |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status.py                                       |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                         |      126 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_collection.py                             |       91 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_group.py                                  |       82 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflow.py                                     |       41 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflowtask.py                                 |      128 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/security/\_\_init\_\_.py                                   |      178 |       30 |       34 |        1 |     81% |117-130, 149-150, 155-164, 169-177, 210, 265, 347-351 |
| fractal\_server/app/security/signup\_email.py                                  |       17 |        6 |        2 |        0 |     58% |     27-35 |
| fractal\_server/app/user\_settings.py                                          |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config.py                                                      |      282 |        6 |       86 |        6 |     96% |673-674, 679, 688, 693, 700, 705->exit |
| fractal\_server/images/\_\_init\_\_.py                                         |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                               |       53 |        0 |       16 |        1 |     99% |   98->109 |
| fractal\_server/images/tools.py                                                |       37 |        0 |       14 |        0 |    100% |           |
| fractal\_server/logger.py                                                      |       44 |        2 |       12 |        2 |     93% |  161, 165 |
| fractal\_server/main.py                                                        |       65 |        1 |       10 |        1 |     97% |       131 |
| fractal\_server/ssh/\_\_init\_\_.py                                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/ssh/\_fabric.py                                                |      264 |        0 |       46 |        3 |     99% |150->152, 257->exit, 299->353 |
| fractal\_server/string\_tools.py                                               |       17 |        0 |        8 |        0 |    100% |           |
| fractal\_server/syringe.py                                                     |       28 |        2 |        2 |        0 |     93% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/utils.py                                                 |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_\_init\_\_.py                                 |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_utils.py                                      |       28 |        0 |       12 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect.py                                      |      131 |        0 |       10 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/deactivate.py                                   |       91 |        1 |       20 |        1 |     98% |       166 |
| fractal\_server/tasks/v2/local/reactivate.py                                   |       71 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_\_init\_\_.py                                   |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_utils.py                                        |       31 |        8 |        2 |        0 |     76% |     78-87 |
| fractal\_server/tasks/v2/ssh/collect.py                                        |      132 |        1 |       10 |        0 |     99% |       306 |
| fractal\_server/tasks/v2/ssh/deactivate.py                                     |       99 |       11 |       20 |        1 |     88% |   191-220 |
| fractal\_server/tasks/v2/ssh/reactivate.py                                     |       83 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_background.py                                  |       62 |        0 |       14 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_database.py                                    |       18 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_package\_names.py                              |       23 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_python\_interpreter.py                         |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_templates.py                                   |       34 |        0 |        8 |        0 |    100% |           |
| fractal\_server/urls.py                                                        |        8 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                       |       42 |        0 |        4 |        0 |    100% |           |
| fractal\_server/zip\_tools.py                                                  |       67 |        0 |       18 |        0 |    100% |           |
|                                                                      **TOTAL** | **8621** |  **413** | **1840** |  **144** | **94%** |           |


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