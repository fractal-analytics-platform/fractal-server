# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                           |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|------------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                                |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                         |       76 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/models/\_\_init\_\_.py                                     |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkusergroup.py                                    |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                                  |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                         |       45 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/user\_settings.py                                   |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/\_\_init\_\_.py                                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/dataset.py                                       |       29 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/v1/job.py                                           |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/project.py                                       |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/state.py                                         |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/task.py                                          |       48 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/models/v1/workflow.py                                      |       55 |        3 |       14 |        1 |     94% |80, 129, 133 |
| fractal\_server/app/models/v2/\_\_init\_\_.py                                  |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/collection\_state.py                             |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                       |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/v2/job.py                                           |       31 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                       |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                          |       25 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflow.py                                      |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                                  |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v1.py                                         |      180 |        1 |       94 |        1 |     99% |        56 |
| fractal\_server/app/routes/admin/v2.py                                         |      180 |        0 |       78 |        0 |    100% |           |
| fractal\_server/app/routes/api/\_\_init\_\_.py                                 |       15 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_\_init\_\_.py                              |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_aux\_functions.py                          |      127 |        0 |       50 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/dataset.py                                   |      223 |        0 |       72 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/job.py                                       |       79 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/project.py                                   |      190 |        0 |       51 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/task.py                                      |       99 |        1 |       36 |        1 |     99% |       104 |
| fractal\_server/app/routes/api/v1/task\_collection.py                          |      118 |        6 |       22 |        2 |     94% |135-136, 145-146, 235-236 |
| fractal\_server/app/routes/api/v1/workflow.py                                  |      135 |        0 |       44 |        1 |     99% |  298->296 |
| fractal\_server/app/routes/api/v1/workflowtask.py                              |       68 |        1 |       24 |        2 |     97% |134->137, 145 |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                              |       27 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                          |      122 |        1 |       46 |        1 |     99% |       421 |
| fractal\_server/app/routes/api/v2/dataset.py                                   |      108 |        0 |       36 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/images.py                                    |      107 |        3 |       52 |        5 |     95% |124, 144->exit, 153, 216->exit, 221 |
| fractal\_server/app/routes/api/v2/job.py                                       |       77 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/project.py                                   |      111 |        0 |       26 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/status.py                                    |       80 |        0 |       26 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/submit.py                                    |       99 |        0 |       27 |        1 |     99% |  222->228 |
| fractal\_server/app/routes/api/v2/task.py                                      |      113 |        0 |       48 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_collection.py                          |      153 |        2 |       38 |        0 |     99% |   240-241 |
| fractal\_server/app/routes/api/v2/task\_collection\_custom.py                  |       70 |        1 |       22 |        2 |     97% |47->73, 122 |
| fractal\_server/app/routes/api/v2/workflow.py                                  |      126 |        0 |       42 |        1 |     99% |  292->289 |
| fractal\_server/app/routes/api/v2/workflowtask.py                              |       72 |        2 |       34 |        2 |     96% |  172, 182 |
| fractal\_server/app/routes/auth/\_\_init\_\_.py                                |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/\_aux\_auth.py                                 |       37 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/auth/current\_user.py                               |       47 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/auth/group.py                                       |       69 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/auth/group\_names.py                                |       17 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/auth/login.py                                       |       10 |        0 |        4 |        1 |     93% |    24->23 |
| fractal\_server/app/routes/auth/oauth.py                                       |       21 |       12 |       10 |        2 |     35% |24-47, 62-63 |
| fractal\_server/app/routes/auth/register.py                                    |       11 |        0 |        4 |        1 |     93% |    22->21 |
| fractal\_server/app/routes/auth/router.py                                      |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/users.py                                       |      102 |        0 |       28 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                                 |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                        |        9 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                     |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/validate\_user\_settings.py                     |       29 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/runner/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/async\_wrap.py                                      |       12 |        0 |        4 |        1 |     94% |    22->24 |
| fractal\_server/app/runner/components.py                                       |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/compress\_folder.py                                 |       57 |        2 |       10 |        2 |     94% |  126, 132 |
| fractal\_server/app/runner/exceptions.py                                       |       50 |        3 |       16 |        4 |     89% |97-99, 123->126, 127 |
| fractal\_server/app/runner/executors/\_\_init\_\_.py                           |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_\_init\_\_.py                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_batching.py                       |       68 |        2 |       28 |        1 |     97% |   151-155 |
| fractal\_server/app/runner/executors/slurm/\_slurm\_config.py                  |      157 |        8 |       54 |        6 |     93% |165-166, 183->187, 317, 335, 341, 366, 450-451 |
| fractal\_server/app/runner/executors/slurm/ssh/\_\_init\_\_.py                 |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/ssh/\_executor\_wait\_thread.py     |       56 |        8 |       18 |        3 |     85% |66-69, 85-87, 103->exit, 108-109, 111->117, 115-116 |
| fractal\_server/app/runner/executors/slurm/ssh/\_slurm\_job.py                 |       35 |        3 |       10 |        2 |     84% |97, 109, 120 |
| fractal\_server/app/runner/executors/slurm/ssh/executor.py                     |      592 |      131 |      170 |       27 |     75% |129, 152, 414-420, 485->487, 487->491, 536, 564-571, 609, 655, 660, 669, 678, 693, 710-721, 727, 847, 930-939, 983-996, 999-1018, 1030-1042, 1067->1062, 1073-1080, 1089, 1094-1102, 1121-1144, 1158-1191, 1192->1212, 1194-1209, 1212->1115, 1220-1225, 1243, 1345->1344, 1385-1395, 1399-1402, 1451-1455, 1473-1482, 1518-1526 |
| fractal\_server/app/runner/executors/slurm/sudo/\_\_init\_\_.py                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/sudo/\_check\_jobs\_status.py       |       24 |        1 |       10 |        2 |     91% |25->31, 62 |
| fractal\_server/app/runner/executors/slurm/sudo/\_executor\_wait\_thread.py    |       47 |        3 |       16 |        1 |     94% |93->exit, 124-127 |
| fractal\_server/app/runner/executors/slurm/sudo/\_subprocess\_run\_as\_user.py |       46 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/sudo/executor.py                    |      449 |       41 |      147 |       14 |     90% |168, 180, 530, 628, 637, 646, 678-689, 825->exit, 828-829, 904-913, 929-933, 961->964, 977-982, 1059, 1077-1083, 1129, 1148-1155, 1216->1215, 1281-1287 |
| fractal\_server/app/runner/extract\_archive.py                                 |       32 |        2 |        8 |        2 |     90% |    25, 85 |
| fractal\_server/app/runner/filenames.py                                        |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/run\_subprocess.py                                  |       20 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/runner/set\_start\_and\_last\_task\_index.py               |       15 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/shutdown.py                                         |       46 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/runner/task\_files.py                                      |       45 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_\_init\_\_.py                                  |      169 |        0 |       37 |        1 |     99% |  209->216 |
| fractal\_server/app/runner/v1/\_common.py                                      |      168 |        8 |       48 |        4 |     94% |98-99, 102->exit, 109, 298, 300, 433-435 |
| fractal\_server/app/runner/v1/\_local/\_\_init\_\_.py                          |       22 |        1 |        4 |        1 |     92% |       162 |
| fractal\_server/app/runner/v1/\_local/\_local\_config.py                       |       33 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_local/\_submit\_setup.py                       |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_local/executor.py                              |       26 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_slurm/\_\_init\_\_.py                          |       87 |        9 |       36 |       13 |     82% |77, 82, 215->219, 239, 241->250, 246->250, 250->255, 255->261, 265->280, 268-275, 283, 285->291, 300-301 |
| fractal\_server/app/runner/v1/\_slurm/\_submit\_setup.py                       |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_slurm/get\_slurm\_config.py                    |       64 |        7 |       30 |        4 |     84% |66->70, 93-98, 130, 137-141 |
| fractal\_server/app/runner/v1/common.py                                        |       34 |        1 |       10 |        1 |     95% |        28 |
| fractal\_server/app/runner/v1/handle\_failed\_job.py                           |       48 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_\_init\_\_.py                                  |      214 |        8 |       61 |        6 |     95% |125-130, 138->140, 140->144, 204, 302, 434, 437 |
| fractal\_server/app/runner/v2/\_local/\_\_init\_\_.py                          |       20 |        1 |        4 |        1 |     92% |       142 |
| fractal\_server/app/runner/v2/\_local/\_local\_config.py                       |       39 |        9 |       12 |        4 |     71% |93, 99, 101->104, 107-117 |
| fractal\_server/app/runner/v2/\_local/\_submit\_setup.py                       |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local/executor.py                              |       26 |        1 |        8 |        2 |     91% |78, 87->91 |
| fractal\_server/app/runner/v2/\_local\_experimental/\_\_init\_\_.py            |       26 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/\_local\_config.py         |       39 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/\_submit\_setup.py         |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local\_experimental/executor.py                |       73 |        0 |       16 |        2 |     98% |71->79, 139->143 |
| fractal\_server/app/runner/v2/\_slurm\_common/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_common/get\_slurm\_config.py            |       70 |        1 |       34 |        3 |     96% |60, 73->77, 104->108 |
| fractal\_server/app/runner/v2/\_slurm\_ssh/\_\_init\_\_.py                     |       33 |        1 |        4 |        1 |     95% |        66 |
| fractal\_server/app/runner/v2/\_slurm\_ssh/\_submit\_setup.py                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm\_sudo/\_\_init\_\_.py                    |       24 |        2 |        6 |        2 |     87% |    62, 67 |
| fractal\_server/app/runner/v2/\_slurm\_sudo/\_submit\_setup.py                 |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/deduplicate\_list.py                             |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/handle\_failed\_job.py                           |       54 |        4 |       12 |        2 |     91% |86-93, 98->108 |
| fractal\_server/app/runner/v2/merge\_outputs.py                                |       22 |        1 |        8 |        2 |     90% |23, 29->32 |
| fractal\_server/app/runner/v2/runner.py                                        |      123 |        4 |       48 |        6 |     94% |45, 118, 158, 216->221, 254->260, 266 |
| fractal\_server/app/runner/v2/runner\_functions.py                             |      102 |        7 |       24 |        2 |     93% |91-93, 102, 126-130 |
| fractal\_server/app/runner/v2/runner\_functions\_low\_level.py                 |       60 |        5 |       18 |        3 |     90% |49-50, 57, 78, 124 |
| fractal\_server/app/runner/v2/task\_interface.py                               |       37 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/versions.py                                         |       11 |        2 |        2 |        1 |     77% |     29-30 |
| fractal\_server/app/schemas/\_\_init\_\_.py                                    |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                                    |       60 |        2 |       30 |        3 |     94% |79, 82, 95->98 |
| fractal\_server/app/schemas/user.py                                            |       33 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_group.py                                     |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_settings.py                                  |       59 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/\_\_init\_\_.py                                 |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/applyworkflow.py                                |       62 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/dataset.py                                      |       52 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/dumps.py                                        |       40 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/manifest.py                                     |       41 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/project.py                                      |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/state.py                                        |       11 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/task.py                                         |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/task\_collection.py                             |       42 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/workflow.py                                     |       67 |        0 |       11 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                                 |       39 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                      |       63 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                        |       38 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/job.py                                          |       60 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/manifest.py                                     |       63 |        0 |       34 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/project.py                                      |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status.py                                       |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                         |       92 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_collection.py                             |       84 |        0 |       30 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflow.py                                     |       40 |        0 |        7 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflowtask.py                                 |      106 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/security/\_\_init\_\_.py                                   |      161 |       33 |       40 |        3 |     77% |111-124, 143-144, 149-158, 163-171, 190, 214-219, 308-312 |
| fractal\_server/app/user\_settings.py                                          |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config.py                                                      |      269 |        8 |      105 |        8 |     95% |226, 246, 625-626, 631, 640, 645, 652, 657->exit |
| fractal\_server/images/\_\_init\_\_.py                                         |        4 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                               |       65 |        1 |       34 |        1 |     98% |        57 |
| fractal\_server/images/tools.py                                                |       29 |        0 |       12 |        0 |    100% |           |
| fractal\_server/logger.py                                                      |       44 |        2 |       12 |        2 |     93% |  160, 164 |
| fractal\_server/main.py                                                        |       72 |        1 |       15 |        2 |     97% |51->56, 142 |
| fractal\_server/ssh/\_\_init\_\_.py                                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/ssh/\_fabric.py                                                |      208 |        0 |       70 |        2 |     99% |152->exit, 194->243 |
| fractal\_server/string\_tools.py                                               |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/syringe.py                                                     |       28 |        2 |        8 |        0 |     94% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/utils.py                                                 |       31 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v1/\_TaskCollectPip.py                                   |       43 |        0 |       24 |        0 |    100% |           |
| fractal\_server/tasks/v1/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v1/background\_operations.py                             |      145 |        1 |       28 |        3 |     98% |90->exit, 121->exit, 143 |
| fractal\_server/tasks/v1/endpoint\_operations.py                               |       71 |        0 |       24 |        4 |     96% |36->exit, 108->exit, 113->exit, 118->exit |
| fractal\_server/tasks/v1/get\_collection\_data.py                              |       11 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v1/utils.py                                              |       22 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_TaskCollectPip.py                                   |       61 |        0 |       22 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_venv\_pip.py                                        |       53 |        0 |       14 |        2 |     97% |77->exit, 110->exit |
| fractal\_server/tasks/v2/background\_operations.py                             |      143 |        0 |       34 |        0 |    100% |           |
| fractal\_server/tasks/v2/background\_operations\_ssh.py                        |      136 |        8 |       30 |        5 |     92% |40, 42, 162, 165->172, 265-271, 349-350 |
| fractal\_server/tasks/v2/endpoint\_operations.py                               |       65 |        0 |       18 |        1 |     99% |  41->exit |
| fractal\_server/tasks/v2/utils.py                                              |       19 |        2 |        4 |        0 |     91% |     51-52 |
| fractal\_server/urls.py                                                        |        7 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                       |       21 |        0 |        2 |        0 |    100% |           |
| fractal\_server/zip\_tools.py                                                  |       56 |        0 |       26 |        0 |    100% |           |
|                                                                      **TOTAL** | **10079** |  **369** | **2845** |  **186** | **95%** |           |


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