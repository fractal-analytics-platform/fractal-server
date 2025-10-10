# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                         |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|----------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                              |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                       |       58 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/\_\_init\_\_.py                                   |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkusergroup.py                                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                                |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                       |       42 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/user\_settings.py                                 |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/\_\_init\_\_.py                                |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/accounting.py                                  |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                     |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/history.py                                     |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/job.py                                         |       34 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                     |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                        |       28 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task\_group.py                                 |       72 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflow.py                                    |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                                |       22 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                   |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_\_init\_\_.py                          |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/accounting.py                            |       59 |        0 |       14 |        3 |     96% |86->88, 88->90, 90->93 |
| fractal\_server/app/routes/admin/v2/impersonate.py                           |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/job.py                                   |      115 |        0 |       42 |        1 |     99% |  183->194 |
| fractal\_server/app/routes/admin/v2/project.py                               |       21 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task.py                                  |       67 |        0 |       18 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group.py                           |       91 |        0 |       44 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group\_lifecycle.py                |      110 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/\_\_init\_\_.py                               |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                            |       41 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                        |      129 |        1 |       40 |        1 |     99% |       376 |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_history.py               |       48 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_lifecycle.py       |       87 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_version\_update.py |       12 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_tasks.py                 |      128 |        0 |       44 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_task\_group\_disambiguation.py      |       42 |        3 |        2 |        1 |     91% |   122-127 |
| fractal\_server/app/routes/api/v2/dataset.py                                 |      110 |        4 |       14 |        0 |     97% |   252-262 |
| fractal\_server/app/routes/api/v2/history.py                                 |      171 |        2 |       34 |        2 |     98% |  206, 437 |
| fractal\_server/app/routes/api/v2/images.py                                  |      103 |        1 |       26 |        1 |     98% |       141 |
| fractal\_server/app/routes/api/v2/job.py                                     |       91 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/pre\_submission\_checks.py                 |       58 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/project.py                                 |       72 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/status\_legacy.py                          |       73 |        5 |       20 |        0 |     95% |   133-146 |
| fractal\_server/app/routes/api/v2/submit.py                                  |      105 |        0 |       24 |        1 |     99% |  233->239 |
| fractal\_server/app/routes/api/v2/task.py                                    |       85 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_collection.py                        |      149 |        2 |       32 |        1 |     98% |242->252, 284-285 |
| fractal\_server/app/routes/api/v2/task\_collection\_custom.py                |       68 |        0 |       18 |        1 |     99% |    61->98 |
| fractal\_server/app/routes/api/v2/task\_collection\_pixi.py                  |       86 |        0 |       18 |        1 |     99% |  103->112 |
| fractal\_server/app/routes/api/v2/task\_group.py                             |      100 |        0 |       34 |        1 |     99% |  225->229 |
| fractal\_server/app/routes/api/v2/task\_group\_lifecycle.py                  |      124 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_version\_update.py                   |       91 |        1 |       16 |        1 |     98% |       203 |
| fractal\_server/app/routes/api/v2/workflow.py                                |      121 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow\_import.py                        |       95 |        2 |       20 |        1 |     97% |   179-182 |
| fractal\_server/app/routes/api/v2/workflowtask.py                            |       76 |        2 |       28 |        2 |     96% |  183, 193 |
| fractal\_server/app/routes/auth/\_\_init\_\_.py                              |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/\_aux\_auth.py                               |       60 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/auth/current\_user.py                             |       74 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/auth/group.py                                     |      106 |        0 |       20 |        1 |     99% |  125->130 |
| fractal\_server/app/routes/auth/login.py                                     |       10 |        0 |        4 |        1 |     93% |    24->23 |
| fractal\_server/app/routes/auth/oauth.py                                     |       21 |       12 |       10 |        2 |     35% |24-49, 64-65 |
| fractal\_server/app/routes/auth/register.py                                  |       11 |        0 |        4 |        1 |     93% |    22->21 |
| fractal\_server/app/routes/auth/router.py                                    |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/auth/users.py                                     |      105 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                      |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                   |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/validate\_user\_settings.py                   |       28 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/pagination.py                                     |       27 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                                  |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user.py                                          |       31 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_group.py                                   |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_settings.py                                |       42 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                               |       59 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/accounting.py                                 |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                    |       38 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                      |       51 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/history.py                                    |       50 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/job.py                                        |       70 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/manifest.py                                   |       71 |        0 |       28 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/project.py                                    |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status\_legacy.py                             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                       |      104 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_collection.py                           |       57 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_group.py                                |       94 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflow.py                                   |       38 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflowtask.py                               |       71 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/security/\_\_init\_\_.py                                 |      176 |       30 |       34 |        1 |     80% |115-128, 147-148, 153-162, 167-175, 208, 266, 348-352 |
| fractal\_server/app/security/signup\_email.py                                |       20 |        8 |        4 |        0 |     50% |     33-44 |
| fractal\_server/app/user\_settings.py                                        |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config.py                                                    |      331 |        5 |       90 |        5 |     97% |829-830, 848, 853, 860, 865->exit |
| fractal\_server/exceptions.py                                                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/\_\_init\_\_.py                                       |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                             |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/status\_tools.py                                      |       48 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/tools.py                                              |       47 |        0 |       20 |        0 |    100% |           |
| fractal\_server/logger.py                                                    |       44 |        3 |       14 |        4 |     88% |96->99, 115, 164, 168 |
| fractal\_server/main.py                                                      |       66 |        1 |       10 |        1 |     97% |       131 |
| fractal\_server/runner/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/components.py                                         |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/exceptions.py                                         |       22 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/executors/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/base\_runner.py                             |       57 |        0 |       32 |        1 |     99% |  133->141 |
| fractal\_server/runner/executors/call\_command\_wrapper.py                   |       26 |        2 |        6 |        1 |     91% |40-42, 46->49 |
| fractal\_server/runner/executors/local/\_\_init\_\_.py                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/local/get\_local\_config.py                 |       40 |        9 |       10 |        4 |     74% |91, 97, 99->102, 105-115 |
| fractal\_server/runner/executors/local/runner.py                             |      125 |        1 |       24 |        3 |     97% |198->205, 233->214, 276 |
| fractal\_server/runner/executors/slurm\_common/\_\_init\_\_.py               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_common/\_batching.py                 |       67 |       36 |       28 |        5 |     42% |49, 125-130, 132-137, 139-144, 149-198 |
| fractal\_server/runner/executors/slurm\_common/\_job\_states.py              |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_common/\_slurm\_config.py            |      162 |       26 |       52 |       11 |     79% |172-173, 190->194, 302, 344, 350, 375, 388-394, 444-445, 447, 451-452, 457-458, 460-468 |
| fractal\_server/runner/executors/slurm\_common/base\_slurm\_runner.py        |      436 |        6 |      104 |        6 |     98% |173, 190, 224, 227, 462, 509->515, 785->792, 832, 901->908, 973->943 |
| fractal\_server/runner/executors/slurm\_common/get\_slurm\_config.py         |       77 |        4 |       34 |        4 |     93% |55, 68->72, 99->103, 121-128 |
| fractal\_server/runner/executors/slurm\_common/remote.py                     |       51 |        6 |       10 |        2 |     87% |57->64, 106-125 |
| fractal\_server/runner/executors/slurm\_common/slurm\_job\_task\_models.py   |       83 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/run\_subprocess.py               |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/runner.py                        |      108 |        0 |        8 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/tar\_commands.py                 |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_sudo/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_sudo/\_subprocess\_run\_as\_user.py  |       23 |        2 |        6 |        2 |     86% |    52, 83 |
| fractal\_server/runner/executors/slurm\_sudo/runner.py                       |       68 |        1 |        6 |        1 |     97% |       176 |
| fractal\_server/runner/filenames.py                                          |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/set\_start\_and\_last\_task\_index.py                 |       14 |        0 |       12 |        0 |    100% |           |
| fractal\_server/runner/shutdown.py                                           |       34 |        0 |       10 |        0 |    100% |           |
| fractal\_server/runner/task\_files.py                                        |       73 |        2 |        6 |        2 |     95% |    54, 75 |
| fractal\_server/runner/v2/\_\_init\_\_.py                                    |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/v2/\_local.py                                         |       15 |        1 |        2 |        1 |     88% |        59 |
| fractal\_server/runner/v2/\_slurm\_ssh.py                                    |       18 |        1 |        2 |        1 |     90% |        64 |
| fractal\_server/runner/v2/\_slurm\_sudo.py                                   |       17 |        1 |        4 |        1 |     90% |        61 |
| fractal\_server/runner/v2/db\_tools.py                                       |       41 |        2 |       10 |        2 |     92% |    27, 41 |
| fractal\_server/runner/v2/deduplicate\_list.py                               |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/v2/merge\_outputs.py                                  |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/v2/runner.py                                          |      188 |        2 |       40 |        2 |     98% |  256, 307 |
| fractal\_server/runner/v2/runner\_functions.py                               |      186 |       10 |       48 |        7 |     92% |113, 151, 174->178, 181-182, 352-357, 421->425, 571-576 |
| fractal\_server/runner/v2/submit\_workflow.py                                |      158 |       10 |       30 |        4 |     93% |114-119, 127->129, 129->133, 186-202, 267 |
| fractal\_server/runner/v2/task\_interface.py                                 |       40 |        0 |        4 |        0 |    100% |           |
| fractal\_server/ssh/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/ssh/\_fabric.py                                              |      324 |        2 |       40 |        2 |     99% |212->214, 312-313, 358->360 |
| fractal\_server/string\_tools.py                                             |       27 |        0 |       16 |        0 |    100% |           |
| fractal\_server/syringe.py                                                   |       28 |        2 |        2 |        0 |     93% |     93-94 |
| fractal\_server/tasks/\_\_init\_\_.py                                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/utils.py                                               |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_\_init\_\_.py                               |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_utils.py                                    |       41 |        0 |       12 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect.py                                    |      127 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect\_pixi.py                              |      118 |        8 |        4 |        0 |     93% |   237-246 |
| fractal\_server/tasks/v2/local/deactivate.py                                 |       87 |        1 |       18 |        1 |     98% |       156 |
| fractal\_server/tasks/v2/local/deactivate\_pixi.py                           |       47 |        2 |        4 |        0 |     96% |     88-89 |
| fractal\_server/tasks/v2/local/delete.py                                     |       43 |        1 |        4 |        1 |     96% |        43 |
| fractal\_server/tasks/v2/local/reactivate.py                                 |       67 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/reactivate\_pixi.py                           |       78 |        8 |        4 |        0 |     90% |   169-181 |
| fractal\_server/tasks/v2/ssh/\_\_init\_\_.py                                 |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_pixi\_slurm\_ssh.py                           |       92 |        2 |       14 |        2 |     96% |   46, 137 |
| fractal\_server/tasks/v2/ssh/\_utils.py                                      |       59 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/collect.py                                      |      124 |        1 |       12 |        0 |     99% |       306 |
| fractal\_server/tasks/v2/ssh/collect\_pixi.py                                |      123 |        8 |        8 |        0 |     94% |   319-337 |
| fractal\_server/tasks/v2/ssh/deactivate.py                                   |       94 |       11 |       22 |        1 |     88% |   193-230 |
| fractal\_server/tasks/v2/ssh/deactivate\_pixi.py                             |       50 |        2 |        8 |        0 |     97% |   114-115 |
| fractal\_server/tasks/v2/ssh/delete.py                                       |       49 |        2 |        8 |        2 |     93% |    59, 76 |
| fractal\_server/tasks/v2/ssh/reactivate.py                                   |       78 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/reactivate\_pixi.py                             |       91 |        8 |        8 |        0 |     92% |   241-256 |
| fractal\_server/tasks/v2/utils\_background.py                                |       68 |        0 |       20 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_database.py                                  |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_package\_names.py                            |       23 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_pixi.py                                      |       52 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_python\_interpreter.py                       |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_templates.py                                 |       45 |        0 |       10 |        0 |    100% |           |
| fractal\_server/types/\_\_init\_\_.py                                        |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/types/validators/\_\_init\_\_.py                             |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/types/validators/\_common\_validators.py                     |       25 |        0 |       12 |        0 |    100% |           |
| fractal\_server/types/validators/\_filter\_validators.py                     |       11 |        0 |        6 |        0 |    100% |           |
| fractal\_server/types/validators/\_workflow\_task\_arguments\_validators.py  |        7 |        0 |        2 |        0 |    100% |           |
| fractal\_server/urls.py                                                      |        8 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                     |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/zip\_tools.py                                                |       67 |        0 |       18 |        0 |    100% |           |
|                                                                    **TOTAL** | **9668** |  **262** | **1776** |  **103** | **96%** |           |


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