# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                         |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|----------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                              |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/\_\_main\_\_.py                                              |      152 |       55 |       34 |        1 |     61% |98-104, 114-130, 273-335, 339-363, 367 |
| fractal\_server/app/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                       |       56 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/\_\_init\_\_.py                                   |        4 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkusergroup.py                                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                                |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                       |       44 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/\_\_init\_\_.py                                |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/accounting.py                                  |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                     |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/history.py                                     |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/job.py                                         |       36 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/profile.py                                     |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                     |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/resource.py                                    |       40 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                        |       25 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task\_group.py                                 |       67 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflow.py                                    |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                                |       23 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                   |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_\_init\_\_.py                          |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_aux\_functions.py                      |       26 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/accounting.py                            |       59 |        0 |       14 |        3 |     96% |86->88, 88->90, 90->93 |
| fractal\_server/app/routes/admin/v2/impersonate.py                           |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/job.py                                   |      134 |        0 |       46 |        1 |     99% |  238->249 |
| fractal\_server/app/routes/admin/v2/profile.py                               |       46 |        0 |        6 |        1 |     98% |    61->64 |
| fractal\_server/app/routes/admin/v2/resource.py                              |       86 |        0 |        8 |        1 |     99% |  130->134 |
| fractal\_server/app/routes/admin/v2/sharing.py                               |       61 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task.py                                  |       77 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group.py                           |      132 |        0 |       50 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group\_lifecycle.py                |      106 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/\_\_init\_\_.py                               |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/alive.py                                      |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/settings.py                                   |       26 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                            |       41 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                        |      128 |        1 |       32 |        1 |     99% |       425 |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_history.py               |       61 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_sharing.py               |       33 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_lifecycle.py       |       80 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_version\_update.py |       12 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_tasks.py                 |      135 |        1 |       46 |        1 |     99% |       202 |
| fractal\_server/app/routes/api/v2/\_aux\_task\_group\_disambiguation.py      |       46 |        3 |        4 |        2 |     90% |55->73, 125-130 |
| fractal\_server/app/routes/api/v2/dataset.py                                 |       99 |        1 |       12 |        1 |     98% |       275 |
| fractal\_server/app/routes/api/v2/history.py                                 |      177 |        2 |       34 |        2 |     98% |  214, 449 |
| fractal\_server/app/routes/api/v2/images.py                                  |      102 |        1 |       26 |        1 |     98% |       152 |
| fractal\_server/app/routes/api/v2/job.py                                     |       88 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/pre\_submission\_checks.py                 |       59 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/project.py                                 |       76 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/sharing.py                                 |       89 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/submit.py                                  |      111 |        0 |       26 |        1 |     99% |  255->257 |
| fractal\_server/app/routes/api/v2/task.py                                    |       88 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_collection.py                        |      146 |        2 |       32 |        1 |     98% |246->256, 283-284 |
| fractal\_server/app/routes/api/v2/task\_collection\_custom.py                |       69 |        0 |       18 |        1 |     99% |    62->99 |
| fractal\_server/app/routes/api/v2/task\_collection\_pixi.py                  |       85 |        0 |       18 |        1 |     99% |  104->114 |
| fractal\_server/app/routes/api/v2/task\_group.py                             |       95 |        0 |       32 |        1 |     99% |  216->220 |
| fractal\_server/app/routes/api/v2/task\_group\_lifecycle.py                  |      116 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_version\_update.py                   |       88 |        1 |       16 |        1 |     98% |       208 |
| fractal\_server/app/routes/api/v2/workflow.py                                |      109 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow\_import.py                        |       92 |        2 |       18 |        1 |     97% |   163-166 |
| fractal\_server/app/routes/api/v2/workflowtask.py                            |       75 |        1 |       26 |        1 |     98% |       191 |
| fractal\_server/app/routes/auth/\_\_init\_\_.py                              |       34 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/auth/\_aux\_auth.py                               |       78 |        0 |       18 |        1 |     99% |  241->254 |
| fractal\_server/app/routes/auth/current\_user.py                             |       45 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/auth/group.py                                     |       85 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/auth/login.py                                     |       10 |        0 |        4 |        1 |     93% |    25->24 |
| fractal\_server/app/routes/auth/oauth.py                                     |       53 |        0 |       10 |        1 |     98% |  108->107 |
| fractal\_server/app/routes/auth/register.py                                  |       11 |        0 |        4 |        1 |     93% |    24->23 |
| fractal\_server/app/routes/auth/router.py                                    |       18 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/auth/users.py                                     |       98 |        0 |       22 |        0 |    100% |           |
| fractal\_server/app/routes/auth/viewer\_paths.py                             |       18 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                      |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                   |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_versions.py                                 |       17 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/aux/validate\_user\_profile.py                    |       28 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/pagination.py                                     |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                                  |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user.py                                          |       46 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_group.py                                   |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                               |       77 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/accounting.py                                 |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                    |       39 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                      |       27 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/history.py                                    |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/job.py                                        |       60 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/manifest.py                                   |       68 |        0 |       28 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/profile.py                                    |       32 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/project.py                                    |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/resource.py                                   |       53 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/sharing.py                                    |       12 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status\_legacy.py                             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                       |       88 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_collection.py                           |       52 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_group.py                                |       71 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflow.py                                   |       29 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflowtask.py                               |       73 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/security/\_\_init\_\_.py                                 |      200 |        5 |       42 |        5 |     96% |109, 140-141, 147, 161, 297->305, 363->344 |
| fractal\_server/app/security/signup\_email.py                                |       28 |        2 |        6 |        2 |     88% |49-50, 51->56 |
| fractal\_server/app/shutdown.py                                              |       37 |        0 |       10 |        0 |    100% |           |
| fractal\_server/config/\_\_init\_\_.py                                       |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config/\_database.py                                         |       22 |        0 |        2 |        0 |    100% |           |
| fractal\_server/config/\_email.py                                            |       39 |        0 |        6 |        0 |    100% |           |
| fractal\_server/config/\_main.py                                             |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config/\_oauth.py                                            |       25 |        0 |        2 |        0 |    100% |           |
| fractal\_server/config/\_settings\_config.py                                 |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/exceptions.py                                                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/\_\_init\_\_.py                                       |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                             |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/status\_tools.py                                      |       48 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/tools.py                                              |       47 |        0 |       20 |        0 |    100% |           |
| fractal\_server/logger.py                                                    |       44 |        3 |       14 |        4 |     88% |96->99, 115, 164, 168 |
| fractal\_server/main.py                                                      |      104 |        1 |       16 |        1 |     98% |       127 |
| fractal\_server/runner/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/components.py                                         |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/\_\_init\_\_.py                                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/\_local.py                                     |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/\_slurm.py                                     |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/slurm\_mem\_to\_MB.py                          |       35 |        0 |       18 |        0 |    100% |           |
| fractal\_server/runner/exceptions.py                                         |       24 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/executors/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/base\_runner.py                             |       61 |        0 |       32 |        1 |     99% |  169->177 |
| fractal\_server/runner/executors/call\_command\_wrapper.py                   |       32 |        2 |        8 |        1 |     92% |52-54, 58->62 |
| fractal\_server/runner/executors/local/\_\_init\_\_.py                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/local/get\_local\_config.py                 |       14 |        0 |        6 |        0 |    100% |           |
| fractal\_server/runner/executors/local/runner.py                             |      128 |        1 |       24 |        3 |     97% |246->253, 279->262, 322 |
| fractal\_server/runner/executors/slurm\_common/\_\_init\_\_.py               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_common/\_batching.py                 |       67 |        1 |       28 |        1 |     98% |        44 |
| fractal\_server/runner/executors/slurm\_common/\_job\_states.py              |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_common/base\_slurm\_runner.py        |      433 |        6 |      110 |        6 |     98% |201, 218, 252, 255, 489, 536->542, 857->864, 899, 968->975, 1038->1010 |
| fractal\_server/runner/executors/slurm\_common/get\_slurm\_config.py         |       74 |        4 |       34 |        4 |     93% |47, 59->63, 85->89, 107-114 |
| fractal\_server/runner/executors/slurm\_common/remote.py                     |       51 |        6 |       10 |        2 |     87% |59->66, 108-127 |
| fractal\_server/runner/executors/slurm\_common/slurm\_config.py              |       69 |        4 |       24 |        4 |     91% |119, 164, 169, 200 |
| fractal\_server/runner/executors/slurm\_common/slurm\_job\_task\_models.py   |       69 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/run\_subprocess.py               |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/runner.py                        |      113 |        0 |        8 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/tar\_commands.py                 |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_sudo/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_sudo/\_subprocess\_run\_as\_user.py  |       23 |        1 |        6 |        1 |     93% |        53 |
| fractal\_server/runner/executors/slurm\_sudo/runner.py                       |       72 |        1 |        6 |        1 |     97% |       186 |
| fractal\_server/runner/filenames.py                                          |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/set\_start\_and\_last\_task\_index.py                 |       14 |        0 |       12 |        0 |    100% |           |
| fractal\_server/runner/task\_files.py                                        |       70 |        2 |        6 |        2 |     95% |    54, 75 |
| fractal\_server/runner/v2/\_\_init\_\_.py                                    |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/v2/\_local.py                                         |       18 |        1 |        2 |        1 |     90% |        75 |
| fractal\_server/runner/v2/\_slurm\_ssh.py                                    |       20 |        1 |        2 |        1 |     91% |       104 |
| fractal\_server/runner/v2/\_slurm\_sudo.py                                   |       18 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/v2/db\_tools.py                                       |       41 |        2 |       10 |        2 |     92% |    26, 40 |
| fractal\_server/runner/v2/deduplicate\_list.py                               |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/v2/merge\_outputs.py                                  |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/v2/runner.py                                          |      201 |        2 |       44 |        3 |     98% |292, 341, 359->376 |
| fractal\_server/runner/v2/runner\_functions.py                               |      192 |       11 |       48 |        8 |     92% |141, 171, 201->204, 280, 365-370, 429->433, 481-482, 579-584 |
| fractal\_server/runner/v2/submit\_workflow.py                                |      140 |        4 |       26 |        4 |     95% |133-138, 146->153, 153->161, 183->211, 251->258 |
| fractal\_server/runner/v2/task\_interface.py                                 |       39 |        0 |        4 |        0 |    100% |           |
| fractal\_server/ssh/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/ssh/\_fabric.py                                              |      309 |        2 |       40 |        2 |     99% |210->212, 310-311, 356->358 |
| fractal\_server/string\_tools.py                                             |       27 |        0 |       16 |        0 |    100% |           |
| fractal\_server/syringe.py                                                   |       28 |        4 |        2 |        0 |     87% |     91-94 |
| fractal\_server/tasks/\_\_init\_\_.py                                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/config/\_\_init\_\_.py                                 |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/config/\_pixi.py                                       |       44 |        0 |       10 |        0 |    100% |           |
| fractal\_server/tasks/config/\_python.py                                     |       13 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/utils.py                                               |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_\_init\_\_.py                               |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_utils.py                                    |       39 |        0 |       12 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect.py                                    |      124 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect\_pixi.py                              |      113 |        8 |        4 |        0 |     93% |   241-250 |
| fractal\_server/tasks/v2/local/deactivate.py                                 |       89 |        1 |       18 |        1 |     98% |       163 |
| fractal\_server/tasks/v2/local/deactivate\_pixi.py                           |       49 |        2 |        4 |        0 |     96% |     92-93 |
| fractal\_server/tasks/v2/local/delete.py                                     |       45 |        1 |        4 |        1 |     96% |        45 |
| fractal\_server/tasks/v2/local/reactivate.py                                 |       70 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/reactivate\_pixi.py                           |       77 |        8 |        4 |        0 |     90% |   188-199 |
| fractal\_server/tasks/v2/ssh/\_\_init\_\_.py                                 |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_pixi\_slurm\_ssh.py                           |       91 |        2 |       14 |        2 |     96% |   45, 136 |
| fractal\_server/tasks/v2/ssh/\_utils.py                                      |       57 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/collect.py                                      |      121 |        1 |       12 |        0 |     99% |       293 |
| fractal\_server/tasks/v2/ssh/collect\_pixi.py                                |      118 |        8 |        8 |        0 |     94% |   327-343 |
| fractal\_server/tasks/v2/ssh/deactivate.py                                   |       96 |       11 |       22 |        1 |     88% |   199-236 |
| fractal\_server/tasks/v2/ssh/deactivate\_pixi.py                             |       52 |        2 |        8 |        0 |     97% |   119-120 |
| fractal\_server/tasks/v2/ssh/delete.py                                       |       51 |        2 |        8 |        2 |     93% |    60, 81 |
| fractal\_server/tasks/v2/ssh/reactivate.py                                   |       81 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/reactivate\_pixi.py                             |       90 |        8 |        8 |        0 |     92% |   259-274 |
| fractal\_server/tasks/v2/utils\_background.py                                |       68 |        0 |       20 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_database.py                                  |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_package\_names.py                            |       23 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_pixi.py                                      |       48 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_python\_interpreter.py                       |        6 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_templates.py                                 |       43 |        0 |       10 |        0 |    100% |           |
| fractal\_server/types/\_\_init\_\_.py                                        |       51 |        0 |        0 |        0 |    100% |           |
| fractal\_server/types/validators/\_\_init\_\_.py                             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/types/validators/\_common\_validators.py                     |       36 |        0 |       16 |        0 |    100% |           |
| fractal\_server/types/validators/\_filter\_validators.py                     |       11 |        0 |        6 |        0 |    100% |           |
| fractal\_server/types/validators/\_workflow\_task\_arguments\_validators.py  |        7 |        0 |        2 |        0 |    100% |           |
| fractal\_server/urls.py                                                      |        8 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                     |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/zip\_tools.py                                                |       73 |        0 |       20 |        0 |    100% |           |
| **TOTAL**                                                                    | **10058** |  **190** | **1822** |   **93** | **97%** |           |


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