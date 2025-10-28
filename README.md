# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                         |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|----------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                              |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/\_\_main\_\_.py                                              |      150 |       55 |       34 |        1 |     61% |102-108, 118-133, 273-333, 337-361, 365 |
| fractal\_server/app/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                       |       56 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/\_\_init\_\_.py                                   |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkusergroup.py                                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                                |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                       |       42 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/user\_settings.py                                 |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/\_\_init\_\_.py                                |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/accounting.py                                  |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                     |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/history.py                                     |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/job.py                                         |       31 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/profile.py                                     |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                     |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/resource.py                                    |       37 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                        |       26 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task\_group.py                                 |       67 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflow.py                                    |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                                |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                   |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_\_init\_\_.py                          |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/\_aux\_functions.py                      |       26 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/accounting.py                            |       59 |        0 |       14 |        3 |     96% |86->88, 88->90, 90->93 |
| fractal\_server/app/routes/admin/v2/impersonate.py                           |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/job.py                                   |      115 |        0 |       42 |        1 |     99% |  183->194 |
| fractal\_server/app/routes/admin/v2/profile.py                               |       46 |        0 |        6 |        1 |     98% |    60->63 |
| fractal\_server/app/routes/admin/v2/project.py                               |       21 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/resource.py                              |       96 |        0 |       14 |        1 |     99% |  132->136 |
| fractal\_server/app/routes/admin/v2/task.py                                  |       58 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group.py                           |       93 |        0 |       46 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v2/task\_group\_lifecycle.py                |      105 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/\_\_init\_\_.py                               |       35 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                            |       41 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                        |      139 |        1 |       42 |        1 |     99% |       379 |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_history.py               |       48 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_lifecycle.py       |       87 |        0 |       16 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_task\_version\_update.py |       12 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions\_tasks.py                 |      128 |        0 |       44 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_task\_group\_disambiguation.py      |       42 |        3 |        2 |        1 |     91% |   122-127 |
| fractal\_server/app/routes/api/v2/dataset.py                                 |      108 |        4 |       12 |        0 |     97% |   243-253 |
| fractal\_server/app/routes/api/v2/history.py                                 |      171 |        2 |       34 |        2 |     98% |  206, 437 |
| fractal\_server/app/routes/api/v2/images.py                                  |      101 |        1 |       26 |        1 |     98% |       141 |
| fractal\_server/app/routes/api/v2/job.py                                     |       91 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/pre\_submission\_checks.py                 |       58 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/project.py                                 |       74 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/status\_legacy.py                          |       73 |        5 |       20 |        0 |     95% |   133-146 |
| fractal\_server/app/routes/api/v2/submit.py                                  |      111 |        0 |       26 |        1 |     99% |  239->246 |
| fractal\_server/app/routes/api/v2/task.py                                    |       86 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_collection.py                        |      146 |        2 |       32 |        1 |     98% |250->260, 287-288 |
| fractal\_server/app/routes/api/v2/task\_collection\_custom.py                |       70 |        0 |       18 |        1 |     99% |   63->100 |
| fractal\_server/app/routes/api/v2/task\_collection\_pixi.py                  |       85 |        0 |       18 |        1 |     99% |  104->114 |
| fractal\_server/app/routes/api/v2/task\_group.py                             |      100 |        0 |       34 |        1 |     99% |  225->229 |
| fractal\_server/app/routes/api/v2/task\_group\_lifecycle.py                  |      115 |        0 |       24 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/task\_version\_update.py                   |       86 |        1 |       16 |        1 |     98% |       203 |
| fractal\_server/app/routes/api/v2/workflow.py                                |      117 |        0 |       20 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/workflow\_import.py                        |       95 |        2 |       20 |        1 |     97% |   180-183 |
| fractal\_server/app/routes/api/v2/workflowtask.py                            |       76 |        2 |       28 |        2 |     96% |  183, 193 |
| fractal\_server/app/routes/auth/\_\_init\_\_.py                              |       30 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/auth/\_aux\_auth.py                               |       60 |        0 |       14 |        0 |    100% |           |
| fractal\_server/app/routes/auth/current\_user.py                             |       67 |        0 |       12 |        1 |     99% |  139->144 |
| fractal\_server/app/routes/auth/group.py                                     |       92 |        0 |       16 |        1 |     99% |  123->128 |
| fractal\_server/app/routes/auth/login.py                                     |       10 |        0 |        4 |        1 |     93% |    24->23 |
| fractal\_server/app/routes/auth/oauth.py                                     |       33 |        0 |       10 |        1 |     98% |    69->68 |
| fractal\_server/app/routes/auth/register.py                                  |       11 |        0 |        4 |        1 |     93% |    22->21 |
| fractal\_server/app/routes/auth/router.py                                    |       16 |        1 |        2 |        1 |     89% |        19 |
| fractal\_server/app/routes/auth/users.py                                     |       90 |        0 |       18 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                      |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                   |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/routes/aux/validate\_user\_profile.py                    |       28 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/routes/pagination.py                                     |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                                  |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user.py                                          |       42 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user\_group.py                                   |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                               |       71 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/accounting.py                                 |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                    |       26 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dumps.py                                      |       28 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/history.py                                    |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/job.py                                        |       60 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/manifest.py                                   |       68 |        0 |       28 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/profile.py                                    |       32 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/project.py                                    |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/resource.py                                   |       48 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status\_legacy.py                             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                       |       90 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_collection.py                           |       52 |        0 |       10 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task\_group.py                                |       73 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflow.py                                   |       26 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/workflowtask.py                               |       63 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/security/\_\_init\_\_.py                                 |      185 |       16 |       38 |        5 |     89% |108, 139-140, 146, 159-167, 200, 290->298, 323, 429-433 |
| fractal\_server/app/security/signup\_email.py                                |       27 |        8 |        6 |        0 |     64% |     46-60 |
| fractal\_server/app/shutdown.py                                              |       34 |        0 |       10 |        0 |    100% |           |
| fractal\_server/config/\_\_init\_\_.py                                       |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config/\_data.py                                             |       22 |        0 |        2 |        0 |    100% |           |
| fractal\_server/config/\_database.py                                         |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/config/\_email.py                                            |       47 |        0 |        6 |        0 |    100% |           |
| fractal\_server/config/\_main.py                                             |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/config/\_oauth.py                                            |       29 |        0 |        2 |        0 |    100% |           |
| fractal\_server/config/\_settings\_config.py                                 |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/exceptions.py                                                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/\_\_init\_\_.py                                       |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                             |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/status\_tools.py                                      |       48 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/tools.py                                              |       47 |        0 |       20 |        0 |    100% |           |
| fractal\_server/logger.py                                                    |       44 |        3 |       14 |        4 |     88% |96->99, 115, 164, 168 |
| fractal\_server/main.py                                                      |       73 |        1 |       10 |        1 |     98% |       125 |
| fractal\_server/runner/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/components.py                                         |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/\_\_init\_\_.py                                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/\_local.py                                     |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/\_slurm.py                                     |       29 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/config/slurm\_mem\_to\_MB.py                          |       35 |        0 |       18 |        0 |    100% |           |
| fractal\_server/runner/exceptions.py                                         |       24 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/executors/\_\_init\_\_.py                             |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/base\_runner.py                             |       59 |        0 |       32 |        1 |     99% |  143->151 |
| fractal\_server/runner/executors/call\_command\_wrapper.py                   |       26 |        2 |        6 |        1 |     91% |40-42, 46->49 |
| fractal\_server/runner/executors/local/\_\_init\_\_.py                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/local/get\_local\_config.py                 |       14 |        0 |        6 |        0 |    100% |           |
| fractal\_server/runner/executors/local/runner.py                             |      126 |        1 |       24 |        3 |     97% |241->248, 276->257, 319 |
| fractal\_server/runner/executors/slurm\_common/\_\_init\_\_.py               |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_common/\_batching.py                 |       67 |       36 |       28 |        5 |     42% |49, 125-130, 132-137, 139-144, 149-198 |
| fractal\_server/runner/executors/slurm\_common/\_job\_states.py              |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_common/base\_slurm\_runner.py        |      422 |        6 |      104 |        6 |     98% |168, 185, 219, 222, 457, 504->510, 822->829, 866, 935->942, 1007->977 |
| fractal\_server/runner/executors/slurm\_common/get\_slurm\_config.py         |       74 |        4 |       34 |        4 |     93% |47, 59->63, 85->89, 107-114 |
| fractal\_server/runner/executors/slurm\_common/remote.py                     |       51 |        6 |       10 |        2 |     87% |57->64, 106-125 |
| fractal\_server/runner/executors/slurm\_common/slurm\_config.py              |       61 |        4 |       22 |        4 |     90% |109, 151, 156, 181 |
| fractal\_server/runner/executors/slurm\_common/slurm\_job\_task\_models.py   |       69 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/\_\_init\_\_.py                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/run\_subprocess.py               |       19 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/runner.py                        |      108 |        0 |        8 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_ssh/tar\_commands.py                 |       13 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_sudo/\_\_init\_\_.py                 |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/executors/slurm\_sudo/\_subprocess\_run\_as\_user.py  |       23 |        1 |        6 |        1 |     93% |        52 |
| fractal\_server/runner/executors/slurm\_sudo/runner.py                       |       67 |        1 |        6 |        1 |     97% |       176 |
| fractal\_server/runner/filenames.py                                          |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/set\_start\_and\_last\_task\_index.py                 |       14 |        0 |       12 |        0 |    100% |           |
| fractal\_server/runner/task\_files.py                                        |       69 |        2 |        6 |        2 |     95% |    54, 75 |
| fractal\_server/runner/v2/\_\_init\_\_.py                                    |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/runner/v2/\_local.py                                         |       18 |        1 |        2 |        1 |     90% |        70 |
| fractal\_server/runner/v2/\_slurm\_ssh.py                                    |       20 |        1 |        2 |        1 |     91% |        98 |
| fractal\_server/runner/v2/\_slurm\_sudo.py                                   |       18 |        0 |        2 |        0 |    100% |           |
| fractal\_server/runner/v2/db\_tools.py                                       |       41 |        2 |       10 |        2 |     92% |    27, 41 |
| fractal\_server/runner/v2/deduplicate\_list.py                               |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/v2/merge\_outputs.py                                  |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/runner/v2/runner.py                                          |      187 |        2 |       40 |        2 |     98% |  248, 299 |
| fractal\_server/runner/v2/runner\_functions.py                               |      192 |       10 |       48 |        7 |     92% |144, 174, 199->203, 206-207, 370-375, 434->438, 585-590 |
| fractal\_server/runner/v2/submit\_workflow.py                                |      139 |        4 |       26 |        4 |     95% |134-139, 147->149, 149->153, 171->199, 240->247 |
| fractal\_server/runner/v2/task\_interface.py                                 |       39 |        0 |        4 |        0 |    100% |           |
| fractal\_server/ssh/\_\_init\_\_.py                                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/ssh/\_fabric.py                                              |      309 |        2 |       40 |        2 |     99% |212->214, 312-313, 358->360 |
| fractal\_server/string\_tools.py                                             |       27 |        0 |       16 |        0 |    100% |           |
| fractal\_server/syringe.py                                                   |       28 |        4 |        2 |        0 |     87% |     91-94 |
| fractal\_server/tasks/\_\_init\_\_.py                                        |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/config/\_\_init\_\_.py                                 |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/config/\_pixi.py                                       |       44 |        0 |       10 |        0 |    100% |           |
| fractal\_server/tasks/config/\_python.py                                     |       16 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/utils.py                                               |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_\_init\_\_.py                               |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/\_utils.py                                    |       39 |        0 |       12 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect.py                                    |      130 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/collect\_pixi.py                              |      117 |        8 |        4 |        0 |     93% |   253-262 |
| fractal\_server/tasks/v2/local/deactivate.py                                 |       89 |        1 |       18 |        1 |     98% |       162 |
| fractal\_server/tasks/v2/local/deactivate\_pixi.py                           |       49 |        2 |        4 |        0 |     96% |     92-93 |
| fractal\_server/tasks/v2/local/delete.py                                     |       45 |        1 |        4 |        1 |     96% |        47 |
| fractal\_server/tasks/v2/local/reactivate.py                                 |       70 |        0 |        4 |        0 |    100% |           |
| fractal\_server/tasks/v2/local/reactivate\_pixi.py                           |       77 |        8 |        4 |        0 |     90% |   187-199 |
| fractal\_server/tasks/v2/ssh/\_\_init\_\_.py                                 |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/\_pixi\_slurm\_ssh.py                           |       91 |        2 |       14 |        2 |     96% |   45, 136 |
| fractal\_server/tasks/v2/ssh/\_utils.py                                      |       57 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/collect.py                                      |      127 |        1 |       12 |        0 |     99% |       313 |
| fractal\_server/tasks/v2/ssh/collect\_pixi.py                                |      122 |        8 |        8 |        0 |     94% |   337-355 |
| fractal\_server/tasks/v2/ssh/deactivate.py                                   |       96 |       11 |       22 |        1 |     88% |   197-234 |
| fractal\_server/tasks/v2/ssh/deactivate\_pixi.py                             |       52 |        2 |        8 |        0 |     97% |   118-119 |
| fractal\_server/tasks/v2/ssh/delete.py                                       |       51 |        2 |        8 |        2 |     93% |    58, 79 |
| fractal\_server/tasks/v2/ssh/reactivate.py                                   |       81 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/ssh/reactivate\_pixi.py                             |       90 |        8 |        8 |        0 |     92% |   260-275 |
| fractal\_server/tasks/v2/utils\_background.py                                |       68 |        0 |       20 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_database.py                                  |       21 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_package\_names.py                            |       23 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_pixi.py                                      |       48 |        0 |        8 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_python\_interpreter.py                       |        6 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/utils\_templates.py                                 |       43 |        0 |       10 |        0 |    100% |           |
| fractal\_server/types/\_\_init\_\_.py                                        |       30 |        0 |        0 |        0 |    100% |           |
| fractal\_server/types/validators/\_\_init\_\_.py                             |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/types/validators/\_common\_validators.py                     |       25 |        0 |       12 |        0 |    100% |           |
| fractal\_server/types/validators/\_filter\_validators.py                     |       11 |        0 |        6 |        0 |    100% |           |
| fractal\_server/types/validators/\_workflow\_task\_arguments\_validators.py  |        7 |        0 |        2 |        0 |    100% |           |
| fractal\_server/urls.py                                                      |        8 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                     |       26 |        0 |        2 |        0 |    100% |           |
| fractal\_server/zip\_tools.py                                                |       67 |        0 |       18 |        0 |    100% |           |
|                                                                    **TOTAL** | **9723** |  **250** | **1768** |   **93** | **97%** |           |


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