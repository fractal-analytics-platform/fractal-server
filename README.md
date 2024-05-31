# Repository Coverage

[Full report](https://htmlpreview.github.io/?https://github.com/fractal-analytics-platform/fractal-server/blob/python-coverage-comment-action-data/htmlcov/index.html)

| Name                                                                      |    Stmts |     Miss |   Branch |   BrPart |   Cover |   Missing |
|-------------------------------------------------------------------------- | -------: | -------: | -------: | -------: | ------: | --------: |
| fractal\_server/\_\_init\_\_.py                                           |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/\_\_init\_\_.py                                       |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/db/\_\_init\_\_.py                                    |       75 |        0 |       24 |        7 |     93% |36->35, 44->43, 52->51, 79->78, 108->107, 115->114, 128->127 |
| fractal\_server/app/models/\_\_init\_\_.py                                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/linkuserproject.py                             |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/security.py                                    |       35 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/\_\_init\_\_.py                             |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/dataset.py                                  |       29 |        0 |        2 |        1 |     97% |    70->69 |
| fractal\_server/app/models/v1/job.py                                      |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/project.py                                  |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/state.py                                    |       14 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v1/task.py                                     |       50 |        0 |       12 |        3 |     95% |57->56, 64->63, 68->67 |
| fractal\_server/app/models/v1/workflow.py                                 |       55 |        3 |       14 |        6 |     87% |62->61, 80, 87->86, 91->90, 128->127, 129, 132->131, 133 |
| fractal\_server/app/models/v2/\_\_init\_\_.py                             |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/collection\_state.py                        |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/dataset.py                                  |       26 |        0 |        2 |        1 |     96% |    53->52 |
| fractal\_server/app/models/v2/job.py                                      |       31 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/project.py                                  |       15 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/task.py                                     |       57 |        0 |       16 |        2 |     97% |46->45, 71->70 |
| fractal\_server/app/models/v2/workflow.py                                 |       17 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/models/v2/workflowtask.py                             |       28 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/\_\_init\_\_.py                                |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/\_\_init\_\_.py                          |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/admin/v1.py                                    |      181 |        0 |       94 |        8 |     97% |61->60, 99->98, 148->147, 201->200, 282->281, 311->307, 345->344, 374->370 |
| fractal\_server/app/routes/admin/v2.py                                    |      193 |        0 |       82 |        8 |     97% |64->63, 93->92, 169->168, 198->194, 232->231, 261->257, 297->293, 352->351 |
| fractal\_server/app/routes/api/\_\_init\_\_.py                            |        8 |        0 |        2 |        1 |     90% |    14->13 |
| fractal\_server/app/routes/api/v1/\_\_init\_\_.py                         |       16 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/\_aux\_functions.py                     |      117 |        0 |       46 |        0 |    100% |           |
| fractal\_server/app/routes/api/v1/dataset.py                              |      216 |        0 |       72 |       12 |     96% |46->41, 71->67, 102->98, 126->122, 164->160, 236->231, 265->261, 291->287, 332->328, 366->362, 434->430, 531->530 |
| fractal\_server/app/routes/api/v1/job.py                                  |       80 |        0 |       24 |        6 |     94% |31->30, 55->51, 77->73, 111->107, 145->141, 173->169 |
| fractal\_server/app/routes/api/v1/project.py                              |      188 |        0 |       57 |        6 |     98% |56->55, 75->74, 101->100, 117->116, 143->142, 241->236 |
| fractal\_server/app/routes/api/v1/task.py                                 |       93 |        1 |       36 |        6 |     95% |31->30, 51->50, 69->68, 102, 116->113, 169->168 |
| fractal\_server/app/routes/api/v1/task\_collection.py                     |      116 |        5 |       22 |        5 |     93% |61->44, 94, 134-135, 211->210, 234-235 |
| fractal\_server/app/routes/api/v1/workflow.py                             |      130 |        0 |       44 |        9 |     95% |52->48, 78->73, 108->104, 129->125, 181->177, 233->229, 266->261, 295->293, 339->338 |
| fractal\_server/app/routes/api/v1/workflowtask.py                         |       64 |        1 |       24 |        6 |     92% |43->38, 83->79, 104->100, 133->136, 144, 160->156 |
| fractal\_server/app/routes/api/v2/\_\_init\_\_.py                         |       24 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/api/v2/\_aux\_functions.py                     |      142 |        7 |       60 |        7 |     93% |270, 315, 319, 355, 421, 431, 442 |
| fractal\_server/app/routes/api/v2/dataset.py                              |      108 |        0 |       36 |        8 |     94% |34->29, 59->55, 90->86, 114->110, 155->151, 203->202, 229->225, 256->251 |
| fractal\_server/app/routes/api/v2/images.py                               |      107 |        3 |       52 |        9 |     92% |48->44, 100->95, 124, 144->exit, 153, 203->199, 216->exit, 221, 242->237 |
| fractal\_server/app/routes/api/v2/job.py                                  |       79 |        0 |       24 |        6 |     94% |31->30, 58->54, 80->76, 114->110, 148->144, 176->172 |
| fractal\_server/app/routes/api/v2/project.py                              |      111 |        0 |       26 |        5 |     96% |32->31, 51->50, 77->76, 93->92, 119->118 |
| fractal\_server/app/routes/api/v2/status.py                               |       80 |        0 |       26 |        1 |     99% |    32->28 |
| fractal\_server/app/routes/api/v2/submit.py                               |       92 |        0 |       27 |        2 |     98% |50->45, 223->229 |
| fractal\_server/app/routes/api/v2/task.py                                 |      111 |        0 |       48 |        5 |     97% |32->31, 56->55, 74->73, 112->109, 194->193 |
| fractal\_server/app/routes/api/v2/task\_collection.py                     |      116 |        3 |       22 |        4 |     95% |61->44, 94, 212->211, 235-236 |
| fractal\_server/app/routes/api/v2/task\_legacy.py                         |       33 |        0 |       12 |        2 |     96% |21->20, 44->43 |
| fractal\_server/app/routes/api/v2/workflow.py                             |      142 |        2 |       54 |       12 |     93% |41->37, 67->62, 95->91, 119->115, 174->170, 227->223, 247, 273->268, 305->301, 309, 319->301, 367->366 |
| fractal\_server/app/routes/api/v2/workflowtask.py                         |       91 |        2 |       52 |        7 |     94% |31->26, 116->112, 137->133, 195->198, 199, 223, 239->235 |
| fractal\_server/app/routes/auth.py                                        |       64 |       12 |       16 |        4 |     75% |68->67, 78-79, 90->89, 98->97, 126-149 |
| fractal\_server/app/routes/aux/\_\_init\_\_.py                            |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_job.py                                   |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/routes/aux/\_runner.py                                |        9 |        0 |        2 |        0 |    100% |           |
| fractal\_server/app/runner/\_\_init\_\_.py                                |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/async\_wrap.py                                 |       12 |        0 |        4 |        2 |     88% |21->20, 22->24 |
| fractal\_server/app/runner/components.py                                  |        3 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/exceptions.py                                  |       50 |        6 |       16 |        7 |     80% |97-99, 110, 115, 120, 123->126, 127 |
| fractal\_server/app/runner/executors/\_\_init\_\_.py                      |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_\_init\_\_.py                |        2 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/executors/slurm/\_batching.py                  |       68 |        2 |       28 |        1 |     97% |   152-156 |
| fractal\_server/app/runner/executors/slurm/\_check\_jobs\_status.py       |       24 |        1 |       10 |        2 |     91% |25->31, 62 |
| fractal\_server/app/runner/executors/slurm/\_executor\_wait\_thread.py    |       47 |        3 |       16 |        1 |     94% |93->exit, 124-127 |
| fractal\_server/app/runner/executors/slurm/\_slurm\_config.py             |      154 |        9 |       54 |        6 |     93% |163-164, 181->185, 309, 327, 333, 348-355, 435-436 |
| fractal\_server/app/runner/executors/slurm/\_subprocess\_run\_as\_user.py |       44 |        1 |       16 |        1 |     97% |        88 |
| fractal\_server/app/runner/executors/slurm/executor.py                    |      441 |       35 |      145 |       14 |     92% |166, 178, 527, 625, 634, 643, 675-686, 822->exit, 825-826, 908, 926-930, 958->961, 974-979, 1056, 1074-1080, 1139-1146, 1207->1206, 1271-1277 |
| fractal\_server/app/runner/filenames.py                                   |        6 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/set\_start\_and\_last\_task\_index.py          |       15 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/task\_files.py                                 |       47 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_\_init\_\_.py                             |      164 |        1 |       33 |        2 |     98% |102, 162->169 |
| fractal\_server/app/runner/v1/\_common.py                                 |      166 |        8 |       48 |        4 |     94% |96-97, 100->exit, 107, 296, 298, 431-433 |
| fractal\_server/app/runner/v1/\_local/\_\_init\_\_.py                     |       22 |        1 |        4 |        1 |     92% |       162 |
| fractal\_server/app/runner/v1/\_local/\_local\_config.py                  |       33 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_local/\_submit\_setup.py                  |        7 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_local/executor.py                         |       26 |        0 |        8 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_slurm/\_\_init\_\_.py                     |       87 |        9 |       36 |       13 |     82% |77, 82, 215->219, 239, 241->250, 246->250, 250->255, 255->261, 265->280, 268-275, 283, 285->291, 300-301 |
| fractal\_server/app/runner/v1/\_slurm/\_submit\_setup.py                  |        9 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v1/\_slurm/get\_slurm\_config.py               |       64 |        7 |       30 |        4 |     84% |66->70, 93-98, 130, 137-141 |
| fractal\_server/app/runner/v1/common.py                                   |       34 |        1 |       10 |        1 |     95% |        28 |
| fractal\_server/app/runner/v1/handle\_failed\_job.py                      |       48 |        0 |       12 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_\_init\_\_.py                             |      177 |       16 |       49 |        8 |     88% |86, 92, 97-110, 131->138, 297->299, 300->302, 328, 331 |
| fractal\_server/app/runner/v2/\_local/\_\_init\_\_.py                     |       20 |        1 |        4 |        1 |     92% |       142 |
| fractal\_server/app/runner/v2/\_local/\_local\_config.py                  |       39 |        9 |       12 |        4 |     71% |93, 99, 101->104, 107-117 |
| fractal\_server/app/runner/v2/\_local/\_submit\_setup.py                  |        8 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_local/executor.py                         |       26 |        1 |        8 |        2 |     91% |78, 87->91 |
| fractal\_server/app/runner/v2/\_slurm/\_\_init\_\_.py                     |       24 |        2 |        6 |        2 |     87% |    67, 72 |
| fractal\_server/app/runner/v2/\_slurm/\_submit\_setup.py                  |       10 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/runner/v2/\_slurm/get\_slurm\_config.py               |       72 |       17 |       36 |        9 |     72% |69, 82->86, 109-114, 118-119, 123-125, 129, 138-145, 149, 156-160 |
| fractal\_server/app/runner/v2/deduplicate\_list.py                        |       14 |        0 |        4 |        0 |    100% |           |
| fractal\_server/app/runner/v2/handle\_failed\_job.py                      |       56 |        4 |       14 |        2 |     91% |86-93, 98->115 |
| fractal\_server/app/runner/v2/merge\_outputs.py                           |       22 |        1 |        8 |        2 |     90% |23, 29->32 |
| fractal\_server/app/runner/v2/runner.py                                   |      135 |        4 |       60 |        7 |     94% |46, 129, 181, 240->245, 252->254, 279->285, 294 |
| fractal\_server/app/runner/v2/runner\_functions.py                        |      115 |        7 |       26 |        2 |     94% |94-96, 105, 129-133 |
| fractal\_server/app/runner/v2/runner\_functions\_low\_level.py            |       62 |        5 |       24 |        4 |     90% |44-45, 47->exit, 54, 76, 119 |
| fractal\_server/app/runner/v2/task\_interface.py                          |       37 |        0 |        8 |        2 |     96% |42->41, 54->53 |
| fractal\_server/app/runner/v2/v1\_compat.py                               |       17 |        0 |        6 |        0 |    100% |           |
| fractal\_server/app/schemas/\_\_init\_\_.py                               |        1 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/\_validators.py                               |       60 |        0 |       30 |        0 |    100% |           |
| fractal\_server/app/schemas/state.py                                      |       13 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/user.py                                       |       49 |        0 |        8 |        2 |     96% |76->68, 122->121 |
| fractal\_server/app/schemas/v1/\_\_init\_\_.py                            |       33 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/applyworkflow.py                           |       62 |        0 |       12 |        2 |     97% |75->74, 86->85 |
| fractal\_server/app/schemas/v1/dataset.py                                 |       52 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/dumps.py                                   |       40 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/manifest.py                                |       41 |        0 |       12 |        2 |     96% |92->91, 124->123 |
| fractal\_server/app/schemas/v1/project.py                                 |       20 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/task.py                                    |       62 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v1/task\_collection.py                        |       42 |        0 |       12 |        2 |     96% |59->58, 73->72 |
| fractal\_server/app/schemas/v1/workflow.py                                |       67 |        0 |       11 |        2 |     97% |102->101, 168->167 |
| fractal\_server/app/schemas/v2/\_\_init\_\_.py                            |       37 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/dataset.py                                 |       63 |        0 |        8 |        3 |     96% |42->41, 79->78, 108->107 |
| fractal\_server/app/schemas/v2/dumps.py                                   |       51 |        2 |        4 |        2 |     93% |59->58, 65-66 |
| fractal\_server/app/schemas/v2/job.py                                     |       60 |        1 |       12 |        3 |     94% |51->50, 62->61, 68 |
| fractal\_server/app/schemas/v2/manifest.py                                |       63 |        0 |       34 |        3 |     97% |54->53, 134->133, 156->155 |
| fractal\_server/app/schemas/v2/project.py                                 |       18 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/status.py                                  |        5 |        0 |        0 |        0 |    100% |           |
| fractal\_server/app/schemas/v2/task.py                                    |       90 |        2 |        8 |        4 |     94% |39->38, 43, 119->118, 121 |
| fractal\_server/app/schemas/v2/task\_collection.py                        |       43 |        1 |       12 |        3 |     93% |58->57, 66, 72->71 |
| fractal\_server/app/schemas/v2/workflow.py                                |       40 |        1 |        7 |        2 |     94% |47->46, 49 |
| fractal\_server/app/schemas/v2/workflowtask.py                            |      123 |        0 |       28 |        5 |     97% |64->63, 78->77, 92->91, 143->142, 157->156 |
| fractal\_server/app/security/\_\_init\_\_.py                              |      142 |       28 |       32 |        3 |     76% |113-126, 145-146, 151-160, 165-173, 187, 191, 315 |
| fractal\_server/config.py                                                 |      180 |        4 |       64 |       12 |     93% |76->75, 126->125, 204->203, 207, 216->exit, 228->227, 231, 235->exit, 276->275, 293->292, 450-451, 456->exit |
| fractal\_server/images/\_\_init\_\_.py                                    |        4 |        0 |        0 |        0 |    100% |           |
| fractal\_server/images/models.py                                          |       65 |        1 |       34 |        8 |     91% |37->36, 41->40, 52->51, 57, 71->70, 89->88, 93->92, 126->125 |
| fractal\_server/images/tools.py                                           |       29 |        0 |       12 |        0 |    100% |           |
| fractal\_server/logger.py                                                 |       44 |        2 |       12 |        2 |     93% |  160, 164 |
| fractal\_server/main.py                                                   |       57 |        0 |        9 |        2 |     97% |49->54, 84->83 |
| fractal\_server/syringe.py                                                |       28 |        2 |        8 |        3 |     86% |66->65, 83->82, 93-94, 97->96 |
| fractal\_server/tasks/\_\_init\_\_.py                                     |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/endpoint\_operations.py                             |       76 |        0 |       26 |        4 |     96% |38->exit, 113->exit, 118->exit, 123->exit |
| fractal\_server/tasks/utils.py                                            |       46 |        0 |        6 |        0 |    100% |           |
| fractal\_server/tasks/v1/\_TaskCollectPip.py                              |       43 |        0 |       24 |        3 |     96% |29->28, 33->32, 57->56 |
| fractal\_server/tasks/v1/\_\_init\_\_.py                                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v1/background\_operations.py                        |      145 |        1 |       28 |        3 |     98% |90->exit, 121->exit, 143 |
| fractal\_server/tasks/v1/get\_collection\_data.py                         |       11 |        0 |        2 |        0 |    100% |           |
| fractal\_server/tasks/v2/\_TaskCollectPip.py                              |       43 |        0 |       24 |        3 |     96% |29->28, 33->32, 57->56 |
| fractal\_server/tasks/v2/\_\_init\_\_.py                                  |        0 |        0 |        0 |        0 |    100% |           |
| fractal\_server/tasks/v2/background\_operations.py                        |      159 |        2 |       38 |        5 |     96% |90->exit, 121->exit, 143, 226, 245->250 |
| fractal\_server/tasks/v2/get\_collection\_data.py                         |       11 |        0 |        2 |        0 |    100% |           |
| fractal\_server/urls.py                                                   |        7 |        0 |        4 |        0 |    100% |           |
| fractal\_server/utils.py                                                  |       21 |        0 |        2 |        0 |    100% |           |
|                                                                 **TOTAL** | **7904** |  **237** | **2320** |  **346** | **94%** |           |


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