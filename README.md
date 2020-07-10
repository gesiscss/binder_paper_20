Repository for "Exploratory Survey Paper on Projects using MyBinder"

- issue: https://github.com/jupyterhub/team-compass/issues/277
- hackmd: https://hackmd.io/x6D_37h9Traez6qBMY3w2g

## Content

### Scripts

1. [parse_mybinder_archive.py](scripts/parse_mybinder_archive.py)

Parses [mybinder.org events archive](https://archive.analytics.mybinder.org) 
and writes launches into `mybinderlaunch` table in a sqlite3 database. 
For more information please run `python parse_mybinder_archive.py --help`.

`mybinderlaunch` table:

column name | desc
----- | ----
timestamp | 
version | 
origin | 
provider | 
spec | 
org | 
ref | 
resolved_ref | 
repo_url | 

2. [create_repo_table.py](scripts/create_repo_table.py)

Reads output of the first script (`mybinderlaunch` table) and creates a `repo` table. 
For more information please run `python create_repo_table.py --help`.

`repo` table:

column name | desc
----- | ----
id | internal id
remote_id | repo id in GitHub, this is used to detect renamed repos
renamed | 1 or 0, if repo is renamed or not
fork | 1 or 0, if repo is forked or not
dockerfile | 1 or 0, if repo is dockerfile repo or not
resolved_ref_now | current resolved ref of the last launched spec
last_spec | 
image_name | 
repo_url | 
provider | 
launch_count | 
first_launch | 
last_launch | 
refs | 
resolved_refs | 

3. [build_images.py](scripts/build_images.py)

Runs `repo2docker` to build images of repos in `repo` table. 
And adds a new column `build_success` into `repo` table. 
For more information please run `python build_images.py --help`.

4. TODO run_images.py

Run each image from previous step and check

- if repo contains any jupyter notebook (`contains_nbs` column)
- if notebooks in repo are executed successfully (`nbs_executed` column)
- if outputs of notebooks same as in repo (`nbs_same_output` column)
- TODO more?


### Analysis

TODO
