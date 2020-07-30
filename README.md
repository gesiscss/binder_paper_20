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
timestamp | launch datetime as isoformat in UTC
version | 
origin | 
provider | 
spec | 
org | 
ref | ref extracted from spec
resolved_ref | resolved ref of spec when launch happened, null for events before 18.06.2020
r2d_version | r2d version of mybinder.org when launch happened
repo_url | 

2. [create_repo_table.py](scripts/create_repo_table.py)

Reads output of the first script (`mybinderlaunch` table) and creates `repo` table. 
For more information please run `python create_repo_table.py --help`.

`repo` table:

column name | desc
----- | ----
id | internal id
remote_id | repo id in GitHub, this is used to detect renamed repos
provider | 
repo_url | 
first_launch_ts | timestamp of the first launch
last_launch_ts | timestamp of the last launch
last_spec | 
ref | ref extracted from last_spec
resolved_ref | resolved ref of the last_spec at the time the script fetches it
resolved_date | date when resolved_ref is fetched
resolved_ref_date | commit date of resolved_ref
fork | 1 or 0, if repo is forked or not. if null, it means TODO
renamed | 0 (not renamed) or number of times that repo is renamed
launch_count | number of launches
binder_dir | "" or "binder" or ".binder"
buildpack | which Buildpack of r2d is used

This script also adds a new column to `launch` table:

column name | desc
----- | ----
repo_id | foreign key reference to id column in repo table

3. [build_and_run_images.py](scripts/build_and_run_images.py)

Runs `repo2docker` to build images of repos in `repo` table. 
For more information please run `python build_and_run_images.py --help`.

`execution` table:

column name | desc
----- | ----
script_timestamp | when the script is executed
repo_id | foreign key reference to id column in repo table
image_name | docker image name
r2d_version | 
build_timestamp | 
build_success | 1 or 0
nb_rel_path | notebook's relative path in repo
nb_success | 1 or 0, if notebook execution successful or not
nb_log_file | logs from notebook execution, e.g. kernel info can be found there

Note: docker version is 19.03.5 (https://github.com/jupyterhub/binderhub/blob/d861de48be8a3eae6cb35c22a976cffbebc45c69/helm-chart/binderhub/values.yaml#L146-L152)

### Analysis

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/gesiscss/binder_paper_20/master?filepath=analysis%2Frepos.ipynb)
