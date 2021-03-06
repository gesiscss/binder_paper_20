import docker
import argparse
import os
import pandas as pd
from docker.errors import APIError
from requests import ReadTimeout
from utils import get_repo2docker_image, get_logger, get_image_name, get_utc_ts, \
     REPO_TABLE as repo_table, EXECUTION_TABLE as execution_table, \
     DEFAULT_IMAGE_PREFIX as default_image_prefix, Timeout, BuildTimeoutException, check_if_exists
from datetime import datetime
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed
from sqlite_utils import Database

# time out for python docker client
DOCKER_TIMEOUT = 300
# https://github.com/jupyterhub/binderhub/blob/b81d913f66236cab840c437975683fbcac1e6e62/binderhub/app.py#L435-L443
# we dont guarantee 2 cpus for image building, so use higher timeout
BUILD_TIMEOUT = 3600 * 6


def create_dir(dir_path):
    """"Create a folder with mode 0o777, so jovyan in container can write into it.
    0o777 is the default mode that makedirs uses but sometimes it is ignored:
    https://docs.python.org/3/library/os.html#os.makedirs
    """
    os.makedirs(dir_path, exist_ok=True)
    # 0o777 means anyone can do anything
    os.chmod(dir_path, 0o777)


def detect_notebooks(repo_id, image_name, repo_output_folder, current_dir, buildpack):
    _, ts_safe = get_utc_ts()
    notebooks_log_file = os.path.join(repo_output_folder, f'notebooks_{ts_safe}_logs.txt')
    client = docker.from_env(timeout=DOCKER_TIMEOUT)
    notebooks = []
    # shell command to find all notebooks
    # excludes all hidden notebooks and also notebooks in hidden folders (includes checkpoints)
    find_cmd = "find . -type f -name '*.ipynb' ! -path '*/.*' -print"
    if buildpack == "NixBuildPack":
        command = ["/usr/local/bin/nix-shell-wrapper", f"{find_cmd} | grep ipynb > /io/notebooks.txt"]
    else:
        command = ["/bin/sh", "-c", f"{find_cmd} > /io/notebooks.txt"]
    with open(notebooks_log_file, 'w') as log_file:
        try:
            container = client.containers.run(
                image=image_name,
                name=f"{repo_id}-detect-notebooks-{script_ts_safe}",
                volumes={
                    current_dir: {"bind": "/src", "mode": "ro"},
                    repo_output_folder: {"bind": "/io", "mode": "rw"},
                },
                command=command,
                detach=True
            )
        except docker.errors.ContainerError as e:
            text = e.stderr
            if isinstance(text, bytes):
                text = text.decode("utf8", "replace")
            log_file.write(text)
            e.container.remove(force=True)
            notebooks_success = 0
        else:
            timeout = 10 * 60
            try:
                with Timeout(seconds=timeout, error_message=f"Timeout ({timeout})"):
                    for log in container.logs(follow=True, stream=True):
                        if isinstance(log, bytes):
                            log = log.decode("utf8", "replace")
                        log_file.write(log)
                    status = container.wait()
            except BuildTimeoutException:
                log_file.write(f"Container Timed out ({timeout})\n")
                logger.info(f"{repo_id} : {image_name} : Notebooks detection container Timed out ({timeout})")
                container.remove(force=True)
                notebooks_success = 0
            else:
                message = f"\nContainer exited with status: {status}\n"
                log_file.write(message)
                container.remove(force=True)
                notebooks_success = 1 if status["StatusCode"] == 0 else 0
                if notebooks_success:
                    notebooks_file = os.path.join(repo_output_folder, 'notebooks.txt')
                    try:
                        with open(notebooks_file, 'r') as f:
                            for line in f:
                                nb_rel_path = line.rstrip()
                                if nb_rel_path.startswith("./"):
                                    nb_rel_path = nb_rel_path[2:]
                                if not nb_rel_path:
                                    # skip empty last line
                                    continue
                                # # skip notebooks in hidden folders
                                # if nb_rel_path.startswith(".local") or \
                                #    nb_rel_path.startswith(".cache") or \
                                #    nb_rel_path.startswith(".julia") or \
                                #    nb_rel_path.startswith(".jupyter") or \
                                #    nb_rel_path.startswith(".opam") or \
                                #    nb_rel_path.startswith(".ipython"):
                                #     # and skip notebooks in hidden folders
                                #     continue
                                notebooks.append(nb_rel_path)
                    except FileNotFoundError as e:
                        notebooks_success = 0
                        logger.exception(f"{repo_id} : {buildpack} : detect_notebooks")
    return notebooks_success, notebooks


def run_image(repo_id, repo_url, image_name, buildpack):
    """This function is mostly copied from
    https://github.com/minrk/repo2docker-checker/blob/bd179da5786e08a12ef92295cf02b38a5c2b8ceb/repo2docker_checker/checker.py#L160
    """
    output_folder = os.path.abspath(run_output_folder)
    repo_folder = f'{repo_id}_{image_name.replace("/", "-").replace(":", "-")}'
    repo_output_folder = os.path.join(output_folder, repo_folder)
    create_dir(repo_output_folder)
    current_dir = os.path.dirname(os.path.realpath(__file__))

    notebooks_success, notebooks = detect_notebooks(repo_id, image_name, repo_output_folder, current_dir, buildpack)
    execution_entries = []
    if not notebooks:
        if notebooks_success:
            logger.info(f"{repo_id} : {repo_url} has no notebook")
        else:
            logger.info(f"{repo_id} : {repo_url} failed to detect notebooks")
        return notebooks_success, execution_entries

    if notebooks_range is not None and type(notebooks_range) == tuple:
        f = 0 if notebooks_range[0] == "" else notebooks_range[0]
        t = len(notebooks)+1 if notebooks_range[1] == "" else notebooks_range[1]
        if not (f <= len(notebooks) < t):
            logger.info(f"{repo_id} : {repo_url} skipping notebooks execution, "
                        f"limit is {notebooks_range} but it has {len(notebooks)} notebook")
            for nb_rel_path in notebooks:
                execution_entries.append({"nb_rel_path": nb_rel_path})
            return notebooks_success, execution_entries

    logger.info(f"{repo_id} : {repo_url} executing {len(notebooks)} notebooks")
    # execute each notebook separately
    client = docker.from_env(timeout=DOCKER_TIMEOUT)
    nb_count = 0
    for nb_rel_path in notebooks:
        nb_count += 1
        _, ts_safe = get_utc_ts()
        nb_log_file = os.path.join(repo_output_folder, f'{nb_rel_path.replace("/", "-")}_{ts_safe}.log')
        execution_entry = {
            "nb_rel_path": nb_rel_path,
            "nb_log_file": os.path.relpath(nb_log_file, current_dir),
        }
        with open(nb_log_file, 'w') as log_file:
            try:
                kind = "notebook"
                container = client.containers.run(
                    image=image_name,
                    name=f"{repo_id}-execute-nb-{nb_count}-{script_ts_safe}",
                    volumes={
                        current_dir: {"bind": "/src", "mode": "ro"},
                        repo_output_folder: {"bind": "/io", "mode": "rw"},
                    },
                    command=[
                        "python3",
                        "-u",
                        "/src/inrepo.py",
                        "--output-dir",
                        "/io",
                        kind,
                        nb_rel_path,
                    ],
                    # run container with mem limit same as singleuser pod memory limit, which is 2g
                    # https://github.com/jupyterhub/mybinder.org-deploy/blob/4cfbd9c7975d5d8b6cccbb02974be8aca499b228/config/prod.yaml#L52
                    mem_limit="2g",
                    # use detach and auto_remove together
                    # https://github.com/docker/docker-py/blob/master/docker/models/containers.py#L788-L790
                    detach=True,  # Run container in the background and return a Container object
                    # auto_remove=True,  # enable auto-removal of the container on daemon side when the container’s process exits.
                    # remove=True,  # Remove the container when it has finished running
                )
            except docker.errors.ContainerError as e:
                text = e.stderr
                if isinstance(text, bytes):
                    text = text.decode("utf8", "replace")
                log_file.write(text)
                e.container.remove(force=True)
                execution_entry["nb_success"] = 0
            else:
                timeout = 30 * 60
                try:
                    with Timeout(seconds=timeout, error_message=f"Timeout ({timeout})"):
                        for log in container.logs(follow=True, stream=True):
                            if isinstance(log, bytes):
                                log = log.decode("utf8", "replace")
                            log_file.write(log)
                        status = container.wait()
                except BuildTimeoutException:
                    log_file.write(f"Container Timed out ({timeout})\n")
                    logger.info(f"{repo_id} : {nb_rel_path} : Notebook execution container Timed out ({timeout})")
                    container.remove(force=True)
                    execution_entry["nb_success"] = 0
                else:
                    message = f"\nContainer exited with status: {status}\n"
                    log_file.write(message)
                    container.remove(force=True)
                    execution_entry["nb_success"] = 1 if status["StatusCode"] == 0 else 0
            finally:
                execution_entries.append(execution_entry)
    return notebooks_success, execution_entries


def build_image(repo_id, repo_url, image_name, resolved_ref):
    result = {}
    client = docker.from_env(timeout=DOCKER_TIMEOUT)
    image = None
    if not force_build:
        try:
            image = client.images.get(image_name)
            logger.info(f"{repo_id} : Image {image_name} found locally")
        except docker.errors.ImageNotFound:
            try:
                repository, tag = image_name.rsplit(":", 1)
                image = client.images.pull(repository, tag)
                logger.info(f"{repo_id} : Image {image_name} found in registry")
            except docker.errors.NotFound:
                # will build
                pass
        else:
            if push:
                # if found locally, push to registry
                logger.info(f"Pushing it to registry")
                repository, tag = image_name.rsplit(":", 1)
                client.images.push(repository, tag)

    if image:
        # image exists locally or in the registry
        result["build_success"] = 1
        result["build_timestamp"] = image.attrs["Created"].split(".")[0]
        # TODO build duration will be null
        # result["build_time"] =
        return result
    else:
        logger.info(f"{repo_id} : Building {image_name}")
        cmd = [
            "jupyter-repo2docker", "--ref", resolved_ref,
            "--image-name", image_name,
            "--user-name", "jovyan",
            "--user-id", "1000",
            # "--no-clean",  # False => Delete source repository after building is done
            "--no-run",
            # "--json-logs",
            # "--build-memory-limit", "?",
            # "--cache_from",  # List of images to try & re-use cached image layers from.
        ]
        if push:
            cmd.append("--push")
        cmd.append(repo_url)
        _, ts_safe = get_utc_ts()
        log_file_name = f'{repo_id}_{image_name.replace("/", "-").replace(":", "-")}_{ts_safe}.log'
        with open(os.path.join(build_log_folder, log_file_name), 'w') as log_file:
            try:
                # https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run
                container = client.containers.run(
                    image=r2d_version,
                    command=cmd,
                    name=f"{repo_id}-image-build-{script_ts_safe}",
                    volumes={
                        "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}
                    },
                    # set memory limit same as in mybinder.org:
                    # https://github.com/jupyterhub/mybinder.org-deploy/blob/4cfbd9c7975d5d8b6cccbb02974be8aca499b228/config/prod.yaml#L33
                    mem_limit="12g",
                    # https://stackoverflow.com/questions/59690457/whats-the-difference-between-auto-remove-and-remove-in-docker-sdk-for-python
                    # use detach and auto_remove together
                    # https://github.com/docker/docker-py/blob/master/docker/models/containers.py#L788-L790
                    detach=True,  # Run container in the background and return a Container object
                    # auto_remove=True,  # enable auto-removal of the container on daemon side when the container’s process exits.
                    # remove=True,  # Remove the container when it has finished running
                )
            except docker.errors.ContainerError as e:
                text = e.stderr
                if isinstance(text, bytes):
                    text = text.decode("utf8", "replace")
                log_file.write(text)
                logger.exception(f"{repo_id} : {repo_url} : build_image")
                e.container.remove(force=True)
                result["build_success"] = 0
            else:
                # ex created: '2020-08-04T13:55:56.323133607Z'
                created = datetime.fromisoformat(
                    container.attrs["Created"].rsplit(".", 1)[0]
                )
                try:
                    with Timeout(seconds=BUILD_TIMEOUT, error_message=f"Timeout ({BUILD_TIMEOUT})"):
                        # NOTE: if timeout happens while pushing the image,
                        #  then the image wont be pushed and removed but will remain in local registry
                        for log in container.logs(follow=True, stream=True):
                            if isinstance(log, bytes):
                                log = log.decode("utf8", "replace")
                            log_file.write(log)
                        status = container.wait()
                except BuildTimeoutException:
                    age = (datetime.utcnow() - created).seconds
                    log_file.write(f"Build Timed out ({age} > {BUILD_TIMEOUT})\n")
                    result["build_success"] = 0
                    result["build_time"] = -1
                    logger.info(f"{repo_id} : {image_name} : Build Timed out ({BUILD_TIMEOUT})")
                else:
                    result["build_success"] = 1 if status["StatusCode"] == 0 else 0
                    if result["build_success"]:
                        result["build_time"] = (datetime.utcnow() - created).seconds
                    logger.info(f"{repo_id} : {image_name} : {status}")
                # Remove this container. Similar to the docker rm command.
                container.remove(force=True)
        result["build_timestamp"] = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
        return result


def build_and_run_image(repo_id, repo_url, image_name, resolved_ref, buildpack):

    def get_execution():
        # return a dict with all columns
        e = {
            "repo_id": repo_id, "image_name": image_name,
            "r2d_version": r2d_version, "script_timestamp": script_ts,
            "build_success": None, "build_timestamp": None, "build_time": None,
            "notebooks_success": None,
            "nb_rel_path": None, "nb_log_file": None, "nb_success": None
        }
        return e

    execution = get_execution()
    r = build_image(repo_id, repo_url, image_name, resolved_ref)
    execution.update(r)
    if execution["build_success"] == 1:
        notebooks_success, _execution_entries = run_image(repo_id, repo_url, image_name, buildpack)
        execution["notebooks_success"] = notebooks_success
        if _execution_entries:
            execution_entries = []
            for e_e in _execution_entries:
                # get a fresh dict for each entry
                execution = get_execution()
                # and update it with results
                execution.update(r)
                execution["notebooks_success"] = notebooks_success
                execution.update(e_e)
                execution_entries.append(execution)
        else:
            # repo has no notebook
            execution_entries = [execution]
    else:
        # build was unsuccessful
        execution_entries = [execution]
    return execution_entries


def build_and_run_images(df_repos, processed):
    execution_list = []
    built_images = []
    rows = df_repos.iterrows()
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        jobs = {}
        jobs_done = 0
        index, row = next(rows)
        while True:
            if row is not None:
                # dataframe still has repo to process
                # add r2d_commit into tag,
                # so when we use a different r2d version for same repo with same resolved_ref,
                # it creates a new image (it doesnt use local or remote image from registry)
                tag = f'{r2d_commit}-{row["resolved_ref"]}'
                image_name = get_image_name(row["provider"], row["last_spec"], image_prefix, tag)
                # TODO before each job, check available disk size for docker and warn
                job = executor.submit(build_and_run_image, row["id"], row["repo_url"], image_name,
                                                           row["resolved_ref"], row["buildpack"])
                jobs[job] = f'{row["id"]}:{row["repo_url"]}'

            if (jobs and len(jobs) == max_workers) or row is None:
                # limit number of jobs with max_workers
                # "row is None" means that there is no repo left in dataframe
                # but we are still here iterating because there are still running jobs
                for job in as_completed(jobs):
                    id_repo_url = jobs[job]
                    try:
                        execution_entries = job.result()
                        logger.info(f"{id_repo_url}: {len(execution_entries)} executions done")
                        execution_list.extend(execution_entries)
                        for e in execution_entries:
                            if e["build_success"] == 1 and e["image_name"] not in built_images:
                                built_images.append(e["image_name"])
                        jobs_done += 1
                        logger.info(f"{processed} + {jobs_done} repos are processed")
                    except Exception as exc:
                        logger.exception(f"{id_repo_url}")
                    del jobs[job]
                    # break to add a new job, if there is any
                    break
            try:
                # get next repo
                index, row = next(rows)
            except StopIteration:
                # chunk is finished
                if not jobs:
                    # process next chunk
                    break
                # wait until all jobs finish
                row = None
    return execution_list, built_images


def remove_images(images):
    client = docker.from_env(timeout=DOCKER_TIMEOUT)
    for image in images:
        logger.info(f"Removing {image}")
        try:
            # Remove an image. Similar to the docker rmi command.
            client.images.remove(image, force=True, noprune=False)
        except ReadTimeout:
            logger.warning(f"Timeout while removing {image}")
    # prune dangling (unused and untagged) images
    # do this, because i think when a built fails, it leaves untagged images behind?
    try:
        client.images.prune(filters={"dangling": True})
    except ReadTimeout:
        logger.warning("Timeout for pruning dangling images. If you want to do this manually, run `docker image prune`")


def build_and_run_all_images(query, image_limit):
    start_time = datetime.now()
    msg = f"Started at {start_time}"
    if verbose:
        print(msg)
    logger.info(msg)

    db = Database(db_name)
    df_repos = pd.read_sql_query(query, db.conn, chunksize=image_limit)

    # NOTE when you update this, update also dict in get_execution()
    columns = {
                "script_timestamp": str,
                "repo_id": int,
                "image_name": str,
                "r2d_version": str,
                "build_timestamp": str,
                "build_success": int,
                # "build_error": str,
                "build_time": int,
                "notebooks_success": int,
                "nb_rel_path": str,
                # kernel_name can be parsed from execution log file of each notebook (nb_log_file)
                # "kernel_name": str,
                "nb_success": int,
                # "nb_error": str,
                # "nb_execution_time": int,
                "nb_log_file": str,
            }
    if execution_table not in db.table_names():
        db[execution_table].create(
            columns,
            # pk="image_name",
            foreign_keys=[
                ("repo_id", repo_table, "id")
            ],
            # all null by default
            # defaults={}
        )

    c = 1
    for df_chunk in df_repos:
        logger.info(f"Building images {c}*{image_limit}")
        processed = (c-1)*image_limit
        execution_list, built_images = build_and_run_images(df_chunk, processed)
        logger.info(f"Saving {len(execution_list)} executions")
        # db[execution_table].insert_all(execution_list, pk="image_name", batch_size=1000, replace=True)
        db[execution_table].insert_all(execution_list, batch_size=1000, columns=columns)
        logger.info(f"Removing images")
        remove_images(built_images)
        c += 1
    # optimize the database
    logger.info("Vacuum")
    db.vacuum()

    end_time = datetime.now()
    msg = f"duration: {end_time-start_time}"
    if verbose:
        print(f"finished at {end_time}")
        print(msg)
    logger.info(msg)


def generate_repos_query(forks=False, buildpacks=None, launches_range=None, repo_limit=0):
    # if fork is 404, it means repo doesnt exists anymore
    # 451 -> "Repository access blocked"
    # where = f'fork IS NOT null AND fork NOT IN (404, 451) '
    if forks:
        where = f'fork NOT IN (404, 451) '
    else:
        where = f'fork=0 '
    # if fork is 404, then resolved_ref is null,
    # there are also 5 repos which have resolved_ref as null, because there was error while git clone/checkout
    # if resolved_ref is "404", it means that resolved ref not found in git history
    where += f'AND resolved_ref IS NOT null AND resolved_ref!="404" '
    if buildpacks:
        where += f'AND buildpack IN ({", ".join(buildpacks)}) '
    if launches_range is not None and type(launches_range) == tuple:
        if launches_range[0] != "":
            where += f'AND launch_count>={launches_range[0]} '
        if launches_range[1] != "":
            where += f'AND launch_count<{launches_range[1]} '
    query = f"SELECT * FROM {repo_table} WHERE {where} ORDER BY first_launch_ts "
    if repo_limit > 0:
        query += f"LIMIT {repo_limit};"
    else:
        query += ";"
    return query


def convert_range(r):
    # validates and converts range from sting to tuple
    if r:
        try:
            f, t = r.split(",", 1)
            if f != "":
                f = int(f)
            if t != "":
                t = int(t)
            r = (f, t)
        except:
            raise ValueError(f'range "{r}" is in wrong format')
        else:
            return r
    else:
        return None


def get_r2d_commit(image):
    if image.endswith(".dirty"):
        image = image[:-6]
    image = image.split(":")[-1].split(".")[-1]
    if image.startswith("g"):
        image = image[1:]
    return image


def get_args():
    parser = argparse.ArgumentParser(description=f'This script runs repo2docker to build images of repos in '
                                                 f'{repo_table} table and executes each notebook of built repos '
                                                 f'and then saves results in {execution_table} table. '
                                                 f'By default it excludes repos that do no exist anymore and '
                                                 f'also repos with invalid spec (the spec of the last launch). '
                                                 f'To exclude more repos check --forks, --buildpacks '
                                                 f', --repo_limit, --launches_range flags. ',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-r2d', '--r2d_version', required=False, default=get_repo2docker_image(),
                        help='repo2docker version to be used for image building, '
                             'such as "jupyter/repo2docker:0.11.0-102.g163718b" '
                             '(https://hub.docker.com/r/jupyter/repo2docker).\n'
                             'Default is what is currently used in mybinder.org')
    parser.add_argument('-lr', '--launches_range', type=str, default='',
                        help='Range for number of launches that a repo must have to be built.\n'
                             'For example "10," is to have repos which have launches >= 10 times.\n'
                             'Default is to build images of all repos.')
    parser.add_argument('-nr', '--notebooks_range', type=str, default='',
                        help='Range for number of notebooks that a repo must have to execute notebooks.\n'
                             'For example "0,50" is to have repos which contain 0 <= # notebooks < 50.\n'
                             'Default is to execute all notebooks that the repo has.')
    parser.add_argument('-f', '--forks', required=False, default=False, action='store_true',
                        help='Build images of forked repos too. Default is False.')
    parser.add_argument('-bp', '--buildpacks', required=False, default="",
                        help='Comma-separated list of buildpacks to be processed. '
                             'Default is to process all.')
    parser.add_argument('-rl', '--repo_limit', type=int, default=0,
                        help='Use this if you want to limit number of repos to process.\n'
                             'Default is 0, which means build images of all repos.')
    parser.add_argument('-q', '--query', required=False,
                        help='Custom query to select repos from database. This overrides all other query args.')
    parser.add_argument('-fb', '--force_build', required=False, default=False, action='store_true',
                        help='Force build image, dont check if locally or in registry exists.')
    parser.add_argument('-p', '--push', required=False, default=False, action='store_true',
                        help=f'Push to remote registry. Default is False.')
    parser.add_argument('-ip', '--image_prefix', required=False, default=default_image_prefix,
                        help=f'Prefix to be prepended to image name of each repo, default is "{default_image_prefix}".')
    parser.add_argument('-il', '--image_limit', type=int, default=500,
                        help='Number of images to save locally before deleting them. Default is 500.')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Max number of processes to run in parallel. '
                                                                         'Default is 4.')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='Default is False.')
    args = parser.parse_args()
    return args


def main():
    # make variables with constant values global
    global verbose
    global logger
    global build_log_folder
    global run_output_folder
    global db_name
    global push
    global image_prefix
    global r2d_version
    global r2d_commit
    global max_workers
    global script_ts
    global script_ts_safe
    global notebooks_range
    global force_build

    args = get_args()
    db_name = args.db_name
    check_if_exists(db_name)
    r2d_version = args.r2d_version
    r2d_commit = get_r2d_commit(r2d_version)
    launches_range = convert_range(args.launches_range)
    notebooks_range = convert_range(args.notebooks_range)
    forks = args.forks
    buildpacks = ['"'+bp.strip()+'"' for bp in args.buildpacks.split(",") if bp]
    repo_limit = args.repo_limit
    query = args.query or generate_repos_query(forks, buildpacks, launches_range, repo_limit)
    image_limit = args.image_limit
    push = args.push
    force_build = args.force_build
    image_prefix = args.image_prefix
    max_workers = args.max_workers
    verbose = args.verbose

    # script_ts is used in outputs of this image (in database, logs and outputs),
    # so we can distinguish different executions in different times
    script_ts, script_ts_safe = get_utc_ts()
    # get main logger for this script
    logger_name = f'{os.path.basename(__file__)[:-3]}_at_{script_ts_safe}'
    logger = get_logger(logger_name)
    # create output folders
    build_log_folder = f"build_images/build_images_logs_{script_ts_safe}"
    create_dir(build_log_folder)
    run_output_folder = f"run_images/run_images_logs_{script_ts_safe}"
    create_dir(run_output_folder)

    if verbose:
        print(f"Logs are in {logger_name}.log")
        print(f"query: {query}")
        print(f"Using {r2d_version}")
    logger.info(query)
    logger.info(f"Using {r2d_version}")

    build_and_run_all_images(query, image_limit)
    logger.info(f"Done")
    print(f"""\n
    Building and running images is done.
    You could now open the database with `sqlite3 {db_name}` command and
    then run `select build_success, count(*) from (select build_success from {execution_table} where script_timestamp="{script_ts}" group by "repo_id") group by "build_success";` 
    to see how many repos are built successfully or not. Or 
    run `select nb_success, count(*) from {execution_table} where script_timestamp="{script_ts}" group by "nb_success";` 
    to see how many notebooks are executed successfully or not. 
    """)


if __name__ == '__main__':
    main()
