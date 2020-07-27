import docker
import argparse
import os
import pathlib
import pandas as pd
from docker.errors import APIError
from requests import ReadTimeout
from utils import get_repo2docker_image, get_logger, get_image_name, \
     REPO_TABLE as repo_table, NOTEBOOK_TABLE as notebook_table, EXECUTION_TABLE as execution_table, \
     DEFAULT_IMAGE_PREFIX as default_image_prefix
from time import strftime
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed
from sqlite_utils import Database

# time out for python docker client
DOCKER_TIMEOUT = 300


def run_image(image_name):
    # TODO
    pass


def build_image(repo_id, repo_url, image_name, resolved_ref):
    client = docker.from_env(timeout=DOCKER_TIMEOUT)
    image = None
    try:
        image = client.images.get(image_name)
    except docker.errors.ImageNotFound:
        try:
            repository, tag = image_name.rsplit(":", 1)
            image = client.images.pull(repository, tag)
        except docker.errors.NotFound:
            pass

    if image:
        # image exists locally or in the registry
        logger.info(f"Image {image_name} is already built")
        return 1
    else:
        logger.info(f"Building {image_name}")
        cmd = [
            "jupyter-repo2docker", "--ref", resolved_ref,
            "--image-name", image_name,
            "--user-name", "jovyan",
            "--user-id", "1000",
            # "--no-clean",  # False => Delete source repository after building is done
            "--no-run",
            # "--json-logs",
            # "--cache_from",  # List of images to try & re-use cached image layers from.
        ]
        if push:
            cmd.append("--push")
        cmd.append(repo_url)
        # TODO add memory, cpu limit to this build container? <- same as dind pod
        # https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run
        container = client.containers.run(
            r2d_image,
            cmd,
            volumes={
                "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}
            },
            # https://stackoverflow.com/questions/59690457/whats-the-difference-between-auto-remove-and-remove-in-docker-sdk-for-python
            # use detach and auto_remove together
            # https://github.com/docker/docker-py/blob/master/docker/models/containers.py#L788-L790
            detach=True,  # Run container in the background and return a Container object
            # auto_remove=True,  # enable auto-removal of the container on daemon side when the container’s process exits.
            # remove=True,  # Remove the container when it has finished running
        )

        log_file_name = f'{repo_id}_{image_name.replace("/", "_")}_{strftime("%Y_%m_%d_%H_%M_%S")}.log'.replace(":", "_")
        with open(os.path.join(build_log_folder, log_file_name), 'wb') as log_file:
            for log in container.logs(follow=True, stream=True):
                log_file.write(log)

        # if log_dict["phase"] == "failure":
        #     # "failure"s are from docker build
        #     # {"message": "The command '/bin/sh -c ${KERNEL_PYTHON_PREFIX}/bin/pip install --no-cache-dir -r \"requirements.txt\"' returned a non-zero code: 1", "phase": "failure"}
        #     build_success = 0
        # elif log_dict["phase"] == "failed":
        #     # ?"failed"s are from python docker client?
        #     # Ex: Error during build: UnixHTTPConnectionPool(host='localhost', port=None): Read timed out.  .... "phase": "failed"}
        #     build_success = 2
        # elif log_dict["message"].startswith("Successfully") and log_dict["phase"] == "building":
        #     # {'message': 'Successfully tagged bp20-binder-2dexamples-2drequirements-55ab5c:11cdea057c300242a30e5c265d8dc79f60f644e1\n', 'phase': 'building'}
        #     build_success = 1
        # else:
        #     # unknown - look at the log file
        #     build_success = 3

        status = container.wait()
        logger.info(f"{repo_id} : {image_name} : {status}")
        # Remove this container. Similar to the docker rm command.
        container.remove(force=True)
        build_success = 1 if status["StatusCode"] == 0 else 0

        return build_success


def build_and_run_image(repo_id, repo_url, image_name, resolved_ref):
    build_success = build_image(repo_id, repo_url, image_name, resolved_ref)
    if build_success == 1:
        # TODO execution_entries = run_image(repo_id, image_name)
        execution_entries = [{"repo_id": repo_id, "image_name": image_name, "build_success": build_success}]
    else:
        execution_entries = [{"repo_id": repo_id, "image_name": image_name, "build_success": build_success}]
    return execution_entries


def build_and_run_images(df_repos):
    execution_list = []
    built_images = []
    rows = df_repos.iterrows()
    # with ThreadPoolExecutor(max_workers=max_workers) as executor:
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        jobs = {}
        jobs_done = 0
        index, row = next(rows)
        while True:
            if row is not None:
                # A tag name must be valid ASCII and may contain lowercase and uppercase letters, digits, underscores,
                # periods and dashes.
                # A tag name may not start with a period or a dash and may contain a maximum of 128 characters.
                tag = f'{r2d_commit}-{row["resolved_ref"]}'
                image_name = get_image_name(row["provider"], row["last_spec"], image_prefix, tag)
                # TODO before each job, check available disk size for docker and warn
                job = executor.submit(build_and_run_image, row["id"], row["repo_url"], image_name, row["resolved_ref"])
                jobs[job] = f'{row["id"]}:{row["repo_url"]}'

            if (jobs and len(jobs) == max_workers) or row is None:
                # limit number of jobs with max_workers
                # row is None means there is no new job
                for job in as_completed(jobs):
                    id_repo_url = jobs[job]
                    try:
                        execution_entries = job.result()
                        execution_list.extend(execution_entries)
                        for e in execution_entries:
                            if e["build_success"] == 1 and e["image_name"] not in built_images:
                                built_images.append(e["image_name"])
                        jobs_done += 1
                    except Exception as exc:
                        logger.exception(f"{id_repo_url}")
                    del jobs[job]
                    # break to add a new job, if there is any
                    break
            try:
                # get next row
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
    db = Database(db_name)
    df_repos = pd.read_sql_query(query, db.conn, chunksize=image_limit)

    # use execution table because we filter some repos (notebooks) out
    if execution_table not in db.table_names():
        db[execution_table].create(
            {
                "repo_id": int,
                "image_name": str,
                "build_success": int,
                "nb_rel_path": str,
                "kernel_name": str,
                "nb_success": int,
            },
            pk="image_name",
            foreign_keys=[
                ("repo_id", repo_table, "id")
            ],
            # all null by default
            # defaults={}
        )
    execution = db[execution_table]

    c = 1
    for df_chunk in df_repos:
        logger.info(f"Building images {c}*{image_limit}")
        execution_list, built_images = build_and_run_images(df_chunk)
        logger.info(f"Saving {len(execution_list)} executions")
        execution.insert_all(execution_list, pk="image_name", batch_size=1000)
        logger.info(f"Removing images")
        remove_images(built_images)
        c += 1


def generate_repos_query(launch_limit=1, forks=False, buildpacks=None, repo_limit=0, notebook_limit=0):
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
    if launch_limit > 1:
        where += f'AND launch_count>={launch_limit} '
    if notebook_limit > 0:
        where += f'AND nbs_count>={notebook_limit} '
    query = f"SELECT * FROM {repo_table} WHERE {where} ORDER BY first_launch_ts "
    if repo_limit > 0:
        query += f"LIMIT {repo_limit};"
    else:
        query += ";"
    return query


def get_r2d_commit(image):
    if image.endswith(".dirty"):
        image = image[:-6]
    image = image.split(":")[-1].split(".")[-1]
    if image.startswith("g"):
        image = image[1:]
    return image


def get_args():
    parser = argparse.ArgumentParser(description=f'This script runs repo2docker to build images of repos in '
                                                 f'{repo_table} table and saves results in {execution_table} table. '
                                                 f'By default it excludes repos that do no exist anymore and '
                                                 f'also repos with invalid spec (the spec of last launch). '
                                                 f'To exclude more repos check --forks, --buildpacks '
                                                 f', --repo_limit, --launch_limit and --notebook_limit flags. ',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-r2d', '--r2d_image', required=False, default=get_repo2docker_image(),
                        help='Full image name of the repo2docker to be used for image building, '
                             'such as "jupyter/repo2docker:0.11.0-98.g8bbced7" '
                             '(https://hub.docker.com/r/jupyter/repo2docker).\n'
                             'Default is what is currently used in mybinder.org')
    parser.add_argument('-ll', '--launch_limit', type=int, default=1,
                        help='Minimum number of launches that a repo must have to be built.\n'
                             'Default is 1, which means build images of all repos.')
    parser.add_argument('-nl', '--notebook_limit', type=int, default=0,
                        help='Minimum number of notebooks that a repo must have to be built.\n'
                             'Default is 0, which means build images of all repos.')
    parser.add_argument('-f', '--forks', required=False, default=False, action='store_true',
                        help='Build images of forked repos too. Default is False.')
    parser.add_argument('-bp', '--buildpacks', required=False, default="",
                        help='Comma-separated list of buildpacks to be processed. '
                             'Default is to process all.')
    parser.add_argument('-rl', '--repo_limit', type=int, default=0,
                        help='Max number of repos to build images.\n'
                             'Default is 0, which means build images of all repos.')
    parser.add_argument('-q', '--query', required=False,
                        help='Custom query to select repos from database. This overrides all other query args.')
    parser.add_argument('-p', '--push', required=False, default=False, action='store_true',
                        help=f'Push to remote registry. Default is False.')
    parser.add_argument('-ip', '--image_prefix', required=False, default=default_image_prefix,
                        help=f'Prefix to be prepended to image name of each repo, default is "{default_image_prefix}".')
    parser.add_argument('-il', '--image_limit', type=int, default=1000,
                        help='Number of images to save locally before deleting them. Default is 1000.')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Max number of processes to run in parallel. '
                                                                         'Default is 4.')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='Default is False.')
    args = parser.parse_args()
    return args


def main():
    global verbose
    global logger
    global build_log_folder
    global run_log_folder
    global db_name
    global push
    global image_prefix
    global r2d_image
    global r2d_commit
    global max_workers

    args = get_args()
    db_name = args.db_name
    r2d_image = args.r2d_image
    r2d_commit = get_r2d_commit(r2d_image)
    launch_limit = args.launch_limit
    notebook_limit = args.notebook_limit
    forks = args.forks
    buildpacks = ['"'+bp.strip()+'"' for bp in args.buildpacks.split(",") if bp]
    repo_limit = args.repo_limit
    query = args.query or generate_repos_query(launch_limit, forks, buildpacks, repo_limit, notebook_limit)
    image_limit = args.image_limit
    push = args.push
    image_prefix = args.image_prefix
    max_workers = args.max_workers
    verbose = args.verbose

    script_ts = strftime("%Y_%m_%d_%H_%M_%S")
    logger_name = f'{os.path.basename(__file__)[:-3]}_at_{script_ts}'.replace("-", "_")
    logger = get_logger(logger_name)
    build_output_folder = "build_images"
    build_log_folder = f"{build_output_folder}/build_images_logs_{script_ts}"
    pathlib.Path(build_log_folder).mkdir(parents=True, exist_ok=True)
    run_output_folder = "run_images"
    run_log_folder = f"{run_output_folder}/run_images_logs_{script_ts}"
    pathlib.Path(run_log_folder).mkdir(parents=True, exist_ok=True)

    if verbose:
        print(f"Logs are in {logger_name}.log")
        print(f"query: {query}")
        print(f"Using {r2d_image}")
    logger.info(query)
    logger.info(f"Using {r2d_image}")

    build_and_run_all_images(query, image_limit)
    print(f"""\n
    Building images is done.
    You could now open the database with `sqlite3 {db_name}` command and 
    then run `select build_success, count(*) from {execution_table} group by "build_success";` 
    to see how many repos are built successfully or not.
    You could also run `docker image prune` to delete dangling (unused and untagged) images.
    """)


if __name__ == '__main__':
    main()
