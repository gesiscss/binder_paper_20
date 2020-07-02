import docker
import argparse
import json
import os
from utils import get_repo2docker_image, get_logger
from time import strftime
from datetime import datetime
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed
from sqlite_utils import Database

client = docker.from_env()


def build_image(r2d_image, repo, ref, image_name, row_id, log_folder="logs"):
    """
    Build an image given a repo, ref and limits
    based on https://github.com/jupyterhub/binderhub/blob/master/binderhub/build.py
    and https://github.com/plasmabio/tljh-repo2docker/blob/master/tljh_repo2docker/docker.py#L57
    """
    # return row_id, 12
    # TODO --push
    push = False
    if push:
        image_name = "TODO/" + image_name
    cmd = [
        "jupyter-repo2docker", "--ref", ref,
        "--image-name", image_name,
        "--user-name", "jovyan",
        "--user-id", "1000",
        # "--no-clean",  # False => Delete source repository after building is done
        "--no-run",
        "--json-logs",
        # "--build-memory-limit", "?",
        # "--cache_from",  # List of images to try & re-use cached image layers from.
    ]
    if push:
        cmd.append("--push")
    cmd.append(repo)
    # TODO add memory, cpu limit to this build container?
    # https://docker-py.readthedocs.io/en/stable/containers.html#docker.models.containers.ContainerCollection.run
    container = client.containers.run(
        r2d_image,
        cmd,
        labels={
            # similar labels added to built image by repo2docker
            # https://github.com/jupyter/repo2docker/blob/8bbced7ded5a21b581f1f3846ffc9f87944ba799/repo2docker/buildpacks/base.py#L165
            # https://github.com/jupyter/repo2docker/blob/8bbced7ded5a21b581f1f3846ffc9f87944ba799/repo2docker/buildpacks/base.py#L562
            "repo2docker.image": r2d_image,
            "repo2docker.repo": repo,
            "repo2docker.ref": ref,
            "repo2docker.build": image_name,
        },
        volumes={
            "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"}
        },
        detach=True,  # Run container in the background and return a Container object
        remove=True,  # Remove the container when it has finished running
    )
    if not os.path.exists(log_folder):
        os.mkdir(log_folder)
    log_file_name = f"{row_id}-{image_name.split(':')[0]}.log"
    with open(os.path.join(log_folder, log_file_name), 'wb') as log_file:
        for log in container.logs(stream=True):
            log_file.write(log)
            # TODO analyse/parse these logs
            # print(log)
    log_dict = json.loads(log)
    print(log_dict)
    if log_dict["phase"] == "failure":
        # {"message": "The command '/bin/sh -c ${KERNEL_PYTHON_PREFIX}/bin/pip install --no-cache-dir -r \"requirements.txt\"' returned a non-zero code: 1", "phase": "failure"}
        build_success = 0
    elif log_dict["message"].startswith("Successfully") and log_dict["phase"] == "building":
        # {'message': 'Successfully tagged bp20-binder-2dexamples-2drequirements-55ab5c:11cdea057c300242a30e5c265d8dc79f60f644e1\n', 'phase': 'building'}
        build_success = 1
    else:
        # unknown - look at the log file
        build_success = 2
    return row_id, build_success


def build_images(db_name, r2d_image, launch_limit=0, forks=False, max_workers=1, continue_=False, verbose=False):
    if verbose:
        start_time = datetime.now()
        print(f"building images, started at {start_time}")
        repo_count = 0
        repo_count_ = 0

    repo_table = "repo"
    db = Database(db_name)
    where = f"resolved_ref_now!=404 AND launch_count>{launch_limit}"
    if not forks:
        where = f"fork=0 AND " + where
    if "build_success" in db[repo_table].columns_dict:
        if continue_:
            where += " AND build_success IS null"
        else:
            raise Exception(f"{repo_table} in {db_name} is already processed. "
                            f"If you want to continue, pass `--cont` flag."
                            f"Or if you want to re-process everything, "
                            f"you could rename `build_success` column manually, "
                            f"e.g. `ALTER TABLE {repo_table} RENAME COLUMN build_success to build_success_old;`")
    rows = db[repo_table].rows_where(where)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        logger_name = f'build_images_at_{strftime("%Y_%m_%d_%H_%M_%S")}'.replace("-", "_")
        logger = get_logger(logger_name)
        jobs = {}
        log_folder = logger_name+"_logs"
        row = next(rows)
        while True:
            # print(row)
            if row:
                job = executor.submit(build_image, r2d_image,
                                                   repo=row["repo_url"],
                                                   ref=row["resolved_ref_now"],
                                                   image_name=row["image_name"],
                                                   row_id=row["id"],
                                                   log_folder=log_folder)
                jobs[job] = f'{row["id"]}:{row["repo_url"]}'
                if verbose:
                    repo_count += 1

            # limit number of jobs with max_workers
            if len(jobs) == max_workers or not row:
                for job in as_completed(jobs):
                    id_repo_url = jobs[job]
                    try:
                        row_id, build_success = job.result()
                        # update row with build info
                        db[repo_table].update(row_id, {"build_success": build_success}, alter=True)
                        logger.info(f"{row_id}: build success: {build_success}")
                        if verbose:
                            repo_count_ += 1
                    except Exception as exc:
                        logger.exception(f"{id_repo_url}")

                    del jobs[job]
                    # break to add a new job, if there is any
                    break

            try:
                row = next(rows)
            except StopIteration:
                # continue creating new jobs until reaching to last row
                # or wait until all jobs finish
                if not jobs:
                    break
                row = None

    # optimize the database
    db.vacuum()
    if verbose:
        end_time = datetime.now()
        print(f"images are built for {repo_count} ({repo_count_}) repos")
        print(f"finished at {end_time}")
        print(f"duration: {end_time - start_time}")


def get_args():
    # TODO desc
    parser = argparse.ArgumentParser(description='.')
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-r', '--r2d_image', required=False,
                        help='Full image name of the repo2docker to be used for image building, '
                             'such as "jupyter/repo2docker:0.11.0-98.g8bbced7" '
                             '(https://hub.docker.com/r/jupyter/repo2docker).'
                             'Default is what is currently used in mybinder.org')
    parser.add_argument('-l', '--launch_limit', type=int, default=0,
                        help='Minimum number of launches for a repo to be built. '
                             'Default is 0, which means build images all repos.')
    parser.add_argument('-f', '--forks', required=False, default=False, action='store_true',
                        help='Build images of forks too. Default is False')
    parser.add_argument('-c', '--cont', required=False, default=False, action='store_true',
                        help='if this script already executed before. default is False')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Default is 4')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='default is False')
    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    db_name = args.db_name
    r2d_image = args.r2d_image
    if not r2d_image:
        r2d_image = get_repo2docker_image()
    print(f"Using {r2d_image}")
    launch_limit = args.launch_limit
    forks = args.forks
    continue_ = args.cont
    max_workers = args.max_workers
    verbose = args.verbose

    build_images(db_name, r2d_image, launch_limit, forks, max_workers, continue_, verbose)
