"""
This script is WIP!
"""
import docker
import argparse
import os
from docker.errors import APIError
from utils import get_logger, REPO_TABLE as repo_table
from time import strftime
from datetime import datetime
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed
from sqlite_utils import Database


def run_image(image_name, row_id, log_folder="logs"):
    output_dir_name = "run_images"
    # NOTE: as far as i understand r2d always installs python3 kernel
    command = ["python3", f"/{output_dir_name}/analyse_notebooks.py"]
    current_dir_name = os.path.dirname(os.path.realpath(__file__))
    output_dir = os.path.join(current_dir_name, output_dir_name)

    # default timeout is 60 seconds
    # NOTE: this timeout has no effect on the client that repo2docker works with inside the build container
    # client = docker.from_env(timeout=120)
    client = docker.from_env()
    container = client.containers.run(
        image=image_name,
        command=command,
        # labels = {},
        volumes={
            "/var/run/docker.sock": {"bind": "/var/run/docker.sock", "mode": "rw"},
            f"{output_dir}": {"bind": f"/{output_dir_name}", "mode": "rw"},
        },
        # use detach and auto_remove together
        # https://github.com/docker/docker-py/blob/master/docker/models/containers.py#L788-L790
        detach=True,  # Run container in the background and return a Container object
        auto_remove=True,  # enable auto-removal of the container on daemon side when the container’s process exits.
        # remove=True,  # Remove the container when it has finished running
    )
    log_file_name = f"{row_id}-{image_name.split(':')[0]}.log"
    with open(os.path.join(log_folder, log_file_name), 'wb') as log_file:
        contains_nbs = None
        for log in container.logs(stream=True):
            log_file.write(log)
            # TODO
            log = log.decode().strip()
            # print("# ", log)
            if "contains_nbs" in log:
                log_dict = eval(log)
                contains_nbs = int(log_dict["contains_nbs"])

    # ensure that container is removed
    try:
        # Remove this container. Similar to the docker rm command.
        container.remove(force=True)
    except APIError:
        # removal is already in progress
        pass
    return row_id, contains_nbs


def run_images(db_name, max_workers=1, continue_=False, verbose=False):
    if verbose:
        start_time = datetime.now()
        print(f"running images, started at {start_time}")

    db = Database(db_name)
    where = "build_success=1"
    if "contains_nbs" in db[repo_table].columns_dict:
        if continue_:
            where += " AND contains_nbs IS null"
        else:
            # TODO other columns
            raise Exception(f"{repo_table} in {db_name} is already processed. "
                            f"If you want to continue, pass `--cont` flag."
                            f"Or if you want to re-process everything, "
                            f"you could rename `contains_nbs` column manually, "
                            f"e.g. `ALTER TABLE {repo_table} RENAME COLUMN contains_nbs to contains_nbs_old;`")
    if verbose:
        jobs_created = 0
        jobs_done = 0
    rows = db[repo_table].rows_where(where)
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        logger_name = f'run_images_at_{strftime("%Y_%m_%d_%H_%M_%S")}'.replace("-", "_")
        logger = get_logger(logger_name)
        jobs = {}
        log_folder = logger_name+"_logs"
        os.mkdir(log_folder)
        row = next(rows)
        while True:
            # print(row)
            if row:
                job = executor.submit(run_image, image_name=row["image_name"],
                                                 row_id=row["id"],
                                                 log_folder=log_folder)
                jobs[job] = f'{row["id"]}:{row["repo_url"]}'
                if verbose:
                    jobs_created += 1

            # limit number of jobs with max_workers
            if len(jobs) == max_workers or not row:
                for job in as_completed(jobs):
                    id_repo_url = jobs[job]
                    try:
                        row_id, contains_nbs = job.result()
                        # update row with build info
                        db[repo_table].update(row_id, {"contains_nbs": contains_nbs}, alter=True)
                        logger.info(f"{row_id}: build contains_nbs: {contains_nbs}")
                        if verbose:
                            jobs_done += 1
                            print(f"{jobs_done} images are processed")
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
        print(f"images are built for {jobs_created} ({jobs_done}) repos")
        print(f"finished at {end_time}")
        print(f"duration: {end_time - start_time}")


def get_args():
    parser = argparse.ArgumentParser(description='This script is WIP!')
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-c', '--cont', required=False, default=False, action='store_true',
                        help='if this script already executed before. default is False')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Max number of processes to run in parallel. '
                                                                         'Default is 4')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='Default is False.')
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    db_name = args.db_name
    continue_ = args.cont
    max_workers = args.max_workers
    verbose = args.verbose

    run_images(db_name, max_workers, continue_, verbose)


if __name__ == '__main__':
    main()
