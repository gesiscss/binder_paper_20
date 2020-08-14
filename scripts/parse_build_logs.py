"""
WIP
TODO: find other common errors why build fails.
TODO: parse also build_and_run_images_at_<ts>.log files, there are some unhandled errors
      for example some repos causes "FileNotFoundError: No such file or directory: notebooks.txt"
      and they dont have any entry in execution table.
TODO: check what other unhandled errors are in build_and_run_images_at_<ts>.log files
"""
import argparse
import os
from sqlite_utils import Database
from utils import EXECUTION_TABLE as execution_table, get_utc_ts, get_logger, check_if_exists


def get_args():
    parser = argparse.ArgumentParser(description=f'Script to parse build logs and '
                                                 f'save new data into {execution_table} table.',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-p', '--build_log_folders', required=True,
                        help='Path of build folders. If multiple, comma-separated')
    parser.add_argument('-n', '--db_name', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    # check inputs
    build_log_folders = []
    for f in args.build_log_folders.split(","):
        abs_f = os.path.abspath(f)
        check_if_exists(abs_f)
        build_log_folders.append(abs_f)
    db_name = args.db_name
    check_if_exists(db_name)

    # prepare logger
    _, script_ts_safe = get_utc_ts()
    logger_name = f'{os.path.basename(__file__)[:-3]}_at_{script_ts_safe}'.replace("-", "_")
    logger = get_logger(logger_name)
    logger.info("Start")
    print(f"Logs are in {logger_name}.log")

    db = Database(db_name)
    # add new columns
    if "buildpack" not in db[execution_table].columns_dict:
        db[execution_table].add_column("buildpack", str)
    if "build_error" not in db[execution_table].columns_dict:
        db[execution_table].add_column("build_error", str)

    # update values of new columns per each row
    for build_log_folder in build_log_folders:
        print(build_log_folder)
        logger.info(build_log_folder)
        folder_name = os.path.basename(build_log_folder)
        script_timestamp = folder_name.split("_")[-1]
        d, t = script_timestamp.split("T")
        script_timestamp = f"{d}T{t.replace('-', ':')}"

        log_files = [i for i in os.listdir(build_log_folder) if i.endswith(".log")]
        len_log_files = len(log_files)
        logger.info(f"{len_log_files} log files")
        count = 0
        for log_file in log_files:
            repo_id = int(log_file.split("_")[0])
            log_file = os.path.join(build_log_folder, log_file)
            # print(log_file)
            count += 1
            # provider_r2d = "404"
            buildpack = "404"
            build_error = "None"
            with open(log_file, "r") as f:
                for line in f:
                    line = line.rstrip()
                    # if line.startswith("Picked") and line.endswith("provider."):
                    #     provider_r2d = line.split(" ")[1]
                    if buildpack == "404" and line.startswith("Using") and line.endswith("builder"):
                        buildpack = line.split(" ")[1]
                    # TODO what are other errors, how to detect them?
                    elif build_error == "None" and "ReadTimeoutError" in line:
                        # NOTE: this doesnt catch only docker timeouts, e.g. also from pip
                        # urllib3.exceptions.ReadTimeoutError: UnixHTTPConnectionPool(host='localhost', port=None) ->
                        # this is the timeout error from docker from repo2docker
                        build_error = "ReadTimeoutError"
                        logger.info(f"{repo_id}: ReadTimeoutError: {line}")
                        break
            # save new data into tables
            new_data = {"buildpack": buildpack, "build_error": build_error}
            # print(new_data)
            if buildpack == "404":
                # TODO check why?
                #  - docker readtimeout
                #  - repo or ref doesnt exist anymore or another cloning error
                logger.warning(f"{repo_id}: {new_data}")
            # update only rows that this log file is related,
            # a repo can have many builds in different times for different r2d versions
            db.conn.execute(f"""UPDATE {execution_table}
                                SET buildpack="{buildpack}", build_error="{build_error}"
                                WHERE script_timestamp="{script_timestamp}" AND repo_id={repo_id};""")
            db.conn.commit()
            # logger.info(f"{script_timestamp} : {repo_id} : {new_data}")
            print(f'{(count*100)/len_log_files:.3f}%\r', end="")

    logger.info("Done")


if __name__ == '__main__':
    main()
