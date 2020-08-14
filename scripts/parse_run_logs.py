"""
WIP
TODO: find other common errors why notebook execution fails. some must be ModuleNotFoundError and ImportError?
"""
import argparse
import os
from sqlite_utils import Database
from utils import EXECUTION_TABLE as execution_table, get_utc_ts, get_logger, check_if_exists


def get_args():
    parser = argparse.ArgumentParser(description=f'Script to parse run logs and '
                                                 f'save new data into {execution_table} table.',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-t', '--script_timestamps', required=False, default="",
                        help='Timestamp to select executions from db. If multiple, comma-separated. '
                             'Default is all executions.')
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    script_timestamps = ['"'+st.strip()+'"' for st in args.script_timestamps.split(",") if st]
    db_name = args.db_name
    check_if_exists(db_name)

    _, script_ts_safe = get_utc_ts()
    logger_name = f'{os.path.basename(__file__)[:-3]}_at_{script_ts_safe}'.replace("-", "_")
    logger = get_logger(logger_name)
    logger.info("Start")
    print(f"Logs are in {logger_name}.log")

    db = Database(db_name)
    # add new columns
    if "kernel_name" not in db[execution_table].columns_dict:
        db[execution_table].add_column("kernel_name", str)
    if "nb_execution_time" not in db[execution_table].columns_dict:
        db[execution_table].add_column("nb_execution_time", int)
    if "nb_error" not in db[execution_table].columns_dict:
        db[execution_table].add_column("nb_error", str)

    execution_count = 0
    if script_timestamps:
        executions = db[execution_table].rows_where(f'nb_log_file is not null AND '
                                                    f'script_timestamp IN ({", ".join(script_timestamps)})')
    else:
        executions = db[execution_table].rows_where('nb_log_file is not null')
    # add values of new columns per each row
    for execution in executions:
        execution_count += 1
        nb_log_file = execution["nb_log_file"]
        # TODO parse also logs of notebooks detection: use repo folder, it is in form of "notebooks_<TS>.log"
        # repo_folder = os.path.dirname(nb_log_file)
        # repo_id = int(repo_folder.split("_")[0])
        # assert repo_id == execution["repo_id"]
        # repo_id = execution["repo_id"]

        # print(nb_log_file)
        kernel_name = "404"
        nb_execution_time = 404
        nb_error = "None"
        with open(nb_log_file, "r") as f:
            for line in f:
                line = line.rstrip()
                if kernel_name == "404" and "execute:" in line and "Executing notebook with kernel:" in line:
                    kernel_name = line.split(" ")[-1]
                elif nb_execution_time == 404 and "inrepo:" in line and "Execution time is" in line:
                    nb_execution_time = int(line.split(" ")[-1])
                # TODO what are other errors, how to detect them?
                elif nb_error == "None" and "execute:" in line and "Timeout waiting for execute reply" in line:
                    nb_error = "TimeoutError"
                    break
        # save new data into tables
        # new_data = {"kernel_name": kernel_name, "nb_execution_time": nb_execution_time, "nb_error": nb_error}
        # if kernel_name == "404":
        #     logger.warning(f"{repo_id} : {nb_log_file} : {new_data}")
        # if nb_error == "None" and nb_execution_time == 404:
        #     logger.warning(f"undetected error: {repo_id} : {nb_log_file}")

        # row = list(
        #     db[execution_table].rows_where(f'nb_log_file="{nb_log_file}"')
        # )
        # if not row:
        #     logger.error(f"Error: {repo_id}: nb log file doesnt exist: {nb_log_file} : {new_data}")
        # elif len(row) > 1:
        #     logger.error(f"Error: {repo_id}: nb log file must be unique: {nb_log_file} : {new_data}")
        # else:
        db.conn.execute(f"""UPDATE {execution_table}
                            SET kernel_name="{kernel_name}", nb_execution_time={nb_execution_time}, nb_error="{nb_error}"
                            WHERE nb_log_file="{nb_log_file}";""")
        db.conn.commit()
        # logger.info(f"{nb_log_file} : {new_data}")
        print(f'{execution_count}\r', end="")

    logger.info(f"{execution_count} executions")
    logger.info("Done")


if __name__ == '__main__':
    main()
