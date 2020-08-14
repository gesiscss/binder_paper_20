import argparse
import os
from sqlite_utils import Database
from utils import EXECUTION_TABLE as execution_table, get_utc_ts, get_logger


def get_args():
    parser = argparse.ArgumentParser(description=f'Script to parse run logs and '
                                                 f'save new data into {execution_table} table.',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-p', '--run_log_folders', required=True,
                        help='Path of run log folders. If multiple, comma-separated')
    parser.add_argument('-n', '--db_name', required=True)
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    # check inputs
    run_log_folders = []
    for f in args.run_log_folders.split(","):
        abs_f = os.path.abspath(f)
        if not os.path.exists(abs_f):
            raise FileNotFoundError(f"{f} doesnt exist")
        run_log_folders.append(abs_f)
    db_name = args.db_name
    if not os.path.exists(db_name):
        raise FileNotFoundError(f"database {db_name} doesnt exist")

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

    current_dir = os.path.dirname(os.path.realpath(__file__))
    # add values of new columns per each row
    for run_log_folder in run_log_folders:
        print(run_log_folder)
        logger.info(run_log_folder)
        # folder_name = os.path.basename(run_log_folder)
        # script_timestamp = folder_name.split("_")[-1]
        # d, t = script_timestamp.split("T")
        # script_timestamp = f"{d}T{t.replace('-', ':')}"

        repo_folders = [f for f in os.listdir(run_log_folder) if os.path.isdir(os.path.join(run_log_folder, f))]
        len_repo_folders = len(repo_folders)
        logger.info(f"{len_repo_folders} log folders")
        count = 0
        # print(repo_folders)
        for repo_folder in repo_folders:
            count += 1
            repo_id = int(repo_folder.split("_")[0])
            # TODO parse also logs of notebooks detection (.startswith("notebooks_"))
            notebook_log_files = [i for i in os.listdir(os.path.join(run_log_folder, repo_folder))
                                  if i.startswith("notebooks-") and i.endswith(".log")]
            # print(notebook_log_files)
            for log_file in notebook_log_files:
                kernel_name = "404"
                nb_execution_time = "404"
                nb_error = "None"
                log_file = os.path.join(run_log_folder, repo_folder, log_file)
                with open(log_file, "r") as f:
                    for line in f:
                        line = line.rstrip()
                        if kernel_name == "404" and "execute:" in line and "Executing notebook with kernel:" in line:
                            kernel_name = line.split(" ")[-1]
                        elif nb_execution_time == "404" and "inrepo:" in line and "Execution time is" in line:
                            nb_execution_time = int(line.split(" ")[-1])
                        # TODO what are other errors, how to detect them?
                        elif nb_error == "None" and "execute:" in line and "Timeout waiting for execute reply" in line:
                            nb_error = "TimeoutError"
                            break
                # save new data into tables
                new_data = {"kernel_name": kernel_name, "nb_execution_time": nb_execution_time, "nb_error": nb_error}
                if kernel_name == "404":
                    logger.warning(f"{repo_id}: {new_data}")
                if nb_error == "None" and nb_execution_time == "404":
                    logger.info(f"undetected error: {repo_id} : {log_file}")
                nb_log_file = os.path.relpath(log_file, current_dir)
                row = list(
                    db[execution_table].rows_where(f'nb_log_file="{nb_log_file}"')
                )
                if not row:
                    logger.error(f"Error: {repo_id}: nb log file doesnt exist: {nb_log_file} : {new_data}")
                elif len(row) > 1:
                    logger.error(f"Error: {repo_id}: nb log file must be unique: {nb_log_file} : {new_data}")
                else:
                    db.conn.execute(f"""UPDATE {execution_table} 
                                        SET kernel_name="{kernel_name}", nb_execution_time={nb_execution_time}, nb_error="{nb_error}" 
                                        WHERE nb_log_file="{nb_log_file}";""")
                    db.conn.commit()
                # logger.info(f"{repo_id} : {new_data}")
                print(f'{(count*100)/len_repo_folders}%\r', end="")

    logger.info("Done")


if __name__ == '__main__':
    main()
