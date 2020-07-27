"""
Script to extract repo data from launch events table.
"""
import argparse
import pandas as pd
from datetime import datetime
from concurrent.futures.process import ProcessPoolExecutor
# from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
from sqlite_utils import Database
from time import strftime, sleep
from utils import get_ref, get_repo_data_from_github_api, get_logger, GithubException, \
    get_repo_data_from_git, LAUNCH_TABLE as launch_table, REPO_TABLE as repo_table, NOTEBOOK_TABLE as notebook_table


def get_repos_from_launch_table(db, providers, launch_limit):
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
    # https://developer.github.com/v3/#rate-limiting
    chunk_size = 1000
    # NOTE: this query orders launch table by timestamp and creates a temporary table t
    # and this temporary table is used to select and also to concat specs, so we have specs in time order and
    # we can use the last launched one to fetch resolved_ref
    # BUT sqlite docs (https://www.sqlite.org/lang_aggfunc.html#groupconcat) says
    # GROUP_CONCAT: The order of the concatenated elements is arbitrary.
    # that's why first we concat timestamp and spec (ts_spec) and then concat all ts_specs of a repo
    # later we will sort and get last launched spec
    query = f"""SELECT t.provider AS provider, t.repo_url AS repo_url, 
                       COUNT(t.repo_url) AS launch_count, 
                       MIN(t.timestamp) AS first_launch_ts, 
                       MAX(t.timestamp) AS last_launch_ts, 
                       GROUP_CONCAT(DISTINCT t.ts_spec) AS ts_specs 
                FROM (SELECT provider, repo_url, timestamp, 
                             (timestamp || ";" || spec) AS ts_spec, 
                             ref, resolved_ref 
                      FROM {launch_table} 
                      ORDER BY timestamp) AS t 
                WHERE provider IN ({", ".join(providers)}) 
                GROUP BY repo_url 
                HAVING launch_count > {launch_limit} 
                ORDER BY first_launch_ts;"""
    logger.info(query)
    df_iter = pd.read_sql_query(query, db.conn, chunksize=chunk_size)
    count = db.conn.execute(f"SELECT count(*) FROM ({query[:-1]});").fetchone()[0]
    return df_iter, count


def get_repo_data(repo_entry, access_token):
    retry = 3
    while retry:
        try:
            repo_data = get_repo_data_from_github_api(repo_entry["provider"], repo_entry["repo_url"], access_token)
            repo_entry.update(repo_data)
            ref = get_ref(repo_entry["provider"], repo_entry["last_spec"])
            repo_entry["ref"] = ref
            if repo_entry["fork"] in [0, 1]:
                repo_data = get_repo_data_from_git(ref, repo_entry["repo_url"])
                repo_entry.update(repo_data)
        except GithubException as e:
            if e.status == 403:
                # github.GithubException.RateLimitExceededException: 403
                # https://developer.github.com/v3/#rate-limiting
                minutes_until_reset = e.data["minutes_until_reset"]
                msg = f'{repo_entry["repo_url"]}: Rate limit error, will try again after sleeping {minutes_until_reset} minutes'
                logger.info(msg)
                if verbose:
                    print(msg)
                sleep(minutes_until_reset * 60)
                # continue to process last repo again
                continue
            else:
                repo_entry["fork"] = e.status
                logger.info(f'Error while processing {repo_entry["repo_url"]}, attempt {4 - retry}')
                logger.exception(f'{repo_entry["repo_url"]}')
                sleep(retry ** retry)
                retry -= 1
                continue
        except Exception as e:
            logger.info(f'Error while processing {repo_entry["repo_url"]}, attempt {4 - retry}')
            logger.exception(f'{repo_entry["repo_url"]}')
            sleep(retry ** retry)
            retry -= 1
            continue
        else:
            break
    return repo_entry


def create_repo_table(db_name, providers, launch_limit, access_token=None, max_workers=4):
    start_time = datetime.now()
    if verbose:
        print(f"creating repo table, started at {start_time}")

    db = Database(db_name)
    if repo_table in db.table_names():
        raise Exception(f"table {repo_table} already exists in {db_name}")
    else:
        # create repo table with id column as primary key
        db[repo_table].create(
            {
                "id": int,
                # there will be repos with same remote_id, because they are renamed
                "remote_id": str,
                "provider": str,
                "repo_url": str,
                "first_launch_ts": str,
                "last_launch_ts": str,
                "last_spec": str,
                "ref": str,
                "resolved_ref": str,
                # date when resolved_ref is fetched
                "resolved_date": str,
                # commit date of resolved_ref
                "resolved_ref_date": str,
                "fork": int,
                "renamed": int,
                "launch_count": int,
                "binder_dir": str,
                "buildpack": str,
                "nbs_count": int,
            },
            pk="id",
        )
        repos = db[repo_table]

    if access_token:
        if notebook_table in db.table_names():
            raise Exception(f"table {notebook_table} already exists in {db_name}")
        else:
            db[notebook_table].create(
                {
                    "repo_id": int,
                    "nb_rel_path": str,
                },
                foreign_keys=[
                    ("repo_id", repo_table, "id")
                ],
            )
            notebooks = db[notebook_table]

    repos_df_iter, count = get_repos_from_launch_table(db, providers, launch_limit)
    if verbose:
        msg = f"{count} repos will be processed"
        logger.info(msg)
        print(msg)
    repo_count = 0
    notebook_count = 0
    jobs_done = 0
    # internal id
    id_ = 0
    for df_chunk in repos_df_iter:
        repos_list = []
        notebooks_list = []
        rows = df_chunk.iterrows()
        # with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            jobs = {}
            index, row = next(rows)
            while True:
                if row is not None:
                    id_ += 1
                    # use spec of the last launch for resolved_ref
                    ts_specs = [ts_spec.split(";") for ts_spec in row["ts_specs"].split(",")]
                    # sort by first element, which is timestamp
                    ts_specs = sorted(ts_specs, key=lambda x: x[0])
                    last_spec = ts_specs[-1][1]
                    repo_entry = {
                        "id": id_, "last_spec": last_spec,
                        "provider": row["provider"], "repo_url": row["repo_url"],
                        "first_launch_ts": row["first_launch_ts"], "last_launch_ts": row["last_launch_ts"],
                        "launch_count": row["launch_count"],
                        }

                    if access_token:
                        job = executor.submit(get_repo_data, repo_entry, access_token)
                        jobs[job] = f'{id_}:{row["repo_url"]}'
                    else:
                        repos_list.append(repo_entry)

                if (jobs and len(jobs) == max_workers) or row is None:
                    # limit number of jobs with max_workers
                    # row is None means there is no new job
                    for job in as_completed(jobs):
                        id_repo_url = jobs[job]
                        try:
                            r = job.result()
                            if "notebooks" in r and r["notebooks"]:
                                for nb_rel_path in r["notebooks"]:
                                    notebooks_list.append({"repo_id": r["id"], "nb_rel_path": nb_rel_path})
                                del r["notebooks"]
                            elif "notebooks" in r:
                                del r["notebooks"]
                            repos_list.append(r)
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

        repos.insert_all(repos_list, pk="id")
        repo_count += len(df_chunk)
        msg = f"{repo_count} ({jobs_done}) repos are processed"
        if access_token:
            msg += f"\n now going to save {len(notebooks_list)} notebooks"
        logger.info(msg)
        if verbose:
            print(msg)

        if access_token:
            notebooks.insert_all(notebooks_list, batch_size=1000)
            notebook_count += len(notebooks_list)
        # print(df_chunk.dtypes)

    if access_token:
        if verbose:
            print("Detecting renamed repos")
        # detect renamed repos (rows with same remote id)
        # and set renamed to 0 or to number of time that repo is named
        # for non-existing repos it will stay as None (default)
        db.conn.execute(f"""UPDATE {repo_table} SET renamed=(SELECT COUNT(remote_id)-1 
                                                     FROM {repo_table} as r 
                                                     WHERE r.remote_id={repo_table}.remote_id AND 
                                                           r.provider={repo_table}.provider) 
                            WHERE remote_id IS NOT null;""")
        db.conn.commit()

        # add repo_id fk into launch table
        if verbose:
            print("Adding repo_id fk into launch table")
        if "repo_id" not in db[launch_table].columns_dict:
            db[launch_table].add_column("repo_id", fk=repo_table, fk_col="id")
        db.conn.execute(f"""UPDATE {launch_table}
                            SET repo_id=(SELECT id
                                         FROM {repo_table}
                                         WHERE repo_url={launch_table}.repo_url);""")
        db.conn.commit()

    # optimize the database
    if verbose:
        print("Vacuum")
    db.vacuum()
    end_time = datetime.now()
    msg = f"repo table is created with {repo_count} ({count}) entries"
    msg += f"\nnotebook table is created with {notebook_count} entries"
    msg += f"\nduration: {end_time - start_time}"
    if verbose:
        print(f"finished at {end_time}")
        print(msg)
    logger.info(msg)


def get_args():
    parser = argparse.ArgumentParser(description=f'This script extracts repo data from `{launch_table}` table and '
                                                 f'saves this data into `{repo_table}` table. '
                                                 f'Note that this script for now only works '
                                                 f'for GitHub and Gist repo providers.'
                                                 f'\nExample command to create repo data from example.db: '
                                                 f'\n\tpython create_repo_table.py -v -n example.db',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-t', '--access_token', required=False,
                        help='Access token for GitHub API. If access token is not provided, '
                             'these additional data will not be fetched: '
                             '`fork`, `remote_id`, `resolved_ref`, `buildpack`, `notebooks`, `renamed`...\n'
                             'Without authentication GitHub API allows 60 requests per hour, '
                             'with authentication it is 5000 (https://developer.github.com/v3/#rate-limiting).\n'
                             'To create one: https://github.com/settings/tokens/new')
    # parser.add_argument('-f', '--providers', required=False, default="GitHub",
    #                     help='Comma-separated list of providers to filter. '
    #                          'Default is "GitHub,Gist".'
    #                          'To include all: "GitHub,Gist,Git,GitLab,Zenodo,Figshare,Hydroshare,Dataverse"')
    parser.add_argument('-l', '--launch_limit', type=int, default=0,
                        help='Minimum number of launches that a repo must have to be saved. '
                             'Default is 0, which means save all repos.')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Max number of processes to run in parallel. '
                                                                         'Default is 4.')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='Default is False.')
    args = parser.parse_args()
    return args


def main():
    global verbose
    global logger

    args = get_args()
    db_name = args.db_name
    # providers = ['"'+p.strip()+'"' for p in args.providers.split(",")]
    providers = ['"'+p.strip()+'"' for p in "GitHub,Gist".split(",")]
    launch_limit = args.launch_limit
    access_token = args.access_token
    if not access_token:
        access_token = None
        print("No token for GitHub API, no additional data will be fetched from GitHub API.")
    max_workers = args.max_workers
    verbose = args.verbose

    logger_name = f'create_repo_table_at_{strftime("%Y_%m_%d_%H_%M_%S")}'.replace("-", "_")
    logger = get_logger(logger_name)
    if verbose:
        print(f"Logs are in {logger_name}.log")

    create_repo_table(db_name, providers, launch_limit, access_token, max_workers)
    print(f"""\n
    Repo data is extracted from `{launch_table}` table and saved into `{repo_table}` table.
    You can open this database with `sqlite3 {db_name}` command and then run any sqlite3 command, 
    e.g., `select count(*) from {repo_table}` to get number of repos.
    """)


if __name__ == '__main__':
    main()
