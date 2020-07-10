"""
Parser for mybinder.org events archive (https://archive.analytics.mybinder.org/)
"""
import argparse
import pandas as pd
from datetime import datetime, timedelta
from time import strftime
from sqlite_utils import Database
from concurrent.futures.process import ProcessPoolExecutor
from concurrent.futures import as_completed
from utils import get_ref, get_org, get_repo_url, get_logger, LAUNCH_TABLE as launch_table


def parse_spec(provider, spec):
    ref = get_ref(provider, spec)
    org = get_org(provider, spec)
    # NOTE: repo_url must be unique, e.g. it must be same for specs
    # such as "1-Nameless-1/Lign167.git/master" and "1-Nameless-1/Lign167/master"
    # so generate repo_urls here instead of in create_repo_table.py
    repo_url = get_repo_url(provider, spec)
    return ref, org, repo_url


def _handle_exceptions_in_archve(df, a_name):
    # events before 12.06.2019 has no origin value
    if 'origin' not in df.columns:
        df["origin"] = "mybinder.org"
    # events-2019-06-12.jsonl has mixed rows: with and without origin value
    if a_name == "events-2019-06-12.jsonl":
        df['origin'].fillna('mybinder.org', inplace=True)
    # events before 12.06.2019 has no (resolved) ref
    if 'ref' not in df.columns:
        # TODO we could use utils.get_resolved_ref(timestamp, provider, spec) when it is implemented
        df['ref'] = ""
    # events-2020-06-18.jsonl has mixed rows: with and without (resolved) ref value
    if a_name == "events-2020-06-18.jsonl":
        df['ref'].fillna('', inplace=True)
    # in some archives Gist launches have wrong provider (GitHub)
    elif a_name == "events-2018-11-25.jsonl":
        df.loc[df['spec'] == "https%3A%2F%2Fgist.github.com%2Fjakevdp/256c3ad937af9ec7d4c65a29e5b6d454", "provider"] = "Gist"
        df.loc[df['spec'] == "https%3A%2F%2Fgist.github.com%2Fjakevdp/256c3ad937af9ec7d4c65a29e5b6d454", "spec"] = "jakevdp/256c3ad937af9ec7d4c65a29e5b6d454"
    elif a_name == "events-2019-01-28.jsonl":
        df.loc[df['spec'] == "loicmarie/ade5ea460444ea0ff72d5c94daa14500", "provider"] = "Gist"
    elif a_name == "events-2019-02-22.jsonl":
        df.loc[df['spec'] == "minrk/6d61e5edfa4d2947b0ee8c1be8e79154", "provider"] = "Gist"
    elif a_name == "events-2019-03-05.jsonl":
        df.loc[df['spec'] == "vingkan/25c74b0e1ea87110a740a9c29a901200", "provider"] = "Gist"
    elif a_name == "events-2019-03-07.jsonl":
        df.loc[df['spec'] == "bitnik/2b5b3ad303859663b222fa5a6c2d3726", "provider"] = "Gist"
    return df


def parse_archive(archive_date, db_name):
    """parse archive of given date and save into the database
    returns number of saved events"""
    a_name = f"events-{str(archive_date)}.jsonl"
    archive_url = f"https://archive.analytics.mybinder.org/{a_name}"

    # first read events from archive
    df = pd.read_json(archive_url, lines=True)
    # drop columns that we dont need for analysis
    # df = df.drop(["schema", "version", "status"], axis=1)
    df = df.drop(["schema", "status"], axis=1)

    # handle exceptions in events archive
    df = _handle_exceptions_in_archve(df, a_name)

    # rename ref to resolved_ref, we will get ref from spec
    # resolved ref is the one which is passed to repo2docker for build
    df.rename(columns={'ref': 'resolved_ref'}, inplace=True)

    # generate new columns that we might need for analysis
    df[["ref", "org", "repo_url"]] = df.apply(lambda row: parse_spec(row["provider"], row["spec"]),
                                              axis=1,
                                              result_type='expand')

    # re-order columns, so more readable
    df = df[['timestamp', 'version', 'origin', 'provider', 'spec', 'org', 'ref', 'resolved_ref', 'repo_url']]

    # save into database, without index
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    # first connect to db
    db = Database(db_name)
    df.to_sql(launch_table, con=db.conn, if_exists="append", index=False)

    return len(df)


def parse_mybinder_archive(start_date, end_date, db_name, max_workers=1, verbose=False):
    if verbose:
        start_time = datetime.now()
        print(f"parsing started at {start_time}")
    start_date = datetime.strptime(start_date, '%Y-%m-%d').date()
    end_date = datetime.strptime(end_date, '%Y-%m-%d').date()
    assert start_date <= end_date, f"start_date: {start_date}, end_date: {end_date}"

    one_day = timedelta(days=1)
    current_date = start_date
    if verbose:
        counter = 0
        total_events = 0

    db = Database(db_name)
    if launch_table in db.table_names():
        raise Exception(f"table {launch_table} already exists in {db_name}")

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        logger_name = db_name[:-3]
        logger_name = "parse_mybinder_archive" + "_at_" + logger_name.split("_at_")[-1]
        logger = get_logger(logger_name)
        jobs = {}
        while current_date <= end_date or jobs:
            # continue creating new jobs until reaching to last date
            # or wait until all jobs finish
            while current_date <= end_date:
                if verbose:
                    print(f"parsing archive of {current_date}")
                job = executor.submit(parse_archive, current_date, db_name)
                jobs[job] = str(current_date)
                current_date += one_day
                if verbose:
                    counter += 1
                # limit number of jobs with max_workers
                if len(jobs) == max_workers:
                    break

            for job in as_completed(jobs):
                current_date_ = jobs[job]
                try:
                    df_len = job.result()
                    if verbose:
                        total_events += df_len
                        print(f"{current_date_}: {df_len} events")
                except Exception as exc:
                    logger.exception(f"Archive {current_date_}")

                del jobs[job]
                # break to add a new job, if there is any
                break

    if verbose:
        print(f"{counter} files are parsed and {total_events} events are saved into the database")
        print("now creating indexes")

    # create indexes on launch table
    columns_to_index = ["timestamp", "origin", "provider", "resolved_ref", "ref", "repo_url"]
    db[launch_table].create_index(columns_to_index)
    # optimize the database
    db.vacuum()

    if verbose:
        end_time = datetime.now()
        print(f"parsing finished at {end_time}")
        duration = f"duration: {end_time-start_time}"
        print(duration)
        logger.info(duration)


def get_args():
    parser = argparse.ArgumentParser(description=f'This script parses mybinder.org events archive '
                                                 f'(https://archive.analytics.mybinder.org/) and '
                                                 f'saves launch events into `{launch_table}` table '
                                                 f'in a sqlite3 database. '
                                                 f'Note that this table may not be ordered by launch timestamp.'
                                                 f'\nExample command to parse and save launch events '
                                                 f'from 05.05.2020 until 10.05.2020: '
                                                 f'\n\tpython parse_mybinder_archive.py -v -s 2020-05-05 -e 2020-05-10',
                                     formatter_class=argparse.RawTextHelpFormatter)
    parser.add_argument('-s', '--start_date', required=False, default="2018-11-03",
                        help='Start parsing from this day on. In form of "YYYY-MM-DD". '
                             'Default is 2018-11-03 which is the date of the first archive.')
    parser.add_argument('-e', '--end_date', required=False, default=str(datetime.today().date()),
                        help='Last date to parse. In form of "YYYY-MM-DD". Default is today.')
    parser.add_argument('-n', '--db_name', required=False, default="mybinder_archive",
                        help='Name of the output database, into where launch events are saved. '
                             'Default is mybinder_archive. '
                             'Timestamp is always appended into the name.')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Max number of processes to run in parallel. '
                                                                         'Default is 4.')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='Default is False.')
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    start_date = args.start_date
    end_date = args.end_date
    db_name = args.db_name
    db_name = f'{db_name}_at_{strftime("%Y_%m_%d_%H_%M_%S")}.db'.replace("-", "_")
    max_workers = args.max_workers
    verbose = args.verbose

    parse_mybinder_archive(start_date, end_date, db_name, max_workers, verbose)
    print(f"""\n
    Launch events from {start_date} until {end_date} are saved into `{launch_table}` table in {db_name}.
    You can open this database with `sqlite3 {db_name}` command and then run any sqlite3 command, 
    e.g., `select count(*) from {launch_table};` to get number of launches.
    """)


if __name__ == '__main__':
    main()
