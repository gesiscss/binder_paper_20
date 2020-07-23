"""
Script to extract repo data from launch events table.
"""
import asyncio
import argparse
import pandas as pd
from datetime import datetime
from time import sleep
from tornado.httpclient import HTTPClientError
from sqlite_utils import Database
from time import strftime
from utils import get_repo_data_from_github_api, get_resolved_ref_now, get_image_name, get_logger, GithubException, \
    get_repo_data_from_git, LAUNCH_TABLE as launch_table, REPO_TABLE as repo_table, \
    DEFAULT_IMAGE_PREFIX as default_image_prefix


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
    df_iter = pd.read_sql_query(f"""SELECT t.provider AS provider, t.repo_url AS repo_url, 
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
                                     ORDER BY first_launch_ts;""",
                                db.conn,
                                chunksize=chunk_size)
    return df_iter


async def create_repo_table(db_name, providers, launch_limit,
                            image_prefix, access_token=None, verbose=False):
    logger_name = f'create_repo_table_at_{strftime("%Y_%m_%d_%H_%M_%S")}'.replace("-", "_")
    logger = get_logger(logger_name)
    start_time = datetime.now()
    repo_count = 0
    if verbose:
        print(f"creating repo table, started at {start_time}")

    db = Database(db_name)
    # list of columns in the order that we will have in repo table
    columns = ['id', 'repo_url', 'provider', 'launch_count', 'first_launch_ts', 'last_launch_ts', 'last_spec']
    if access_token:
        columns.extend(['remote_id', 'fork', 'renamed', 'image_name',
                        'resolved_ref', 'resolved_date', 'resolved_ref_date',
                        'binder_dir', 'buildpack'])
    if repo_table in db.table_names():
        raise Exception(f"table {repo_table} already exists in {db_name}")
    else:
        # create repo table with id column as primary key
        repos = db[repo_table]
        # to_sql doesnt support setting pk column
        # that's why here we have to add a temp row and delete it again
        # set 1 for int columns and "" for text ones
        # remote_id is not int, because for gist repos it is commit hash
        int_columns = ["id", "fork", "renamed", "launch_count"]
        r = {c: 1 if c in int_columns else "" for c in columns}
        # here we set the pk column
        repos.insert(r, pk="id")
        repos.delete(1)

    repos_df_iter = get_repos_from_launch_table(db, providers, launch_limit)
    id_ = 1
    for df_chunk in repos_df_iter:
        df_chunk["id"] = 0
        if access_token:
            # additional data with default None, will be fetched from GitHub API and git history
            df_chunk["resolved_ref"] = None
            # date when resolved_ref is fetched
            df_chunk["resolved_date"] = None
            # commit date of resolved_ref
            df_chunk["resolved_ref_date"] = None
            df_chunk["image_name"] = None
            df_chunk["buildpack"] = None
            df_chunk["binder_dir"] = None
            # there will be repos with same remote_id, because they are renamed
            df_chunk["remote_id"] = None
            df_chunk["fork"] = None
            df_chunk["renamed"] = None
        # column for last launched spec
        df_chunk["last_spec"] = None
        rows = df_chunk.iterrows()
        index, row = next(rows)
        len_rows = len(df_chunk)
        retry = 3
        while True:
            # internal id
            df_chunk.at[index, "id"] = id_
            # use spec of the last launch for resolved_ref
            ts_specs = [ts_spec.split(";") for ts_spec in row["ts_specs"].split(",")]
            # sort by first element, which is timestamp
            ts_specs = sorted(ts_specs, key=lambda x: x[0])
            last_spec = ts_specs[-1][1]
            df_chunk.at[index, "last_spec"] = last_spec
            if access_token:
                try:
                    # first fetch resolved_ref
                    df_chunk.at[index, "resolved_date"] = datetime.utcnow().replace(second=0, microsecond=0).isoformat()
                    resolved_ref = await get_resolved_ref_now(row["provider"], last_spec, access_token)
                    df_chunk.at[index, "resolved_ref"] = resolved_ref
                    if resolved_ref and resolved_ref != "404":
                        df_chunk.at[index, "image_name"] = get_image_name(row["provider"], last_spec, image_prefix)
                        # get resolved_ref_date, binder_dir and buildpack
                        repo_data = get_repo_data_from_git(row["repo_url"], resolved_ref)
                        # if repo_data contains only 404s,
                        # it means that resolved_ref of last_spec is removed from git history
                        # TODO should we try again with previous spec?
                        df_chunk.at[index, "binder_dir"] = repo_data["binder_dir"]
                        df_chunk.at[index, "buildpack"] = repo_data["buildpack"]
                        df_chunk.at[index, "resolved_ref_date"] = repo_data["resolved_ref_date"]

                    # get repo data, fork and repo_id, from GitHub API
                    repo_data = await get_repo_data_from_github_api(row["provider"], row["repo_url"], access_token)
                    if repo_data:
                        df_chunk.at[index, "fork"] = repo_data.get("fork")
                        df_chunk.at[index, "remote_id"] = repo_data.get("remote_id")
                except ValueError as e:
                    minutes_until_reset = e.args[0].split(" minutes")[0].split()[-1].strip()
                    minutes_until_reset = int(minutes_until_reset)
                    msg = f'{row["repo_url"]}: Rate limit error, will try again after sleeping {minutes_until_reset} minutes'
                    logger.info(msg)
                    if verbose:
                        print(msg)
                    sleep(minutes_until_reset*60)
                    # continue to process last repo again
                    continue
                except GithubException as e:
                    if e.status == 403:
                        # github.GithubException.RateLimitExceededException: 403
                        # https://developer.github.com/v3/#rate-limiting
                        minutes_until_reset = e.data["minutes_until_reset"]
                        msg = f'{row["repo_url"]}: Rate limit error, will try again after sleeping {minutes_until_reset} minutes'
                        logger.info(msg)
                        if verbose:
                            print(msg)
                        sleep(minutes_until_reset*60)
                        # continue to process last repo again
                        continue
                    else:
                        logger.exception(f'{row["repo_url"]}')
                except Exception as e:
                    if retry > 1:
                        logger.info(f'Error while processing {row["repo_url"]}, attempt {4-retry}')
                        sleep(retry**retry)
                        retry -= 1
                        continue
                    else:
                        if isinstance(e, HTTPClientError):
                            # tornado.httpclient.HTTPClientError is raised in get_resolved_ref_now
                            df_chunk.at[index, "resolved_ref"] = str(e.code)
                        logger.exception(f'{row["repo_url"]}')
            # get next row
            id_ += 1
            len_rows -= 1
            retry = 3
            if len_rows == 0:
                # process next chunk
                break
            index, row = next(rows)

        # re-order columns, so more readable + also drop redundant columns
        df_chunk = df_chunk[columns]
        df_chunk.set_index('id', inplace=True)
        df_chunk.to_sql(repo_table, con=db.conn, if_exists="append", index=True)

        repo_count += len(df_chunk)
        # print(df_chunk.dtypes)

    if access_token:
        # detect renamed repos
        # get rows with same remote id
        df_renamed = pd.read_sql_query(f"""SELECT remote_id, 
                                                  COUNT(remote_id) AS duplicated, 
                                                  GROUP_CONCAT(DISTINCT id) AS ids 
                                           FROM {repo_table} 
                                           WHERE remote_id IS NOT null 
                                           GROUP BY provider, remote_id
                                           HAVING duplicated > 1;""",
                                       db.conn)
        # and for that rows set renamed column to 1
        for index, row in df_renamed.iterrows():
            logger.info(f'Remote id {row["remote_id"]} is renamed {row["duplicated"]} times: {row["ids"]}')
            for id_ in row["ids"].split(","):
                id_ = int(id_.strip())
                db[repo_table].update(id_, {"renamed": 1})

        # and set renamed as 0 for the rest of the repos which still exists
        # for non-existing repos it will stay as None (default)
        db.conn.execute(f"UPDATE {repo_table} SET renamed=0 WHERE fork IN (0, 1) AND (renamed!=1 OR renamed IS null);")
        db.conn.commit()

        # add repo_id fk into launch table
        if "repo_id" not in db[launch_table].columns_dict:
            db[launch_table].add_column("repo_id", fk=repo_table, fk_col="remote_id")
        db.conn.execute(f"""UPDATE {launch_table} 
                            SET repo_id=(SELECT remote_id 
                                         FROM {repo_table} 
                                         WHERE repo_url={launch_table}.repo_url);""")
        db.conn.commit()

    # optimize the database
    db.vacuum()
    end_time = datetime.now()
    msg = f"repo table is created with {repo_count} entries"
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
                             '`resolved_ref`, `image_name`, `fork`, `remote_id`, `renamed`, `buildpack`...\n'
                             'Without authentication GitHub API allows 60 requests per hour, '
                             'with authentication it is 5000 (https://developer.github.com/v3/#rate-limiting).\n'
                             'To create one: https://github.com/settings/tokens/new')
    # parser.add_argument('-f', '--providers', required=False, default="GitHub",
    #                     help='Comma-separated list of providers to filter. '
    #                          'Default is "GitHub,Gist".'
    #                          'To include all: "GitHub,Gist,Git,GitLab,Zenodo,Figshare,Hydroshare,Dataverse"')
    parser.add_argument('-p', '--image_prefix', required=False, default=default_image_prefix,
                        help=f'Prefix to be prepended to image name of each repo, default is "{default_image_prefix}".')
    parser.add_argument('-l', '--launch_limit', type=int, default=0,
                        help='Minimum number of launches that a repo must have to be saved. '
                             'Default is 0, which means save all repos.')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='Default is False.')
    args = parser.parse_args()
    return args


def main():
    args = get_args()
    db_name = args.db_name
    # providers = ['"'+p.strip()+'"' for p in args.providers.split(",")]
    providers = ['"'+p.strip()+'"' for p in "GitHub,Gist".split(",")]
    image_prefix = args.image_prefix
    launch_limit = args.launch_limit
    # fork = args.fork
    access_token = args.access_token
    if not access_token:
        access_token = None
        print("No token for GitHub API, no additional data will be fetched from GitHub API.")
        # if fork:
        #     print("No access token is provided, so we will process 60 repos per hour and"
        #           " process will take very long time")
    verbose = args.verbose

    asyncio.run(create_repo_table(db_name, providers, launch_limit, image_prefix, access_token, verbose))
    print(f"""\n
    Repo data is extracted from `{launch_table}` table and saved into `{repo_table}` table.
    You can open this database with `sqlite3 {db_name}` command and then run any sqlite3 command, 
    e.g., `select count(*) from {repo_table}` to get number of repos.
    """)


if __name__ == '__main__':
    main()
