import asyncio
import argparse
import pandas as pd
from datetime import datetime
from time import sleep
from tornado.httpclient import HTTPClientError
from sqlite_utils import Database
from time import strftime
from utils import get_repo_data, get_resolved_ref_now, get_image_name, get_logger, GithubException, is_dockerfile_repo


async def create_repo_table(db_name, providers, launch_limit,
                            image_prefix, access_token=None, verbose=False):
    logger_name = f'create_repo_table_at_{strftime("%Y_%m_%d_%H_%M_%S")}'.replace("-", "_")
    logger = get_logger(logger_name)
    if verbose:
        start_time = datetime.now()
        print(f"creating repo table, started at {start_time}")
        repo_count = 0

    launch_table = "mybinderlaunch"
    repo_table = "repo"
    db = Database(db_name)
    # list of columns in the order that we will have in repo table
    columns = ['id', 'repo_url', 'provider', 'launch_count', 'first_launch', 'last_launch', 'specs', 'refs', 'resolved_refs']
    if access_token:
        columns.extend(['remote_id', 'fork', 'dockerfile', 'resolved_ref_now', 'image_name'])
    if repo_table in db.table_names():
        raise Exception(f"table {repo_table} already exists in {db_name}")
    else:
        # create repo table with id column as primary key
        repos = db[repo_table]
        # to_sql doesnt support setting pk column
        # that's why here we have to add a temp row and delete it again
        r = {c: 1 if c in ["id", "launch_count", "dockerfile"] else "" for c in columns}
        # here we set the pk column
        repos.insert(r, pk="id")
        repos.delete(1)

    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
    # https://developer.github.com/v3/#rate-limiting
    chunk_size = 10000
    df_iter = pd.read_sql_query(f"""SELECT provider, repo_url, 
                                           COUNT(repo_url) AS launch_count, 
                                           MIN(timestamp) AS first_launch, 
                                           MAX(timestamp) AS last_launch, 
                                           GROUP_CONCAT(DISTINCT spec) AS specs, 
                                           GROUP_CONCAT(DISTINCT ref) AS refs, 
                                           GROUP_CONCAT(DISTINCT resolved_ref) AS resolved_refs 
                                     FROM {launch_table} 
                                     WHERE provider IN ({", ".join(providers)}) 
                                     GROUP BY repo_url 
                                     HAVING launch_count > {launch_limit} 
                                     ORDER BY first_launch;""",
                                db.conn,
                                chunksize=chunk_size)
    id_ = 1
    for df_chunk in df_iter:
        df_chunk["id"] = 0
        if access_token:
            # fetch additional data from GitHub API
            df_chunk["image_name"] = None
            df_chunk["resolved_ref_now"] = None
            df_chunk["fork"] = None
            # there will be repos with same remote_id, because they are renamed
            df_chunk["remote_id"] = None
            df_chunk["dockerfile"] = None
        rows = df_chunk.iterrows()
        index, row = next(rows)
        len_rows = len(df_chunk)
        retry = 3
        while True:
            df_chunk.at[index, "id"] = id_
            if access_token:
                try:
                    # use spec of last launch
                    spec = row["specs"].split(",")[-1].strip()
                    resolved_ref_now = await get_resolved_ref_now(row["provider"], spec, access_token)
                    df_chunk.at[index, "resolved_ref_now"] = resolved_ref_now
                    if resolved_ref_now and resolved_ref_now != "404":
                        image_name = get_image_name(row["provider"], spec, image_prefix, resolved_ref_now)
                        df_chunk.at[index, "image_name"] = image_name
                        df_chunk.at[index, "dockerfile"] = is_dockerfile_repo(row["provider"], row["repo_url"], resolved_ref_now)
                    repo_data = await get_repo_data(row["provider"], row["repo_url"], access_token)
                    if repo_data:
                        df_chunk.at[index, "remote_id"] = repo_data.get("remote_id")
                        df_chunk.at[index, "fork"] = repo_data.get("fork")
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
                            df_chunk.at[index, "resolved_ref_now"] = str(e.code)
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

        if verbose:
            repo_count += len(df_chunk)
        # print(df_chunk.dtypes)

    # detect renamed repos
    # first create the "renamed" column with default 0
    db[repo_table].add_column("renamed", int, not_null_default=0)
    # now get rows with same remote id
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

    # optimize the database
    db.vacuum()
    if verbose:
        end_time = datetime.now()
        print(f"repo table is created with {repo_count} entries")
        print(f"finished at {end_time}")
        print(f"duration: {end_time - start_time}")


def get_args():
    parser = argparse.ArgumentParser(description='Script to extract repo data from mybinder launches table.')
    parser.add_argument('-n', '--db_name', required=True)
    # parser.add_argument('-k', '--fork', required=False, default=False, action='store_true',
    #                     help='Adds a column which indicates if a repo is a fork or not. Implemented only for GitHub. '
    #                          'Setting this flag will increase time of this script '
    #                          'because this requires requests to GitHub API which has a low rate limit '
    #                          '(see --access_token). Default is False.')
    parser.add_argument('-t', '--access_token', required=False,
                        help='Access token for GitHub API. '
                             # 'This is used to check if a repo is fork or not, so pass access token only if you set --fork flag.'
                             'Without authentication GitHub API allows 60 requests per hour, '
                             'with authentication it is 5000 (https://developer.github.com/v3/#rate-limiting).'
                             'To create one: https://github.com/settings/tokens/new')
    # parser.add_argument('-f', '--providers', required=False, default="GitHub",
    #                     help='Comma-separated list of providers to filter. '
    #                          'Default is "GitHub,Gist".'
    #                          'To include all: "GitHub,Gist,Git,GitLab,Zenodo,Figshare,Hydroshare,Dataverse"')
    parser.add_argument('-p', '--image_prefix', required=False, default="bp20-",
                        help='Prefix to be prepended to image name of each repo, default is "bp20-".')
    parser.add_argument('-l', '--launch_limit', type=int, default=0,
                        help='Minimum number of launches for a repo to be saved in the database. '
                             'Default is 0, which means save all.')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='default is False')
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


if __name__ == '__main__':
    main()
