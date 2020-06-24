import asyncio
import argparse
import pandas as pd
from datetime import datetime
from time import sleep
from sqlalchemy import create_engine
from time import strftime
from utils import is_fork, get_resolved_ref_now, get_image_name, get_logger, GithubException


async def create_repo_table(db_name, launch_table, repo_table, providers, launch_limit,
                            image_prefix, access_token=None, verbose=False):
    logger_name = f'create_repo_table_at_{strftime("%Y_%m_%d_%H_%M_%S")}'.replace("-", "_")
    logger = get_logger(logger_name)
    if verbose:
        start_time = datetime.now()
        print(f"creating repo table, started at {start_time}")
        repo_count = 0

    engine = create_engine(f'sqlite:///{db_name}', echo=False)
    if engine.dialect.has_table(engine, repo_table):
        raise Exception(f"table {repo_table} already exists in {db_name}")

    with engine.connect() as connection:
        # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.read_sql_query.html
        # https://developer.github.com/v3/#rate-limiting
        chunk_size = 5000 if access_token else 10000
        df_iter = pd.read_sql_query(f'SELECT provider, spec, repo_url, '
                                    f'COUNT(repo_url) AS launch_count, '
                                    f'MIN(timestamp) AS min_ts, '
                                    f'MAX(timestamp) AS max_ts, '
                                    f'GROUP_CONCAT(DISTINCT ref) AS refs, '
                                    f'GROUP_CONCAT(DISTINCT resolved_ref) AS resolved_refs '
                                    f'FROM {launch_table} '
                                    f'WHERE provider IN ({", ".join(providers)}) '
                                    f'GROUP BY repo_url '
                                    f'HAVING launch_count > {launch_limit} '
                                    f'ORDER BY min_ts;',
                                    connection,
                                    chunksize=chunk_size)
        id_ = 0
        for df_chunk in df_iter:
            df_chunk["id"] = 0
            if access_token:
                # fetch additional data from GitHub API
                df_chunk["image_name"] = ""
                df_chunk["resolved_ref_now"] = ""
                df_chunk["fork"] = None
            rows = df_chunk.iterrows()
            index, row = next(rows)
            len_rows = len(df_chunk)
            retry = 3
            while True:
                df_chunk.at[index, "id"] = id_
                if access_token:
                    try:
                        resolved_ref_now = await get_resolved_ref_now(row["provider"], row["spec"], access_token)
                        df_chunk.at[index, "resolved_ref_now"] = resolved_ref_now
                        image_name = get_image_name(row["provider"], row["spec"], image_prefix, resolved_ref_now)
                        df_chunk.at[index, "image_name"] = image_name
                        df_chunk.at[index, "fork"] = await is_fork(row["provider"], row["repo_url"], access_token)
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
                    except Exception as exc:
                        if retry > 1:
                            logger.info(f'Error while processing {row["repo_url"]}, attempt {4-retry}')
                            sleep(retry**retry)
                            retry -= 1
                            continue
                        else:
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
            columns = ['id', 'repo_url', 'provider', 'launch_count', 'min_ts', 'max_ts', 'refs', 'resolved_refs']
            if access_token:
                columns.extend(['fork', 'image_name', 'resolved_ref_now'])
            df_chunk = df_chunk[columns]
            df_chunk.set_index('id', inplace=True)
            df_chunk.to_sql(repo_table, con=connection, if_exists="append", index=True)
            if verbose:
                repo_count += len(df_chunk)
            # print(df_chunk.dtypes)

    if verbose:
        end_time = datetime.now()
        print(f"repo table is created with {repo_count} entries, finished at {end_time}")
        print(f"duration: {end_time - start_time}")


def get_args():
    parser = argparse.ArgumentParser(description='Script to extract repo data from mybinder launches table.')
    parser.add_argument('-n', '--db_name', required=True)
    parser.add_argument('-i', '--launch_table', required=False, default="mybinderlaunch",
                        help='Name of the mybinder launches table, based on that repo table is crated. '
                             'Default is "mybinderlaunch".')
    parser.add_argument('-o', '--repo_table', required=False, default="repo",
                        help='Name of the repo table where all repos are saved. '
                             'Default is "repo".')
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
    launch_table = args.launch_table
    repo_table = args.repo_table
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

    asyncio.run(create_repo_table(db_name, launch_table, repo_table, providers, launch_limit, image_prefix, access_token, verbose))


if __name__ == '__main__':
    main()
