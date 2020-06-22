"""
Parser for mybinder.org events archive (https://archive.analytics.mybinder.org/)
"""
import hashlib
import string
import escapism
import argparse
import logging
import pandas as pd
from datetime import datetime, timedelta
from time import strftime
from urllib.parse import unquote
from sqlalchemy import create_engine
from concurrent.futures.process import ProcessPoolExecutor
# from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed


PROVIDER_PREFIXES = {
    # name: prefix
    'GitHub': 'gh',  # github.com: repo name or full url + branch/tag/commit
    'Gist': 'gist',  # gist.github.com: username/gistId or full url + commit sha
    'GitLab': 'gl',  # gitlab.com: repo name or full url + branch/tag/commit
    'Git': 'git',  # Bare bones git repo provider: full url + branch/tag/commit
    'Zenodo': 'zenodo',  # Zenodo DOI
    'Figshare': 'figshare',
    'Hydroshare': 'hydroshare',
    'Dataverse': 'dataverse',
}


def strip_suffix(text, suffix=".git"):
    if text.endswith(suffix):
        text = text[:-(len(suffix))]
    return text


def _safe_build_slug(build_slug, limit, hash_length=6):
    """
    This function is copied from
    https://github.com/jupyterhub/binderhub/blob/ec14d4a7f83c29dd4586dd884ed02e1b63ffcae1/binderhub/builder.py#L166
    """
    build_slug_hash = hashlib.sha256(build_slug.encode('utf-8')).hexdigest()
    safe_chars = set(string.ascii_letters + string.digits)

    def escape(s):
        return escapism.escape(s, safe=safe_chars, escape_char='-')

    build_slug = escape(build_slug)
    if hash_length > 0:
        return '{name}-{hash}'.format(
            name=build_slug[:limit - hash_length - 1],
            hash=build_slug_hash[:hash_length],
        ).lower()
    else:
        return build_slug[:limit].lower()


def generate_safe_image_name(repo_name, tag_name):
    """
    <repo_name>:<tag_name>
    """
    safe_repo_name = _safe_build_slug(repo_name, limit=128)
    safe_tag_name = _safe_build_slug(tag_name, limit=128)
    # safe_tag_name = _safe_build_slug(tag_name, limit=128, hash_length=0)
    return f"{safe_repo_name}:{safe_tag_name}"


def get_resolved_ref(timestamp, provider, spec):
    """resolved ref in that timestamp, we have to go that point in time and find commit sha, record_id...

    - GitHub, Gist, Git and GitLab:
      - https://stackoverflow.com/questions/6990484/how-to-checkout-in-git-by-date
      - branch name: we have to find out which commit it is pointing to (in that time)
      - tag name: this is tricky, a tag always points to the same commit but user can always redefine a branch
    - Zenodo:
      - same as branch names in in git repo providers:
        for example this DOI 10.5281/zenodo.3547881 represents all versions,
        and will always resolve to the latest one.
        this means it (to which version it points) changes in time,
        thats why we cant use it as resolved spec and cant use 3547881 as resolved ref
        - ex: https://zenodo.org/record/3550239#.Xrqx_3UzaV4
    - Figshare: spec without version is accepted as version 1 and this makes it easy to generate the record_id TODO but then why it computes the record_id in get_resolved_ref?
    - Hydroshare: Hydroshare does not provide a history. TODO but then why it computes the record_id in get_resolved_ref? does repo2docker needs it?
    - Dataverse: record_id is the resolved ref here, same process as in the following link is needed
      - https://github.com/jupyterhub/binderhub/blob/ec14d4a7f83c29dd4586dd884ed02e1b63ffcae1/binderhub/repoproviders.py#L302
    """
    # TODO
    return NotImplemented


def get_ref(provider, spec):
    """
    returns ref which is passed to repo2docker --ref
    """
    assert provider in PROVIDER_PREFIXES, f"unknown provider: {provider}"
    # NOTE: branch names can contain "/"
    if provider == 'GitHub':
        org, repo_name, unresolved_ref = spec.split('/', 2)
        # Git branch, tag, or commit SHA
        ref = unresolved_ref
    elif provider == 'Gist':
        # spec is usually in form of "ELC/8fdc0f490b3058872a7014f01416dfb6/master"
        parts = spec.split('/')
        # Git commit SHA or master
        if len(parts) > 2:
            ref = parts[2]
        else:
            # in the archive there are specs like "ELC/380e584b87227b15727ec886223d9d4a/master/master"
            # SergiyKolesnikov/f94d91b947051ab5d2ba1aa30e25f050 - this is also valid spec
            ref = "master"
    elif provider == 'GitLab':
        quoted_namespace, unresolved_ref = spec.split('/', 1)
        # Git branch, tag, or commit SHA
        ref = unquote(unresolved_ref)
    elif provider == 'Git':
        quoted_repo_url, unresolved_ref = spec.rsplit('/', 1)
        # Git branch, tag, or commit SHA
        ref = unquote(unresolved_ref)
    elif provider == 'Zenodo':
        # ex specs: 10.5281/zenodo.2546072, 10.5281/zenodo.3337784
        ref = spec.split("zenodo.")[-1]
        # in archive there are also specs like: 10.15139/S3/YCSYUN, 10.7910/DVN/VZSO5S, 10.22002/d1.1259 ...
        ref = spec.split("/")[-1].split(".")[-1]
        # TODO we have to retrieve the record_id as done here:
        # https://github.com/jupyterhub/binderhub/blob/ec14d4a7f83c29dd4586dd884ed02e1b63ffcae1/binderhub/repoproviders.py#L220
        # ex spec: 10.5281/zenodo.3242073 -> This DOI represents all versions, and will always resolve to the latest one
    elif provider == 'Figshare':
        # ex spec: 10.6084/m9.figshare.9782777.v1 or 10.6084/m9.figshare.9782777
        ref = spec.split(".figshare.")[-1]
        if ".v" not in ref:
            # spec without version is accepted as version 1 - check get_resolved_ref method of FigshareProvider
            # this makes it easy to generate the record_id:
            ref = ref + ".v1"  # record_id
    elif provider == 'Dataverse':
        # ex spec: 10.7910/DVN/FLUPBJ but in the archive there are also specs like doi:10.7910/DVN/UJYEAD,
        # 10.11588/data/W60KUN, 10.15139/S3/W6AR1Z
        ref = spec.split("/")[-1]
    elif provider == 'Hydroshare':
        # Hydroshare does not provide a history
        # ref is resource_id
        ref = spec.split("?")[0].strip("/").split("/")[-1].split(".")[-1]
    return ref


def parse_spec(provider, spec):
    """
    Generates namespace, org, repo_name, ref, image_name, repo_url data from provider and spec data.
    - ref: this is the ref which is passed to repo2docker, not the one in binder form
    - image_name: "<repo_name>:<tag_name>" will be passed to repo2docker, it is the name of the output image
    - repo_url: will be passed to repo2docker

    Notes:
    - binder spec contains unresolved ref which is used in events log - this is what we process here
    - in binder, resolved ref is computed from spec and passed to repo2docker for build

    :param provider:
    :param spec:
    :return:
    """
    assert provider in PROVIDER_PREFIXES, f"unknown provider: {provider}"
    # TODO ? replace get_ref with get_resolved_ref which returns resolved ref from timestamp
    ref = get_ref(provider, spec)

    if provider == 'GitHub':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22GitHub%22
        org, repo_name, _ = spec.split('/', 2)
        # there are specs like "1-Nameless-1/Lign167.git/master"
        repo_name = strip_suffix(repo_name)
        namespace = f'{org}/{repo_name}'
        repo_url = f'https://github.com/{namespace}'
        docker_repo_name = namespace
    elif provider == 'Gist':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22Gist%22
        user_name, gist_id, *_ = spec.split('/')
        org = user_name
        repo_name = gist_id
        namespace = f'{org}/{repo_name}'
        repo_url = f'https://gist.github.com/{namespace}'
        docker_repo_name = namespace
    elif provider == 'GitLab':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22GitLab%22
        quoted_namespace, _ = spec.split('/', 1)
        namespace = unquote(quoted_namespace)
        org, repo_name = namespace.split('/', 1)
        # there are specs like "ipyhc/ipyhc.gitlab.io/master"
        repo_name = strip_suffix(repo_name)
        namespace = f'{org}/{repo_name}'
        repo_url = f'https://gitlab.com/{namespace}'
        docker_repo_name = namespace
    elif provider == 'Git':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22Git%22
        quoted_repo_url, _ = spec.rsplit('/', 1)
        repo_url = unquote(quoted_repo_url)
        org = None
        # repo_name = None
        # namespace = None
        docker_repo_name = repo_url
    elif provider == 'Zenodo':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22Zenodo%22
        org = None
        # repo_name = ref
        # namespace = None
        repo_url = f"https://doi.org/{spec}"
        docker_repo_name = ref
    elif provider == 'Figshare':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22Figshare%22
        org = None
        # repo_name = ref
        # namespace = None
        record_id = ref
        repo_url = f"https://doi.org/{spec}"
        docker_repo_name = record_id
    elif provider == 'Dataverse':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22Dataverse%22
        org = None
        # repo_name = ref
        # namespace = None
        repo_url = f"https://doi.org/{spec}"
        docker_repo_name = ref
    elif provider == 'Hydroshare':
        # https://binderlytics.herokuapp.com/binder-launches?sql=select+distinct+spec+from+binder+where+provider+%3D+%22Hydroshare%22
        org = None
        # repo_name = ref
        # namespace = None
        resource_id = ref
        repo_url = f"https://www.hydroshare.org/resource/{resource_id}"
        docker_repo_name = resource_id

    provider_prefix = PROVIDER_PREFIXES[provider]
    image_prefix = "urr"  # for unresolved ref
    docker_repo_name = f"{image_prefix}-{provider_prefix}-{docker_repo_name}"
    # image name consists of repo and tag names: "repo_name:tag_name"
    # repo name will stay same, because it is based on provider and spec
    # but when/if changed to resolved ref, tag name also changes. this is good
    image_name = generate_safe_image_name(docker_repo_name, ref)

    # remove repo_name, namespace columns - they are not needed probably
    # org column is good to use while analysing organisations using mybinder
    # return org, repo_name, namespace, ref, image_name, repo_url
    return org, ref, image_name, repo_url


def parse_archive(archive_date, db_name, table_name):
    """parse archive of given date and save into db"""
    a_name = f"events-{str(archive_date)}.jsonl"
    archive_url = f"https://archive.analytics.mybinder.org/{a_name}"

    # first read events from archive
    df = pd.read_json(archive_url, lines=True)
    # drop columns that we dont need for analysis
    # df = df.drop(["schema", "version", "status"], axis=1)
    df = df.drop(["schema", "status"], axis=1)

    # handle exceptions in events archive
    # events before 12.06.2019 has no origin value
    if 'origin' not in df.columns:
        df["origin"] = "mybinder.org"
    # events-2019-06-12.jsonl has mixed rows: with and without origin value
    if a_name == "events-2019-06-12.jsonl":
        df['origin'].fillna('mybinder.org', inplace=True)
    # in some archives Gist launches have wrong provider (GitHub)
    elif a_name == "events-2018-11-25.jsonl":
        df.loc[df[
                   'spec'] == "https%3A%2F%2Fgist.github.com%2Fjakevdp/256c3ad937af9ec7d4c65a29e5b6d454", "provider"] = "Gist"
        df.loc[df[
                   'spec'] == "https%3A%2F%2Fgist.github.com%2Fjakevdp/256c3ad937af9ec7d4c65a29e5b6d454", "spec"] = "jakevdp/256c3ad937af9ec7d4c65a29e5b6d454"
    elif a_name == "events-2019-01-28.jsonl":
        df.loc[df['spec'] == "loicmarie/ade5ea460444ea0ff72d5c94daa14500", "provider"] = "Gist"
    elif a_name == "events-2019-02-22.jsonl":
        df.loc[df['spec'] == "minrk/6d61e5edfa4d2947b0ee8c1be8e79154", "provider"] = "Gist"
    elif a_name == "events-2019-03-05.jsonl":
        df.loc[df['spec'] == "vingkan/25c74b0e1ea87110a740a9c29a901200", "provider"] = "Gist"
    elif a_name == "events-2019-03-07.jsonl":
        df.loc[df['spec'] == "bitnik/2b5b3ad303859663b222fa5a6c2d3726", "provider"] = "Gist"

    # generate new columns that we need for analysis
    # df[["org", "repo_name", "namespace", "ref", "image_name", "repo_url"]] = df.apply(lambda row: parse_spec(row["spec"]), axis=1, result_type='expand')
    df[["org", "ref", "image_name", "repo_url"]] = df.apply(lambda row: parse_spec(row["provider"], row["spec"]),
                                                            axis=1,
                                                            result_type='expand')

    # save into database, without index
    # https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_sql.html
    engine = create_engine(f'sqlite:///{db_name}', echo=False)
    df.to_sql(table_name, con=engine, if_exists="append", index=False)

    return len(df)


def parse_mybinder_archive(start_date, end_date, max_workers, engine, db_name, create_repo, logger, verbose=False):
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

    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        jobs = {}
        while current_date <= end_date or jobs:
            while current_date <= end_date:
                if verbose:
                    print(f"parsing archive of {current_date}")
                table_name = "mybinderlaunch"
                job = executor.submit(parse_archive, current_date, db_name, table_name)
                jobs[job] = str(current_date)
                current_date += one_day
                if verbose:
                    counter += 1
                # limit # jobs with max_workers
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
        print(f"{counter} files are parsed and {total_events} events are saved into sqlite db.")
        print("now creating indexes.")

    # create indexes and repo table
    columns_to_index = ["timestamp", "spec", "repo_url", "ref", "origin", "provider"]
    with engine.connect() as connection:
        for column_name in columns_to_index:
            connection.execute(f"CREATE INDEX ix_mybinderlaunch_{column_name} ON {table_name} ({column_name})")
        if create_repo:
            if verbose:
                print("creating repo table.")
            repo_table_name = "repo"
            connection.execute(f'create table {repo_table_name} AS '
                               f'select spec, repo_url, image_name, '
                               f'min(timestamp) as min_ts, max(timestamp) as max_ts, '
                               f'GROUP_CONCAT(DISTINCT ref) as refs '
                               f'from {table_name} group by "repo_url";')
            # TODO columns_to_index = []

    if verbose:
        end_time = datetime.now()
        print(f"parsing finished at {end_time}.")
        print(f"duration: {end_time-start_time}.")


def get_args():
    """
    python scripts/parser.py -s 2020-05-12 -e 2020-05-14
    """
    parser = argparse.ArgumentParser(description='Parser for mybinder.org events archive.')
    parser.add_argument('-s', '--start_date', required=False, default="2018-11-03",
                        help='Start parsing from this day on. In form of "YYYY-MM-DD". '
                             'Default is 2018-11-03 which is the date of the first archive.')
    parser.add_argument('-e', '--end_date', required=False, default=str(datetime.today().date()),
                        help='Last date to parse. In form of "YYYY-MM-DD". Default is today.')
    parser.add_argument('-n', '--db_name', required=False, default="mybinder_archive",
                        help='Default is mybinder_archive. '
                             'Start, end date and timestamp is always appended into the name.')
    parser.add_argument('-m', '--max_workers', type=int, default=4, help='Default is 4')
    parser.add_argument('-c', '--create_repo', required=False, default=False, action='store_true',
                        help='Create repo table? default is False')
    parser.add_argument('-v', '--verbose', required=False, default=False, action='store_true',
                        help='default is False')
    args = parser.parse_args()
    return args


def get_logger(start_date, end_date):
    name = f"logger_{start_date}_{end_date}".replace("-", "_")
    logger = logging.getLogger(name)
    file_handler = logging.FileHandler('{}_at_{}.log'.format(name, strftime("%Y_%m_%d_%H_%M_%S")))
    file_handler.setLevel(logging.ERROR)
    # format_ = '%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s'
    format_ = '%(asctime)s %(levelname)-8s %(message)s'
    formatter = logging.Formatter(format_)
    file_handler.setFormatter(formatter)
    logger.handlers = [file_handler]
    return logger


def main():
    args = get_args()
    start_date = args.start_date
    end_date = args.end_date
    db_name = args.db_name
    max_workers = args.max_workers
    create_repo = args.create_repo
    verbose = args.verbose

    db_name = f'{db_name}_{start_date}_{end_date}_at_{strftime("%Y_%m_%d_%H_%M_%S")}.db'.replace("-", "_")
    engine = create_engine(f'sqlite:///{db_name}', echo=False)
    logger = get_logger(start_date, end_date)
    # print(start_date, end_date, db_name, max_workers)

    parse_mybinder_archive(start_date, end_date, max_workers, engine, db_name, create_repo, logger, verbose)


if __name__ == '__main__':
    main()
