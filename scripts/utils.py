import logging
import hashlib
import string
import escapism
import time
import requests
import subprocess
import tempfile
from datetime import datetime
from yaml import safe_load
from github import Github, GithubException
from urllib.parse import unquote
from sqlite_utils import Database
from binderhub.repoproviders import strip_suffix, GitHubRepoProvider, GitRepoProvider, \
     GitLabRepoProvider, GistRepoProvider, ZenodoProvider, FigshareProvider, \
     HydroshareProvider, DataverseProvider

LAUNCH_TABLE = "mybinderlaunch"
REPO_TABLE = "repo"
DEFAULT_IMAGE_PREFIX = "bp20-"


REPO_PROVIDERS = {
    'GitHub': GitHubRepoProvider,
    'Gist': GistRepoProvider,
    'Git': GitRepoProvider,
    'GitLab': GitLabRepoProvider,
    'Zenodo': ZenodoProvider,
    'Figshare': FigshareProvider,
    'Hydroshare': HydroshareProvider,
    'Dataverse': DataverseProvider,
}


def get_org(provider, spec):
    """
    returns organization or user name
    """
    if provider == 'GitHub':
        org, repo_name, _ = spec.split('/', 2)
    elif provider == 'Gist':
        # here org is actually the user name
        org, gist_id, *_ = spec.split('/')
    elif provider == 'GitLab':
        quoted_namespace, _ = spec.split('/', 1)
        namespace = unquote(quoted_namespace)
        org, repo_name = namespace.split('/', 1)
    elif provider == 'Git':
        # TODO
        org = None
    elif provider in ['Zenodo', 'Figshare', 'Dataverse', 'Hydroshare']:
        # TODO
        org = None
    else:
        raise Exception(f"unknown provider: {provider}")
    return org


def get_ref(provider, spec):
    """
    returns ref from spec. this ref might be resolved (e.g. master) or unresolved (e.g. commit hash),
    it depends on user input in binder form.
    this provider and spec data is expected from events archive.

    but in binder, resolved ref (of that ref/spec at that time) is passed to repo2docker --ref.
    and this resolved ref is calculated from spec see repo_providers.py in binderhub.
    """
    # NOTE: branch names can contain "/"
    if provider == 'GitHub':
        org, repo_name, unresolved_ref = spec.split('/', 2)
        ref = unresolved_ref
    elif provider == 'Gist':
        parts = spec.split('/')
        if len(parts) > 2:
            # spec is usually in form of "ELC/8fdc0f490b3058872a7014f01416dfb6/master"
            # or "AhmadAlwareh/75cea0a7d0442a8c125561011a327a61/66a9fe58188ba819d3a655cc38a788be2dcdae49"
            # but in the archive there are specs like "ELC/380e584b87227b15727ec886223d9d4a/master/master"
            ref = parts[2]
        elif len(parts) == 2:
            # "SergiyKolesnikov/f94d91b947051ab5d2ba1aa30e25f050" is also valid spec
            ref = "master"
        else:
            raise Exception(f"Unknown {provider} spec: {spec}")
    elif provider in ['GitLab', 'Git']:
        _, ref = spec.rsplit('/', 1)
        # _ is quoted_namespace for GitLab and quoted_repo_url for Git
    elif provider in ['Zenodo', 'Figshare', 'Dataverse', 'Hydroshare']:
        # Zenodo, Figshare, Dataverse and Hydroshare have no ref info in spec.
        ref = None
    else:
        raise Exception(f"unknown provider: {provider}")
    return ref


def get_resolved_ref(timestamp, provider, spec):
    """
    resolved ref in that timestamp, we have to go that point in time and find commit sha, record_id...
    """
    # TODO
    return NotImplemented


async def get_resolved_ref_now(provider, spec, access_token=None):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")

    if provider in ["GitHub", "Gist"]:
        provider = REPO_PROVIDERS[provider](spec=spec)
        provider.access_token = access_token
        resolved_ref_now = await provider.get_resolved_ref()
        if resolved_ref_now is None:
            # resolved ref not found
            return "404"
        else:
            return resolved_ref_now
    else:
        return None


def get_repo_url(provider, spec):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")
    repo_url = REPO_PROVIDERS[provider](spec=spec).get_repo_url()
    repo_url = strip_suffix(repo_url, ".git").lower()
    return repo_url


def _safe_build_slug(build_slug, limit, hash_length=6):
    """
    Copied from https://github.com/jupyterhub/binderhub/blob/58a0b72021d17264519438f6e06f452021617a35/binderhub/builder.py#L166

    This function catches a bug where build slug may not produce a valid image name
    (e.g. repo name ending with _, which results in image name ending with '-' which is invalid).
    This ensures that the image name is always safe, regardless of build slugs returned by providers
    (rather than requiring all providers to return image-safe build slugs below a certain length).
    Since this changes the image name generation scheme, all existing cached images will be invalidated.
    """
    build_slug_hash = hashlib.sha256(build_slug.encode('utf-8')).hexdigest()
    safe_chars = set(string.ascii_letters + string.digits)

    def escape(s):
        return escapism.escape(s, safe=safe_chars, escape_char='-')

    build_slug = escape(build_slug)
    return '{name}-{hash}'.format(
        name=build_slug[:limit - hash_length - 1],
        hash=build_slug_hash[:hash_length],
    ).lower()


def get_image_name(provider, spec, image_prefix):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")
    build_slug = REPO_PROVIDERS[provider](spec=spec).get_build_slug()
    safe_build_slug = _safe_build_slug(build_slug, limit=255 - len(image_prefix))
    image_name = f"{image_prefix}{safe_build_slug}".replace('_', '-').lower()
    return image_name


def is_dockerfile_repo(provider, repo_url, resolved_ref):
    """Detects if a repo uses Dockerfile as binder config.
    This function makes head requests to the possible locations of Dockerfiles and
    if the page exists (status code), this means the repo is dockerfile repo.
    Another possibility is to make requests to GitHub API,
    but because of rate limit `create_repo_table.py` script waits ~30 mins per hour,
    so we decided to do it this way and use time more efficiently.
    """
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")

    def _path_exists(url):
        retry = 1
        response = None
        while retry <= 3:
            try:
                # allow redirects for renamed repos
                response = requests.head(url, allow_redirects=True, timeout=retry)
            except requests.exceptions.Timeout:
                retry += 1
            else:
                # break, if no timeout
                break
        if response is not None:
            if response.status_code == 200:
                return True
            return False
        # None means that we couldnt fetch it because of timeout, probably github is down
        return None

    if provider in ["GitHub", "Gist"]:
        full_name = repo_url.split("github.com/")[-1]
        if provider == "GitHub":
            # If a Dockerfile is present, all other configuration files will be ignored.
            # (https://repo2docker.readthedocs.io/en/latest/config_files.html#dockerfile-advanced-environments)
            # repo2docker searches for these folders in order (binder/, .binder/, root).
            # Having both ``.binder/`` and ``binder/`` folders is not allowed.
            # And if one of these folders exists, configuration files in that folder are considered only.
            # for example this is not a dockerfile repo with files ./Dockerfile and .binder/requirements.txt
            dockerfile_paths = [["binder", "binder/Dockerfile"], [".binder", ".binder/Dockerfile"], ["", "Dockerfile"]]
            # url = "https://raw.githubusercontent.com/{full_name}/{resolved_ref}/{dockerfile_path}"
            url = "https://github.com/{full_name}/tree/{resolved_ref}/{dockerfile_path}"
            exist = False
            for dir_, file_path in dockerfile_paths:
                if dir_ != "":
                    url_ = url.format(full_name=full_name, resolved_ref=resolved_ref, dockerfile_path=dir_)
                    dir_exist = _path_exists(url_)
                else:
                    # root dir always exists
                    dir_exist = True
                if dir_exist:
                    url_ = url.format(full_name=full_name, resolved_ref=resolved_ref, dockerfile_path=file_path)
                    exist = _path_exists(url_)
                    break
        else:
            dockerfile_path = "Dockerfile"
            url = "https://gist.githubusercontent.com/{full_name}/raw/{resolved_ref}/{dockerfile_path}"
            url_ = url.format(full_name=full_name, resolved_ref=resolved_ref, dockerfile_path=dockerfile_path)
            exist = _path_exists(url_)
        if exist is None:
            # currently not available
            return None
        return 1 if exist else 0
    else:
        return None


async def get_repo_data_from_github_api(provider, repo_url, access_token=None):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")

    if provider in ["GitHub", "Gist"]:
        repo_data = {}
        try:
            g = Github(access_token)
            if provider == "GitHub":
                # github repo url is in this form: "https://github.com/{self.user}/{self.repo}"
                full_name = repo_url.split("github.com/")[-1]
                repo = g.get_repo(f"{full_name}")
            else:
                repo = g.get_gist(f'{repo_url.split("/")[-1]}')
            # we need remote_id to detect renamed repos/users
            repo_data["remote_id"] = repo.id
        except GithubException as e:
            if e.status == 404:
                # repo doesnt exists anymore
                repo_data["fork"] = 404
                return repo_data
            elif e.status == 403:
                reset_seconds = g.rate_limiting_resettime - time.time()
                # round expiry up to nearest 5 minutes (as it is done in bhub)
                minutes_until_reset = 5 * (1 + (reset_seconds // 60 // 5))
                e.data["minutes_until_reset"] = minutes_until_reset
                raise e
        if getattr(repo, "fork", None) or getattr(repo, "fork_of", None):
            # GitHub object has fork attribute, but Gist object has fork_of
            repo_data["fork"] = 1
        else:
            repo_data["fork"] = 0
        return repo_data
    else:
        return None


def get_repo2docker_image():
    """
    Get the r2d image used in mybinder.org
    """
    url = "https://raw.githubusercontent.com/jupyterhub/mybinder.org-deploy/master/mybinder/values.yaml"
    values_yaml = requests.get(url)
    helm_chart = safe_load(values_yaml.text)
    r2d_image = helm_chart['binderhub']['config']['BinderHub']['build_image']
    return r2d_image


def git_execute(command, cwd=None):
    result = subprocess.run(command, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd)
    if result.returncode:
        raise Exception(result.stderr)
    return result


def get_mybinder_repo2docker_history():
    with tempfile.TemporaryDirectory() as tmp_dir_path:
        # clone mybinder.org-deploy repo
        command = ["git", "clone", "https://github.com/jupyterhub/mybinder.org-deploy.git", tmp_dir_path]
        result = git_execute(command)

        # get change history of repo2docker
        # git log --date=iso8601-strict -L /repo2docker/,+1:mybinder/values.yaml
        command = ["git", "log", "--date=iso8601-strict", "-L", "/repo2docker/,+1:mybinder/values.yaml"]
        result = git_execute(command, tmp_dir_path)

        r2d_history = {}
        for line in result.stdout.splitlines():
            # print(line)
            if line.startswith("commit"):
                commit = line[6:].strip()
            elif line.startswith("Date:"):
                date_str = line[5:].strip()
                date_ = datetime.fromisoformat(date_str)
                # have date in UTC and in isoformat
                # this also removes timezone info
                date_str_utc = datetime.utcfromtimestamp(date_.timestamp()).isoformat()
                # print(date_str, date_, date_str_utc)
            elif line.startswith("- "):
                old_image = line.split(":", maxsplit=1)[-1].strip()
            elif line.startswith("+ "):
                new_image = line.split(":", maxsplit=1)[-1].strip()
                if old_image:
                    # to skip first commit which adds repo2docker
                    r2d_history[date_str_utc] = {"commit": commit, "old": old_image, "new": new_image}
                old_image = None
    return r2d_history


def get_logger(name):
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    file_handler = logging.FileHandler(f'{name}.log')
    file_handler.setLevel(logging.DEBUG)
    # format_ = '%(asctime)s %(processName)-10s %(name)s %(levelname)-8s %(message)s'
    format_ = '%(asctime)s %(levelname)-8s %(message)s'
    formatter = logging.Formatter(format_)
    file_handler.setFormatter(formatter)
    logger.handlers = [file_handler]
    return logger


def drop_column(db_name, table_name, columns):
    """columns is the list of columns that you want to keep in table"""
    db = Database(db_name)
    db.conn.execute(f"""BEGIN TRANSACTION;
    CREATE TEMPORARY TABLE {table_name}_backup({",".join(columns)});
    INSERT INTO {table_name}_backup SELECT {",".join(columns)} FROM {table_name};
    DROP TABLE {table_name};
    CREATE TABLE {table_name}({",".join(columns)});
    INSERT INTO {table_name} SELECT {",".join(columns)} FROM {table_name}_backup;
    DROP TABLE {table_name}_backup;
    COMMIT;""")
