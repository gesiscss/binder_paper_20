import logging
import time
import requests
import subprocess
import tempfile
import signal
from datetime import datetime
from yaml import safe_load
from github import Github, GithubException
from urllib.parse import unquote
from sqlite_utils import Database
from binderhub.repoproviders import strip_suffix, GitHubRepoProvider, GitRepoProvider, \
     GitLabRepoProvider, GistRepoProvider, ZenodoProvider, FigshareProvider, \
     HydroshareProvider, DataverseProvider
from binderhub.builder import _safe_build_slug
from repo2docker.buildpacks import CondaBuildPack, DockerBuildPack, JuliaProjectTomlBuildPack, JuliaRequireBuildPack, \
    LegacyBinderDockerBuildPack, NixBuildPack, PipfileBuildPack, PythonBuildPack, RBuildPack
from repo2docker.utils import chdir

LAUNCH_TABLE = "mybinderlaunch"
REPO_TABLE = "repo"
EXECUTION_TABLE = "execution"

DEFAULT_IMAGE_PREFIX = "bp20-"

BUILDPACKS = [
    LegacyBinderDockerBuildPack,
    DockerBuildPack,
    JuliaProjectTomlBuildPack,
    JuliaRequireBuildPack,
    NixBuildPack,
    RBuildPack,
    CondaBuildPack,
    PipfileBuildPack,
    PythonBuildPack
]


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


def get_utc_ts():
    ts = datetime.utcnow().replace(microsecond=0).isoformat()
    ts_safe = ts.replace(":", "-")
    return ts, ts_safe


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
        # there are "/master"s and it is invalid
        ref = unresolved_ref.strip("/")
    elif provider == 'Gist':
        parts = spec.split('/')
        if len(parts) > 2:
            # spec is usually in form of "ELC/8fdc0f490b3058872a7014f01416dfb6/master"
            # or "AhmadAlwareh/75cea0a7d0442a8c125561011a327a61/66a9fe58188ba819d3a655cc38a788be2dcdae49"
            # but in the archive there are specs like "ELC/380e584b87227b15727ec886223d9d4a/master/master"
            ref = parts[2].strip("/")
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


# async def get_resolved_ref_now(provider, spec, access_token=None):
#     if provider not in REPO_PROVIDERS:
#         raise Exception(f"unknown provider: {provider}")
#
#     if provider in ["GitHub", "Gist"]:
#         provider = REPO_PROVIDERS[provider](spec=spec)
#         provider.access_token = access_token
#         resolved_ref = await provider.get_resolved_ref()
#         if resolved_ref is None:
#             # resolved ref not found
#             return "404"
#         else:
#             return resolved_ref
#     else:
#         return None


def get_repo_url(provider, spec):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")
    repo_url = REPO_PROVIDERS[provider](spec=spec).get_repo_url()
    repo_url = strip_suffix(repo_url, ".git").lower()
    return repo_url


def get_image_name(provider, spec, image_prefix, ref):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")
    build_slug = REPO_PROVIDERS[provider](spec=spec).get_build_slug()
    safe_build_slug = _safe_build_slug(build_slug, limit=255 - len(image_prefix))
    image_name = f"{image_prefix}{safe_build_slug}:{ref}".replace('_', '-').lower()
    return image_name


def git_execute(command, cwd=None, env=None):
    result = subprocess.run(command, universal_newlines=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, cwd=cwd, env=env)
    if result.returncode:
        raise Exception(f"{command}: {result.stderr}")
    return result


def get_repo_data_from_git(ref, repo_url):
    """
    - get commit date of resolved ref from git history
    - use repo2docker to detect binder_dir and buildpack
    """
    repo_data = {
        "resolved_date": datetime.utcnow().replace(second=0, microsecond=0).isoformat(),
        "resolved_ref": None,
        "resolved_ref_date": None,
        "binder_dir": None,
        "buildpack": None,
    }
    with tempfile.TemporaryDirectory() as tmp_dir_path:
        command = ["git", "clone", repo_url, tmp_dir_path]
        git_execute(command, env={"GIT_TERMINAL_PROMPT": "0"})

        # check if resolved ref exists in repo
        # it is possible that a commit, which is launched, is removed from history
        # ex: https://github.com/vaughnkoch/test1 and
        # https://github.com/vaughnkoch/test1/commit/464062f227e35eea5d01e138b83bb01912587060
        command = ["git", "checkout", ref]
        try:
            git_execute(command, tmp_dir_path)
        except Exception as e:
            e_txt = e.args[0].strip()
            if f"error: pathspec '{ref}' did not match any file(s) known to git" in e_txt or \
                "fatal: reference is not a tree" in e_txt:
                repo_data["resolved_ref"] = "404"
                # repo_data["resolved_ref_date"] = "404"
                # repo_data["binder_dir"] = "404"
                # repo_data["buildpack"] = "404"
                return repo_data
            else:
                raise e
        else:
            command = ["git", "rev-parse", "HEAD"]
            result = git_execute(command, tmp_dir_path)
            resolved_ref = result.stdout.strip()
            repo_data["resolved_ref"] = resolved_ref

            # get commit date of resolved ref
            command = ["git", "show", "-s", "--format=%cI", resolved_ref]
            result = git_execute(command, tmp_dir_path)
            resolved_ref_date = result.stdout.strip()
            date_ = datetime.fromisoformat(resolved_ref_date)
            # have date in UTC and in isoformat
            # this also removes timezone info
            repo_data["resolved_ref_date"] = datetime.utcfromtimestamp(date_.timestamp()).isoformat()

            default_buildpack = PythonBuildPack
            with chdir(tmp_dir_path):
                for BP in BUILDPACKS:
                    bp = BP()
                    try:
                        if bp.detect():
                            picked_buildpack = bp
                            break
                    except RuntimeError as e:
                        if "The legacy buildpack has been removed." == e.args[0]:
                            picked_buildpack = LegacyBinderDockerBuildPack()
                            setattr(picked_buildpack, "binder_dir", "")
                            break
                        else:
                            raise e
                else:
                    picked_buildpack = default_buildpack()

                repo_data["binder_dir"] = picked_buildpack.binder_dir
                repo_data["buildpack"] = picked_buildpack.__class__.__name__
    return repo_data


def get_repo_data_from_github_api(provider, repo_url, access_token=None):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")

    if provider in ["GitHub", "Gist"]:
        repo_data = {"remote_id": None, "fork": None}
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
            else:
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


class BuildTimeoutException(Exception):
    pass


class Timeout:
    """
    Warning: dont use this class in multi-threading, because signal only works in main thread.
    """
    def __init__(self, seconds=1, error_message='Timeout'):
        self.seconds = seconds
        self.error_message = error_message

    def handle_timeout(self, signum, frame):
        # TODO raising builtin TimeoutError didnt work, but why?
        raise BuildTimeoutException(self.error_message)

    def __enter__(self):
        signal.signal(signal.SIGALRM, self.handle_timeout)
        signal.alarm(self.seconds)

    def __exit__(self, type, value, traceback):
        # reset timer of signal
        signal.alarm(0)
