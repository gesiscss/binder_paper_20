import logging
import hashlib
import string
import escapism
import time
from github import Github, GithubException
from urllib.parse import unquote
from sqlite_utils import Database
from binderhub.repoproviders import strip_suffix, GitHubRepoProvider, GitRepoProvider, \
     GitLabRepoProvider, GistRepoProvider, ZenodoProvider, FigshareProvider, \
     HydroshareProvider, DataverseProvider

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
        try:
            provider = REPO_PROVIDERS[provider](spec=spec)
            provider.access_token = access_token
            resolved_ref_now = await provider.get_resolved_ref()
        except ValueError as e:
            # catch rate limit error and sleep -> github_api_request raises ValueError
            # raise ValueError("GitHub rate limit exceeded. Try again in %i minutes." % minutes_until_reset)
            # minutes_until_reset = e.args[0].split(" minutes")[0].split()[-1].strip()
            # setattr(e, "data", {"minutes_until_reset": minutes_until_reset})
            raise e
        if resolved_ref_now is None:
            return "404"
        else:
            return resolved_ref_now
    else:
        return None


def get_repo_url(provider, spec):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")
    repo_url = REPO_PROVIDERS[provider](spec=spec).get_repo_url()
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


def get_image_name(provider, spec, image_prefix, ref):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")
    build_slug = REPO_PROVIDERS[provider](spec=spec).get_build_slug()
    safe_build_slug = _safe_build_slug(build_slug, limit=255 - len(image_prefix))
    image_name = f"{image_prefix}{safe_build_slug}:{ref}".replace('_', '-').lower()
    return image_name


async def is_fork(provider, repo_url, access_token=None):
    if provider not in REPO_PROVIDERS:
        raise Exception(f"unknown provider: {provider}")

    repo_url = strip_suffix(repo_url, ".git")
    if provider in ["GitHub", "Gist"]:
        try:
            g = Github(access_token)
            if provider == "GitHub":
                # github repo url is in this form: "https://github.com/{self.user}/{self.repo}"
                full_name = repo_url.split("github.com/")[-1]
                repo = g.get_repo(f"{full_name}")
            else:
                repo = g.get_gist(f'{repo_url.split("/")[-1]}')
        except GithubException as e:
            if e.status == 404:
                # repo doesnt exists anymore
                return 404
            elif e.status == 403:
                reset_seconds = g.rate_limiting_resettime - time.time()
                # round expiry up to nearest 5 minutes (as it is done in bhub)
                minutes_until_reset = 5 * (1 + (reset_seconds // 60 // 5))
                e.data["minutes_until_reset"] = minutes_until_reset
                raise e
        if getattr(repo, "fork", None) or getattr(repo, "fork_of", None):
            # GitHub object has fork attribute, but Gist object has fork_of
            return 1
        return 0
    else:
        return None


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
