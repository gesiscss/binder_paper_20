"""
This script is WIP!

This script is to run inside binder containers.
It requires python 3 kernel, which is always installed by repo2docker.
And it also requiers nbformat, nbconvert and jupyter_client packages which are installed by repo2docker by default.
FIXME: this is not true for dockerfile repos.
TODO: it is important that which versions of nbformat and nbconvert that repo2docker installs,
 for example now it installs nbcovert 5.6.* but version 6.0.0 is on the way and has breaking changes
"""

import os
import nbformat
from nbconvert.preprocessors.execute import executenb


def execute_notebook(file_path, data, output_dir):
    # nbformat.NO_CONVERT: This special value can be passed to the reading and writing functions,
    # to indicate that the notebook should be loaded/saved in the format it’s supplied.
    nb_node = nbformat.read(file_path, as_version=nbformat.NO_CONVERT)
    # https://github.com/jupyter/nbconvert/blob/5.6.1/nbconvert/preprocessors/execute.py#L716
    # https://github.com/jupyter/nbconvert/blob/5.6.1/nbconvert/preprocessors/execute.py#L84
    # defaults:
    # timeout = 30
    # allow_errors = False
    output_nb_node = executenb(nb_node)
    # executenb executes nb from top to bottom
    # nb_node cells has execution_count
    nb_dir = os.path.join(output_dir, data["dir"])
    if not os.path.isdir(nb_dir):
        os.mkdir(nb_dir)
    nbformat.write(output_nb_node, f"{nb_dir}/{data['file']}", version=nbformat.NO_CONVERT)


def execute_notebooks(notebooks):
    output_dir = "/run_images/nbs"
    if not os.path.isdir(output_dir):
        os.mkdir(output_dir)
    for file_path, data in notebooks.items():
        # TODO use asyncio for execute_notebook?
        execute_notebook(file_path, data, output_dir)


def get_notebooks():
    # cwd must be /home/jovyan for non-dockerfile repos
    cwd = os.getcwd()
    notebooks = {}
    for root, dirs, files in os.walk(cwd):
        for file in files:
            if file.endswith(".ipynb"):
                file_path = os.path.join(root, file)
                rel_file_path = file_path.split(cwd+"/", 1)[-1]
                rel_dir = rel_file_path.rsplit(file, 1)[0]
                nb = {file_path: {"file": file, "root": root, "dir": rel_dir, "file_path": rel_file_path}}
                # print nb dict, it will be written into log file in run_images.py
                print(nb)
                notebooks.update(nb)
    return notebooks


def main():
    # import time
    # time.sleep(2)
    notebooks = get_notebooks()
    contains_nbs = 1 if notebooks else 0
    print({"contains_nbs": contains_nbs, "count": len(notebooks)})
    # TODO
    # nbs_executed = execute_notebooks(notebooks)
    # print({"nbs_executed": nbs_executed})
    # nbs_same_output = compare_nbs(notebooks)
    # print({"nbs_same_output": nbs_same_output})


if __name__ == '__main__':
    main()
