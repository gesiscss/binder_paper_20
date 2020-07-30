#!/usr/bin/env python3
"""Commands to detect notebooks within a repo2docker image
"""
import argparse
import logging
import os
import tempfile

import tornado.log

log = logging.getLogger(__name__)


def detect_notebooks(output_dir):
    """
    Based on https://github.com/minrk/repo2docker-checker/blob/bd179da5786e08a12ef92295cf02b38a5c2b8ceb/repo2docker_checker/checker.py#L146
    """
    notebooks = []
    cwd = os.getcwd()
    for root, dirs, files in os.walk(cwd):
        if ".ipynb_checkpoints" in root.split(os.path.sep):
            # skip accidentally committed checkpoints
            continue
        for file in files:
            if file.endswith(".ipynb"):
                rel_file_path = os.path.relpath(os.path.join(root, file), cwd)
                log.info(rel_file_path)
                notebooks.append(rel_file_path)

    dest_path = os.path.join(output_dir, "notebooks.txt")
    log.info(f"Saving paths of {len(notebooks)} notebooks to {dest_path}")
    with open(dest_path, "w") as f:
        for nb_rel_path in notebooks:
            f.write(nb_rel_path+"\n")


def main():
    tornado.log.enable_pretty_logging()
    logging.getLogger().setLevel(logging.INFO)

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--output-dir",
        type=str,
        default=tempfile.gettempdir(),
        help="Directory to store test results",
    )
    opts = parser.parse_args()
    detect_notebooks(opts.output_dir)


if __name__ == "__main__":
    main()
