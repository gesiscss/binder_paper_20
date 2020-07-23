The database downloaded by `postBuild` is the output of first 2 scripts 
(at [commit 28ffed2](https://github.com/gesiscss/binder_paper_20/tree/28ffed27aaca7e262318592463c4d4e21231c5d1)):

1. `python parse_mybinder_archive.py -v -e 2020-06-30 -m 16` 
2. `python create_repo_table.py -v -n mybinder_archive_at_<timestamp>.db --access_token <token>`

building images and further analysis of repos are not executed yet.