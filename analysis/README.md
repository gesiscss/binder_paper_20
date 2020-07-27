The database downloaded by `postBuild` is the output of first 2 scripts 
(at [commit 4c85425](https://github.com/gesiscss/binder_paper_20/tree/4c85425c923c36d17dc6bea763a10ba116d9081a)):

1. `python parse_mybinder_archive.py -v -e 2020-06-30 -m <#worker>` 
2. `python create_repo_table.py -v -n mybinder_archive_at_<timestamp>.db --access_token <token> -m <#worker>`

building images and further analysis of repos are not executed yet.
