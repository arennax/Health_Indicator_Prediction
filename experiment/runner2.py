"""
To profile the file, install line_profiler, decotrate functions with @profile and run:

```
kernprof -v -b  -l runner.py
```
"""

import pdb
import sys
import os
import time 
import random
import traceback
from time import sleep
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
import pandas as pd
from pathos.threading import ThreadPool as ThPool
from pathos.multiprocessing import  ProcessPool as ProPool

from github import Github
from utils import get_repo_names
from utils import get_existing_results
from utils import get_commits_from_clone_repo
from utils import check_in_problem_repo
from utils import write_problem_repo
from utils import get_commits_stats_from_clone_repo

OUTPUT_PATH = "./results/"
random_time = [60, 179, 110, 80, 200, 250, 300, 400]
QUOTA_LIMIT = 4500
NUM_PER_PAGE = 200
MAX_RATE = 4500


def get_a_miner(idx=None):
    _token= {
        "A":"f2e075347cd12545ab6ebedbc473a270a199f12d",
        "B":"aee73d28f7ac28bd881fa4fbbff45cc5cd02d2a8",
        "C":"43cfe4bcf383d476ed266c14d9444f14b922a961",
        "D":"db4f930fae768bfa46a3136f031d6afcfcddca0e",
        "E":"290955a490001b8a86e342a2c04af0583cbe0d07",
        "F":"78ff4388ac5df1561c5a8a41d37fc3054809d5d4",
        "G":"8f82bbe21a57f80301e92b36dc1c6463113fdc47",
        "H":"3c91d12e625860ca41a10fa9b08165ebd23bed0f",
        "I":"a0bd6ab71e9b1d4f7a47a6e408f5d0a987c6e37b",
        "J":"0f650cd2a885ed5bb8fe78ddd5dce0f749b99cab",
        "zhe": "8b471082f6fa8e5f2238da37666beea3f948b881",
        "huy": "138525460717398b423d46cc85f21c8d200ac44a",
        "george": "246a80dd48724eaddfc140845851b18c43089c1f"}

    # for key, v in _token.items():
    #     try:
    #         g = Github(v, per_page=NUM_PER_PAGE )
    #         print(g.rate_limiting[0])
    #     except Exception as e:
    #         print(v)
    #         continue
        

    tokens = list(_token.values())
    # tokens = ["f2e075347cd12545ab6ebedbc473a270a199f12d",
    #             "aee73d28f7ac28bd881fa4fbbff45cc5cd02d2a8"
    #             "43cfe4bcf383d476ed266c14d9444f14b922a961",
    #         "db4f930fae768bfa46a3136f031d6afcfcddca0e",
    #         "290955a490001b8a86e342a2c04af0583cbe0d07",]
    g = None
    if idx and idx < len(tokens):
        user_token = tokens[idx]
        g = Github(user_token, per_page=NUM_PER_PAGE )

        if g.rate_limiting[0] > QUOTA_LIMIT:
            return g 
    else:
        while True:
            idx = random.choice(range(len(tokens)))
            user_token = tokens[idx]
            g = Github(user_token, per_page=NUM_PER_PAGE )
            if g.rate_limiting[0] > QUOTA_LIMIT:
                return g 


class Miner(object):
    def __init__(self, user_token,num_workers=1,  debug = False,  batch_size=100, use_clone=True, commits_stats_from_clone=True, target=1000):
        super().__init__()
        self.debug_counts = 200 if debug else 0
        self.g = get_a_miner(0)
        self.results = None
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.use_clone=use_clone
        self.target = target
        self.commits_stats_from_clone = commits_stats_from_clone

    def _create_output_folder(self):
        result_path = OUTPUT_PATH + self.repo_name.split("/")[-1]
        os.makedirs(result_path, exist_ok=True)
        return result_path
    
    def get_data(self, repo_name, debug=True):
        self.repo_name = repo_name
        self.output_folder = self._create_output_folder()
        self.repo = self.g.get_repo(repo_name)

        # actions = [self._get_commits, self._get_watchers, self.save_results]
        
        actions = [self._get_commits, self._get_pull_requests, 
                   self._get_issues, self._get_stargazers, 
                   self._get_forks, self._get_watchers, self.save_results]

        for act in actions:
            act()
    def save_results(self):
        self.results.to_csv(
            OUTPUT_PATH + f"{self.repo_name.split('/')[-1]}_monthly.csv", index=False
        )
    def _get_results_by_threading(self, func, params):
        """
        Query github API by multithreading.
        return a list containing all results.
        """
        num_workers = self.num_workers
        if func.__name__ not in  ["multi_pulls","multi_commits", "multi_watchers"]:
            num_workers = 1;
        stats = []
        start = time.time()
        for i in range(len(params)//NUM_PER_PAGE):
            # pdb.set_trace()
            if self.num_workers != 1 and (i==0 or (i+1)*NUM_PER_PAGE% 400==0):
                sec = random.choice(range(10,60))
                print("Sleep {} sec".format(sec))
                sleep(sec)
            p = ThPool(num_workers)
            temp = p.map(func, params[i*NUM_PER_PAGE:(i+1)*NUM_PER_PAGE])
            stats += temp

        print(f"{self.repo_name}, {func.__name__} takes: {round(time.time()-start,3)} secs" )
        return stats
        # @profile
    def _get_watchers(self):  # Total time: 4.25912 s for debug before multithread=
        """
        Get number of watchers. Each watcher requires a API call.
        # for debug 
        Before multithreading, Total time: 4.25912 s 
        After multithreading, Total time: 1.125 s
        """
        # def multi_watchers(watcher):
        #     one = {"user_id": watcher.login}
        #     # created_at line takes 79.0% time of this function
        #     one["created_at"] = watcher.created_at 
        #     return one  

        

        watchers = self.repo.get_subscribers() # <---- this was wrong, not get_watchers!!
        stats = []
        for watcher in watchers:
            one = {"user_id": watcher.login}
            # created_at line takes 79.0% time of this function
            one["created_at"] = watcher.created_at 
            stats.append(one)
        
        stats_pd = pd.DataFrame.from_records(stats)
        stats_pd.sort_values(by=["created_at"])

        self.results["monthly_watchers"] = 0
        for i in range(len(self.results)):
            if i != len(self.results) - 1:
                mask = (stats_pd.created_at >= self.results.dates[i]) & (
                    stats_pd.created_at < self.results.dates[i + 1]
                )
            else:
                mask = stats_pd.created_at >= self.results.dates[i]
            self.results.at[i, "monthly_watchers"]= len(stats_pd[mask])
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_watchers.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False, columns=["created_at", "user_id"])

        # @profile
    def _get_stargazers(self):  # Total time: 0.811028 s for debug
        """
        Get monthly stargazers and update it in self.results, will finally save to .csv file
        """
        stargazer = self.repo.get_stargazers_with_dates()
        stats = []
        counts = self.debug_counts
        for star in stargazer:
            if self.debug_counts:
                counts -= 1
                if counts == 0:
                    break
            one = {"user_id": star.user.login}
            one["starred_at"] = star.starred_at
            stats.append(one)
        
        stats_pd = pd.DataFrame.from_records(stats)
        stats_pd.sort_values(by=["starred_at"])
        
        self.results["monthly_stargazer"] = 0
        for i in range(len(self.results)):
            if i != len(self.results) - 1:
                mask = (stats_pd.starred_at >= self.results.dates[i]) & (
                    stats_pd.starred_at < self.results.dates[i + 1]
                )
            else:
                mask = stats_pd.starred_at >= self.results.dates[i]
            self.results.at[i, "monthly_stargazer"] = len(stats_pd[mask])
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_stargazer.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False, columns=["starred_at", "user_id"])

    # @profile
    def _get_forks(self):  # Total time: 2.84025 s for debug
        """
        Get monthly forks and update it in self.results, will finally save to .csv file
        """
        forks = self.repo.get_forks()
        stats = []
        counts = self.debug_counts
        for fork in forks:  # this line takes 90.1% time of this function
            if self.debug_counts:
                counts -= 1
                if counts == 0:
                    break
            one = {"user_id": fork.owner.login}
            one["created_at"] = fork.created_at
            stats.append(one)
        
        stats_pd = pd.DataFrame.from_records(stats)
        stats_pd.sort_values(by=["created_at"])

        self.results["monthly_forks"] = 0
        for i in range(len(self.results)):
            if i != len(self.results) - 1:
                mask = (stats_pd.created_at >= self.results.dates[i]) & (
                    stats_pd.created_at < self.results.dates[i + 1]
                )
            else:
                mask = stats_pd.created_at >= self.results.dates[i]
            self.results.at[i, "monthly_forks"] = len(stats_pd[mask])
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_forks.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False, columns=["created_at", "user_id"])

    # @profile
    def _get_issues(self, state="all"):  # Total time: 1.4058 s for debug
        """
        Get all the issues from this repo.
        In the csv file, we have the following cols:

        issue_id, state(open/closed), comments(int), created_at, closed_at

        """
        def multi_issues(issue):
            one = {"id": str(issue.number)}
            one["state"] = issue.state
            one["comments"] = issue.comments
            one["created_at"] = str(issue._created_at.value)
            one["closed_at"] = (
                str(issue._closed_at.value)
                if issue._closed_at.value
                else str(pd.to_datetime(1))
            )  # set not closed issue date to 1970-01-01 for calcualte monthly closed issues.
            one["title"] = str(issue.title)
            return one

        all_issues = self.repo.get_issues(state=state, direction="desc")
        stats = []
        total_pages = all_issues.totalCount // NUM_PER_PAGE + 1
        total_miners = [get_a_miner().get_repo(repo_name).get_issues(state=state, direction="desc") for i in range(all_issues.totalCount//MAX_RATE+1)]
        page_ptr = 0
        for miner in total_miners:
            tmp = page_ptr
            ISSUEs = []
            for  i in range(page_ptr, page_ptr+45):
                ISSUEs += miner.get_page(i)  
            stats += self._get_results_by_threading(multi_issues, ISSUEs)
            page_ptr += 45



        # stats = self._get_results_by_threading(multi_issues, all_issues)

        stats_pd = pd.DataFrame.from_records(stats)
        stats_pd.created_at = stats_pd.created_at.astype("datetime64[ns]")
        stats_pd.closed_at = stats_pd.closed_at.astype(
            "datetime64[ns]", errors="ignore"
        )

        self.results["monthly_open_issues"] = 0
        self.results["monthly_closed_issues"] = 0
        self.results["monthly_issue_comments"] = 0  # comments from open and closed issues

        for i in range(len(self.results)):
            if i != len(self.results) - 1:
                open_mask = (
                    (stats_pd.created_at >= self.results.dates[i])
                    & (stats_pd.created_at < self.results.dates[i + 1])
                    & (stats_pd.state == "open")
                )
                closed_mask = (
                    (stats_pd.closed_at >= self.results.dates[i])
                    & (stats_pd.closed_at < self.results.dates[i + 1])
                    & (stats_pd.state == "closed")
                )
            else:
                open_mask = (stats_pd.created_at >= self.results.dates[i]) & (
                    stats_pd.state == "open"
                )
                closed_mask = (stats_pd.closed_at >= self.results.dates[i]) & (
                    stats_pd.state == "closed"
                )
            self.results.at[i, "monthly_open_issues"] = len(stats_pd[open_mask])
            self.results.at[i, "monthly_closed_issues"] = len(stats_pd[closed_mask])
            self.results.at[i, "monthly_issue_comments"] = sum(
                stats_pd[open_mask].comments
            ) + sum(
                stats_pd[closed_mask].comments
            )  # comments on both open + closed issues.
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_issues.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False,
            columns=["id",  "created_at", "closed_at", "state", "comments", "title"])

    
    def _get_commits(self):
        stats_pd = pd.read_csv("./results/zephyr/zephyr_commits_and_comments.csv", index_col=False)
        stats_pd.committed_at = stats_pd.committed_at.astype("datetime64[ns]")
        start_date, end_date = (
            str(stats_pd.committed_at.min())[:7],
            str(stats_pd.committed_at.max())[:7],
        )  # i.e, 2019-09
       
        new_pd = pd.DataFrame(
            {"dates": pd.date_range(start=start_date, end=end_date, freq="MS")}
        )
        new_pd["monthly_commits"] = 0
        new_pd["monthly_commit_comments"] = 0
        new_pd["monthly_contributors"] = 0
        
        # fmt:off
        for i in range(len(new_pd)):
            if i != len(new_pd) - 1:
                mask = (stats_pd.committed_at >= new_pd.dates[i]) & (
                    stats_pd.committed_at < new_pd.dates[i + 1]
                )
            else:
                mask = stats_pd.committed_at >= new_pd.dates[i]
            new_pd.at[i, "monthly_commits"] = len(stats_pd[mask])
            new_pd.at[i, "monthly_contributors"] = len(stats_pd[mask].committer_id.unique())
            # if self.debug_counts:
            #     print(stats_pd[mask].committer_id.unique())
        # fmt:on

        self.results = new_pd.copy()
        csv_file_name = f"{self.repo_name.split('/')[-1]}_commits_and_comments.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        print("commits done")
    
    def _get_pull_requests(self, state="all"):  # Total time: 192.765 s for debug
        """
        Get all the PR from this repo. Note that issues and PR share the same ID system.
        In the csv file, we have the following cols:
        
        PR_id, state(open/closed), comments, created_at, closed_at, merged, merged_at,

        """
        def multi_pulls(pr):
            one = {"id": str(pr.number)}
            one["state"] = pr.state
            ## FIXME pr.comments line takes 91.4% time of this function, this will call API once!
            one["comments"] = (pr.comments)  
            one["created_at"] = str(pr.created_at)
            # set not closed pr date to 1970-01-01 for calcualte monthly stats
            one["closed_at"] = (
                str(pr.closed_at) if pr.closed_at else str(pd.to_datetime(1))
            )
            one["merged"] = bool(pr._merged.value)
            # set not merged pr date to 1970-01-01 for calcualte monthly stats.
            one["merged_at"] = (
                str(pr.merged_at) if pr.merged_at else str(pd.to_datetime(1))
            )
            one["merged_by"] = str(pr.merged_by.login) if pr.merged_by else None
            return one
        
        pulls = self.repo.get_pulls(state=state, sort="created", base="master", direction="desc")
        stats = []
        total_pages = pulls.totalCount // NUM_PER_PAGE + 1
        total_miners = [get_a_miner().get_repo(repo_name).get_pulls(state=state, sort="created", base="master", direction="desc") for i in range(pulls.totalCount//MAX_RATE+1)]
        page_ptr = 0
        for miner in total_miners:
            tmp = page_ptr
            PRs = []
            for  i in range(page_ptr, page_ptr+45):
                PRs += miner.get_page(i)  
            stats += self._get_results_by_threading(multi_pulls, PRs)
            page_ptr += 45


        # stats = self._get_results_by_threading(multi_pulls, pulls)
        stats_pd = pd.DataFrame.from_records(stats)
        stats_pd.created_at = stats_pd.created_at.astype("datetime64[ns]")
        stats_pd.closed_at = stats_pd.closed_at.astype("datetime64[ns]", errors="ignore")
        stats_pd.merged_at = stats_pd.merged_at.astype("datetime64[ns]", errors="ignore")

        self.results["monthly_open_PRs"] = 0
        self.results["monthly_closed_PRs"] = 0
        self.results["monthly_merged_PRs"] = 0
        self.results["monthly_PR_mergers"] = 0
        self.results["monthly_PR_comments"] = 0  # comments from open and closed issues

        for i in range(len(self.results)):
            if i != len(self.results) - 1:
                open_mask = (stats_pd.created_at >= self.results.dates[i]) & (
                    stats_pd.created_at < self.results.dates[i + 1]
                )
                closed_mask = (
                    (stats_pd.closed_at >= self.results.dates[i])
                    & (stats_pd.closed_at < self.results.dates[i + 1])
                    & (stats_pd.state == "closed")
                    & (stats_pd.merged == False)
                )  # all merged PR's state = close, so have to get rid of merged.
                merged_mask = (
                    (stats_pd.closed_at >= self.results.dates[i])
                    & (stats_pd.closed_at < self.results.dates[i + 1])
                    & (stats_pd.merged)
                )
            else:
                open_mask = stats_pd.created_at >= self.results.dates[i]
                closed_mask = (
                    (stats_pd.closed_at >= self.results.dates[i])
                    & (stats_pd.state == "closed")
                    & (stats_pd.merged == False)
                )
                merged_mask = (stats_pd.closed_at >= self.results.dates[i]) & (
                    stats_pd.merged
                )
            self.results.at[i, "monthly_open_PRs"] = len(stats_pd[open_mask])
            self.results.at[i, "monthly_closed_PRs"] = len(stats_pd[closed_mask])
            self.results.at[i, "monthly_merged_PRs"] = len(stats_pd[merged_mask])
            self.results.at[i, "monthly_PR_mergers"] = len(
                stats_pd[merged_mask].merged_by.unique()
            )
            self.results.at[i, "monthly_PR_comments"] = (
                sum(stats_pd[open_mask].comments)
                + sum(stats_pd[closed_mask].comments)
                + sum(stats_pd[merged_mask].comments)
            )  # num of comments on open + closed + merged PRs.
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_pr.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False,
            columns=[
                "id",
                "created_at",
                "closed_at",
                "merged_at",
                "state",
                "comments",
                "merged"
            ]
        )


_token= {
        "A":"f2e075347cd12545ab6ebedbc473a270a199f12d",
        "B":"aee73d28f7ac28bd881fa4fbbff45cc5cd02d2a8",
        "C":"43cfe4bcf383d476ed266c14d9444f14b922a961",
        "D":"db4f930fae768bfa46a3136f031d6afcfcddca0e",
        "E":"290955a490001b8a86e342a2c04af0583cbe0d07",
        "F":"78ff4388ac5df1561c5a8a41d37fc3054809d5d4",
        "G":"8f82bbe21a57f80301e92b36dc1c6463113fdc47",
        "H":"3c91d12e625860ca41a10fa9b08165ebd23bed0f",
        "I":"a0bd6ab71e9b1d4f7a47a6e408f5d0a987c6e37b",
        "J":"0f650cd2a885ed5bb8fe78ddd5dce0f749b99cab",
        "zhe": "8b471082f6fa8e5f2238da37666beea3f948b881",
        "kewen": "4dd5721c02aef780fc7e5a52e131111301822162",
        "huy": "138525460717398b423d46cc85f21c8d200ac44a",
        "george": "246a80dd48724eaddfc140845851b18c43089c1f"}

if __name__ == "__main__":
    if len(sys.argv) != 2:
        assert("Pass token index!")
    token_idx = int(sys.argv[1])
    repo_names = get_repo_names("./data/repo_linux.csv", token_idx, len(_token))
    existing_results = get_existing_results("./results/")
   
    val = len(repo_names)
    print(f"total repos: {val}")
    print(f"token_idx: {token_idx}")
    token = list(_token.values())[token_idx]
    for repo_name in sorted(repo_names):
        sub_name = repo_name.split("/")[-1]
        # if  sub_name in existing_results:
        #     # print(f"{repo_name} exists, skipping...")
        #     continue 
        if check_in_problem_repo(repo_name):
            # print(f"{repo_name} has a problem, found in problem_repo.txt, skipping...")
            continue
        miner = Miner(token)
        # if miner.g.rate_limiting[0] < QUOTA_LIMIT:
        #     # sleep(random.choice(random_time))
        #     print(f"{repo_name}: token is not ready...")
        #     break
        #     # miner = Miner(token, debug=False)

        print(f"{repo_name}: start...")
        try:
            error_message = miner.get_data(repo_name)
        except Exception:
            # write_problem_repo(repo_name)
            print(f"{miner.repo_name} has errors...")
            traceback.print_exc()
            continue
