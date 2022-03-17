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
import numpy as np
from datetime import timezone
import re

OUTPUT_PATH = "./results/"
random_time = [60, 179, 110, 80, 200, 250, 300, 400]
QUOTA_LIMIT = 100


class Miner:
    def __init__(self, user_token, debug=False, num_workers=1, batch_size=200, use_clone=True, commits_stats_from_clone=True, token_idx = 0):
        self.g = Github(user_token, per_page=100)
        self.debug_counts = 200 if debug else 0
        self.results = None
        self.num_workers = num_workers
        self.batch_size = batch_size
        self.use_clone=use_clone
        self.commits_stats_from_clone = commits_stats_from_clone
        self.token_idx = token_idx

    def get_rate_limit(self, func_name, quota_need, isPrint = False):
        remaining = self.g.rate_limiting
        if (isPrint):
            print(f"Token idx {self.token_idx}, {self.repo_name}, Running {func_name}, Rate limit: {remaining}")
        
        start = time.time()
        while self.g.rate_limiting[0] < quota_need:
            delay = random.choice(random_time)
            print(f"Token idx {self.token_idx}, {self.repo_name},  Delay {delay} sec")
            sleep(delay) 

        elapse = time.time() - start
        if elapse > 100:
            print(f"Wait {elapse/60} minutes!")

    def get_data(self, repo_name, debug=True):
        print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))
        self.repo_name = repo_name
        self.output_folder = self._create_output_folder()
        self.repo = self.g.get_repo(repo_name)
        
        actions = [self._fetch_commit_data, self._get_releases, self._add_commits_data_to_results, 
                   self._get_pull_requests, self._get_issues, self._get_stargazers, 
                   self._get_forks, self.save_results]
#        actions = [self._fetch_commit_data, self._get_releases, self._add_commits_data_to_results, 
#                   self._get_pull_requests, self._get_issues, self._get_stargazers, 
#                   self._get_forks, self._get_watchers, self.save_results]
#        actions = [self._get_releases, self._get_commits, 
#                   self._get_pull_requests, self.save_results]

        for act in actions:
            act()

    def save_results(self):
        self.results.to_csv(
            OUTPUT_PATH + f"{self.repo_name.replace('/','-')}_by_release.csv", index=False
#            OUTPUT_PATH + f"{self.repo_name.split('/')[-1]}_by_release.csv", index=False
        )
   
    def _get_results_by_threading(self, func, params):
        """
        Query github API by multithreading.
        return a list containing all results.
        """
        num_workers = self.num_workers
#        if func.__name__ not in  ["multi_pulls","multi_commits", "multi_watchers", "multi_releases"]:
#            num_workers = 1;
        if self.debug_counts:
            p = ThPool(num_workers)
            pool_args = params[: self.debug_counts]
            return p.map(func, pool_args)
        else:
            stats = []
            start = time.time()
            totalCount = params.totalCount
            for i in range(int(params.totalCount/self.batch_size)+1):
#                if self.num_workers != 1 and  i != 0 and (i+1)*self.batch_size % 800==0:
#                    print("Sleep 30 sec")
#                    sleep(30)
                p = ThPool(num_workers)
                self.get_rate_limit(str(func.__name__), 50, True)
                print(f'[start: {i*self.batch_size}] [end: {(i + 1) * self.batch_size}] [totalCount: {params.totalCount}]')
                endIndex = min((i+1) * self.batch_size, totalCount)
                temp = p.map(func, params[i*self.batch_size:endIndex])
                stats += temp
            print(f"{self.repo_name}, {func.__name__} takes: {round(time.time()-start,3)} secs" )
            print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))
#            print('Requests remaining = ' + str(self.g.rate_limiting[0]))
        return stats

    def _create_output_folder(self):
        result_path = OUTPUT_PATH + self.repo_name.replace("/", '-')
#        result_path = OUTPUT_PATH + self.repo_name.split("/")[-1]
        os.makedirs(result_path, exist_ok=True)
        return result_path

    def _read_existing_data(self, file_name):
        path = os.path.join(self.output_folder, file_name)
        if (os.path.isfile(path)):
            return pd.read_csv(path)
        else:
            return None

    def _fetch_commit_data(self):
        """
        Get commits activity grouped by release. 
        """
        #def retreieve_commits(commits_dates):
        def retrieve_commits():
            stats = []
            commits = self.repo.get_commits()
            print('Got commits paginated list')
            self.commit_hash_map = dict()
            i = 0
            for commit in commits:
                self.get_rate_limit('_fetch_commit_data', 10, (i % 100 == 0))
                one = {"commit_id": commit.sha}
                #one["committer_id"] = commit.author.login if commit.author else "None"
                one["committer_id"] = commit.commit.author.email if (commit.commit.author and commit.commit.author.email) else 'None'

                #one["committed_at"] = commits_dates[commit.sha][0]
                one["committed_at"] = commit.commit.author.date.astimezone(tz = timezone.utc).replace(tzinfo = None)
                one['committer_domain'] = extract_domain_from_email(commit.commit.author.email) \
                        if (commit.commit.author and commit.commit.author.email) else "None"
                self.commit_hash_map[one['commit_id']] = one['committed_at']
                stats.append(one)
                i = i + 1
            return stats

        def extract_domain_from_email(email_id):
            try:
                domain = re.match(r'.*@(.*)', email_id).group(1)
#                print('Email id = ' + str(email_id))
#                print('domain = ' + str(domain))
            except:
                domain = "None"
            return domain
       
        print(f'Entering fetch commits for {self.repo_name}')
        csv_file_name = f"{self.repo_name.split('/')[-1]}_commits_and_comments.csv"
        stats_pd = self._read_existing_data(csv_file_name)
        if stats_pd is not None:
            stats_pd.committed_at = stats_pd.committed_at.astype("datetime64[ns]")
            self.commit_stats = stats_pd
        else:
            if self.commits_stats_from_clone:
                stats = get_commits_stats_from_clone_repo(self.repo_name)
            else:
                #commits_dates = get_commits_from_clone_repo(self.repo_name)
                #stats = retreieve_commits(commits_dates) # get commits dates by clone repo
                stats = retrieve_commits()
            #pdb.set_trace()
            stats_pd = pd.DataFrame.from_records(stats, columns=["commit_id", "committer_id", "committed_at", "committer_domain"])
            stats_pd.committed_at = stats_pd.committed_at.astype("datetime64[ns]")

            path = os.path.join(self.output_folder, csv_file_name)
            stats_pd.to_csv(
                path,
                index=False,
                columns=["commit_id", "committer_id", "committed_at", "committer_domain"],
            )
            self.commit_stats = stats_pd
        print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))


    # @profile
    def _add_commits_data_to_results(self):  
    #def _get_commits(self):  
        """
        Get commits activity grouped by release. 
        """
        #def retreieve_commits(commits_dates):
#        def retrieve_commits():
#            stats = []
#            commits = self.repo.get_commits()
#            print('Got commits paginated list')
#            for commit in commits:
#                one = {"commit_id": commit.sha}
#                #one["committer_id"] = commit.author.login if commit.author else "None"
#                one["committer_id"] = commit.commit.author.email if (commit.commit.author and commit.commit.author.email) else 'None'
#
#                #one["committed_at"] = commits_dates[commit.sha][0]
#                one["committed_at"] = commit.commit.author.date.astimezone(tz = timezone.utc).replace(tzinfo = None)
#                one['committer_domain'] = extract_domain_from_email(commit.commit.author.email) \
#                        if (commit.commit.author and commit.commit.author.email) else "None"
#                stats.append(one)
#            return stats
#
#        def extract_domain_from_email(email_id):
#            try:
#                domain = re.match(r'.*@(.*)', email_id).group(1)
##                print('Email id = ' + str(email_id))
##                print('domain = ' + str(domain))
#            except:
#                domain = "None"
#            return domain
#       
#        print('Entering get commits')
#        if self.commits_stats_from_clone:
#            stats = get_commits_stats_from_clone_repo(self.repo_name)
#        else:
#            #commits_dates = get_commits_from_clone_repo(self.repo_name)
#            #stats = retreieve_commits(commits_dates) # get commits dates by clone repo
#            stats = retrieve_commits()
#        #pdb.set_trace()
#        stats_pd = pd.DataFrame.from_records(stats)
#        stats_pd.committed_at = stats_pd.committed_at.astype("datetime64[ns]")
        #stats_pd['committer_domain'] = stats_pd.apply(lambda row: get_committer_domain(row['committer_id']), axis = 1)
        #stats_pd['committer_domain'] = stats_pd.apply(lambda row: print('row = ' + str(row)), axis = 1)

        self.results['number_of_contributors'] = 0
        self.results['number_of_commits'] = 0
        self.results['number_of_new_contributors'] = 0
        self.results['number_of_contributor-domains'] = 0
        self.results['number_of_new_contributor-domains'] = 0

        current_contributors = set()
        current_contributor_domains = set()
        stats_pd = self.commit_stats
        for i in range(len(self.results)):
            if i == 0:
                mask = stats_pd.committed_at <= self.results.date[i]
            else:
                mask = (stats_pd.committed_at <= self.results.date[i]) & (
                        stats_pd.committed_at > self.results.date[i-1]
                )
            commit_pd = stats_pd[mask]
            self.results.at[i, 'number_of_contributors'] = commit_pd['committer_id'].nunique()
            self.results.at[i, 'number_of_contributor-domains'] = commit_pd['committer_domain'].nunique()
            self.results.at[i, 'number_of_commits'] = commit_pd.shape[0]
            new_contributors = commit_pd[~commit_pd['committer_id'].isin(current_contributors)]['committer_id'].drop_duplicates()
            new_contributor_domains = commit_pd[~commit_pd['committer_domain'].isin(current_contributor_domains)]['committer_domain'].drop_duplicates()
            #print(new_contributors)
            #print('new contributors count = ' + str(new_contributors.count()))
            self.results.at[i, 'number_of_new_contributors'] = new_contributors.count()
            self.results.at[i, 'number_of_new_contributor-domains'] = new_contributor_domains.count()
            current_contributors.update(new_contributors.tolist())
            current_contributor_domains.update(new_contributor_domains.tolist())

#        self.results = new_pd.copy()
#        csv_file_name = f"{self.repo_name.split('/')[-1]}_commits_and_comments.csv"
#        path = os.path.join(self.output_folder, csv_file_name)
#        stats_pd.to_csv(
#            path,
#            index=False,
#            columns=["commit_id", "committer_id", "committed_at", "committer_domain"],
#        )
        print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))
#        print('Requests remaining = ' + str(self.g.rate_limiting[0]))

    def _create_tag_map(self):
        tag_map = dict()
        for tag in self.tags:
            tag_map[tag.name] = tag.commit
        return tag_map

    def _get_releases(self):
        """
        Get releases for this repo
        """
        def multi_releases(release):
            one = {"release": str(release.id)}
            #one['title'] = release.title
            one['tag_name'] = release.tag_name
            commit = self.tag_map[release.tag_name]
            sha = commit.sha
            one['commit_id'] = sha
            #one['committed_at'] = commit.commit.author.date.astimezone(tz = timezone.utc).replace(tzinfo = None)
            #one['date'] = commit.commit.author.date.astimezone(tz = timezone.utc).replace(tzinfo = None) #This might be making an API call
            if (sha in self.commit_hash_map):
                one['date'] = self.commit_hash_map[sha]
            else:
                one['date'] = commit.commit.author.date.astimezone(tz = timezone.utc).replace(tzinfo = None)
            return one

        # Get tags data
        print(f'Entering get_release for {self.repo_name}')
        self.tags = self.repo.get_tags()
        self.tag_map = self._create_tag_map()

        all_releases = self.repo.get_releases()
        stats = self._get_results_by_threading(multi_releases, all_releases)
        stats_pd = pd.DataFrame.from_records(stats, columns=['release', 'tag_name', 'commit_id', 'date']).sort_values(by='date', ignore_index = True)
        stats_pd['days_since_last_release'] = 0

        for i in range(len(stats_pd)):
            if (i == 0):
                stats_pd.at[i, 'days_since_last_release'] = 0
            else:
                stats_pd.at[i, 'days_since_last_release'] = (stats_pd.at[i, 'date'] - stats_pd.at[i - 1, 'date']).days

        #stats_pd.date = stats_pd.date.astype("datetime64[ns]")
        print(stats_pd)
        self.results = stats_pd.copy()
        csv_file_name = f"{self.repo_name.replace('/','-')}_releases.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(
                path,
                index=False
        )
        print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))
#        print('Requests remaining = ' + str(self.g.rate_limiting[0]))

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
            #one["created_at"] = str(issue._created_at.value)
            one["created_at"] = issue.created_at.astimezone(tz = timezone.utc).replace(tzinfo = None)
            one["closed_at"] = (
                #str(issue._closed_at.value)
                issue.closed_at.astimezone(tz = timezone.utc).replace(tzinfo = None)
                if issue.closed_at
                else pd.to_datetime(1)
                #else str(pd.to_datetime(1))
            )  # set not closed issue date to 1970-01-01 for calcualte monthly closed issues.
            one["title"] = str(issue.title)
            return one

        all_issues = self.repo.get_issues(state=state)
        print('All issues count for ' + str(self.repo_name) + ' = ' + str(all_issues.totalCount))
        stats = self._get_results_by_threading(multi_issues, all_issues)

        stats_pd = pd.DataFrame.from_records(stats, columns=['id', 'state', 'comments', 'created_at', 'closed_at', 'title'])
#        stats_pd.created_at = stats_pd.created_at.astype("datetime64[ns]")
#        stats_pd.closed_at = stats_pd.closed_at.astype(
#            "datetime64[ns]", errors="ignore"
#        )

        self.results["number_of_open_issues"] = 0
        self.results["number_of_closed_issues"] = 0
        self.results["number_of_issue_comments"] = 0  # comments from open + closed issues

        for i in range(len(self.results)):
            if i == 0:
                open_mask = (stats_pd.created_at <= self.results.date[i])
                closed_mask = (stats_pd.closed_at <= self.results.date[i]) & (stats_pd.state == 'closed')
            else:
                open_mask = (
                        (stats_pd.created_at <= self.results.date[i]) 
                        & (stats_pd.created_at > self.results.date[i-1])
                )
                closed_mask = (
                            (stats_pd.closed_at <= self.results.date[i]) 
                            & (stats_pd.closed_at > self.results.date[i-1]) 
                            & (stats_pd.state == 'closed')
                )
#            if i != len(self.results) - 1:
#                open_mask = (
#                    (stats_pd.created_at >= self.results.dates[i])
#                    & (stats_pd.created_at < self.results.dates[i + 1])
#                    & (stats_pd.state == "open")
#                )
#                closed_mask = (
#                    (stats_pd.closed_at >= self.results.dates[i])
#                    & (stats_pd.closed_at < self.results.dates[i + 1])
#                    & (stats_pd.state == "closed")
#                )
#            else:
#                open_mask = (stats_pd.created_at >= self.results.dates[i]) & (
#                    stats_pd.state == "open"
#                )
#                closed_mask = (stats_pd.closed_at >= self.results.dates[i]) & (
#                    stats_pd.state == "closed"
#                )
            self.results.at[i, "number_of_open_issues"] = len(stats_pd[open_mask])
            self.results.at[i, "number_of_closed_issues"] = len(stats_pd[closed_mask])
            self.results.at[i, "number_of_issue_comments"] = sum(
                stats_pd[open_mask].comments
            ) + sum(
                stats_pd[closed_mask].comments
            )  # comments on both open + closed issues.
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_issues.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False,
            columns=["id",  "created_at", "closed_at", "state", "comments", "title"])

    # @profile
    def _get_stargazers(self):  # Total time: 0.811028 s for debug
        """
        Get release wise stargazers and update it in self.results, will finally save to .csv file
        """
        print(f'Entering collection of stars for {self.repo_name}')
        stargazer = self.repo.get_stargazers_with_dates()
        stats = []
        counts = self.debug_counts
        temp_counter = 0
        for star in stargazer:
            self.get_rate_limit('_get_stargazers', 10, (temp_counter % 100 == 0))
            if self.debug_counts:
                counts -= 1
                if counts == 0:
                    break
            one = {"user_id": star.user.login}
            one["starred_at"] = star.starred_at.astimezone(tz = timezone.utc).replace(tzinfo = None) if star.starred_at else None
            stats.append(one)
            temp_counter = temp_counter + 1
        
        stats_pd = pd.DataFrame.from_records(stats, columns=["starred_at", "user_id"])
        stats_pd.sort_values(by=["starred_at"])
        
        self.results["number_of_stargazers"] = 0
        for i in range(len(self.results)):
            if i == 0:
                mask = stats_pd.starred_at <= self.results.date[i]
            else:
                mask = (
                           (stats_pd.starred_at <= self.results.date[i])
                           & (stats_pd.starred_at > self.results.date[i-1])
                )
#            if i != len(self.results) - 1:
#                mask = (stats_pd.starred_at >= self.results.dates[i]) & (
#                    stats_pd.starred_at < self.results.dates[i + 1]
#                )
#            else:
#                mask = stats_pd.starred_at >= self.results.dates[i]
            self.results.at[i, "number_of_stargazers"] = len(stats_pd[mask])
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_stargazer.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False, columns=["starred_at", "user_id"])
        print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))
#        print('Requests remaining = ' + str(self.g.rate_limiting[0]))

    # @profile
    def _get_forks(self):  # Total time: 2.84025 s for debug
        """
        Get monthly forks and update it in self.results, will finally save to .csv file
        """
        forks = self.repo.get_forks()
        stats = []
        counts = self.debug_counts
        temp_counter = 0
        print(f'Number of forks for {self.repo_name} = {forks.totalCount}')
        for fork in forks:  # this line takes 90.1% time of this function
            self.get_rate_limit('_get_forks', 10, (temp_counter % 100 == 0))
            if self.debug_counts:
                counts -= 1
                if counts == 0:
                    break
            one = {"user_id": fork.owner.login if fork.owner else fork.full_name.split('/')[0]}
            one["created_at"] = fork.created_at.astimezone(tz = timezone.utc).replace(tzinfo = None) if fork.created_at else None
            stats.append(one)
            temp_counter = temp_counter + 1
        
        stats_pd = pd.DataFrame.from_records(stats, columns=["created_at", "user_id"])
        stats_pd.sort_values(by=["created_at"])

        self.results["number_of_forks"] = 0
        for i in range(len(self.results)):
            if i == 0:
                mask = stats_pd.created_at <= self.results.date[i]
            else:
                mask = (
                           (stats_pd.created_at <= self.results.date[i])
                           & (stats_pd.created_at > self.results.date[i-1])
                )
#            if i != len(self.results) - 1:
#                mask = (stats_pd.created_at >= self.results.dates[i]) & (
#                    stats_pd.created_at < self.results.dates[i + 1]
#                )
#            else:
#                mask = stats_pd.created_at >= self.results.dates[i]
            self.results.at[i, "number_of_forks"] = len(stats_pd[mask])
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_forks.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False, columns=["created_at", "user_id"])
        print('Requests remaining = ' + str(self.g.rate_limiting[0]) + ' for token idx: ' + str(self.token_idx))

    # @profile
    def _get_watchers(self):  # Total time: 4.25912 s for debug before multithread=
        """
        Get number of watchers. Each watcher requires a API call.
        # for debug 
        Before multithreading, Total time: 4.25912 s 
        After multithreading, Total time: 1.125 s
        """
        def multi_watchers(watcher):
            one = {"user_id": watcher.login}
            # created_at line takes 79.0% time of this function
            one["created_at"] = watcher.created_at.astimezone(tz = timezone.utc).replace(tzinfo = None) if watcher.created_at else None
            return one  

        watchers = self.repo.get_subscribers() # <---- this was wrong, not get_watchers!!
        stats = self._get_results_by_threading(multi_watchers, watchers)
        
        stats_pd = pd.DataFrame.from_records(stats, columns=["created_at", "user_id"])
        stats_pd.sort_values(by=["created_at"])

        self.results["number_of_watchers"] = 0
        for i in range(len(self.results)):
            if i == 0:
                mask = stats_pd.created_at <= self.results.date[i]
            else:
                mask = (
                        (stats_pd.created_at <= self.results.date[i])
                        & (stats_pd.created_at > self.results.date[i-1])
                )
#            if i != len(self.results) - 1:
#                mask = (stats_pd.created_at >= self.results.dates[i]) & (
#                    stats_pd.created_at < self.results.dates[i + 1]
#                )
#            else:
#                mask = stats_pd.created_at >= self.results.dates[i]
            self.results.at[i, "number_of_watchers"]= len(stats_pd[mask])
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_watchers.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False, columns=["created_at", "user_id"])

    # @profile
    def _get_pull_requests(self, state="all"):  # Total time: 192.765 s for debug
        """
        Get all the PR from this repo. Note that issues and PR share the same ID system.
        In the csv file, we have the following cols:
        
        PR_id, state(open/closed), comments, created_at, closed_at, merged, merged_at,

        """
        print(f'Entering get pull requests for {self.repo_name}')
        pulls = self.repo.get_pulls(state=state, sort="created")#, base="master")
        totalCount = pulls.totalCount
        print(f'Number of pull requests for {self.repo_name} = {totalCount}')
#        print('Pulls are here')

        def sequential_pull(pull_list):
            ret = []
            temp_counter = 0
            for pr in pull_list:
                self.get_rate_limit('_get_pull_requests', 10, (temp_counter % 100 == 0))
                one = {"id": str(pr.number)}
                one["state"] = pr.state
                ## FIXME pr.comments line takes 91.4% time of this function, this will call API once!
                #one["comments"] = (pr.comments)
                one["created_at"] = str(pr.created_at.astimezone(tz = timezone.utc).replace(tzinfo = None))
                # set not closed pr date to 1970-01-01 for calcualte monthly stats
                one["closed_at"] = (
                    str(pr.closed_at.astimezone(tz = timezone.utc).replace(tzinfo = None)) if pr.closed_at else str(pd.to_datetime(1))
                )
                # set not merged pr date to 1970-01-01 for calcualte monthly stats.
                one["merged_at"] = (
                    str(pr.merged_at.astimezone(tz = timezone.utc).replace(tzinfo = None)) if pr.merged_at else str(pd.to_datetime(1))
                )
                #one["merged_by"] = str(pr.merged_by.login) if pr.merged_by else None
                #one["merged"] = bool(pr._merged.value)
                one["merged"] = True if pr.merged_at else False
                ret.append(one)
                temp_counter = temp_counter + 1
            return ret

        def multi_pulls(pr):
            one = {"id": str(pr.number)}
            one["state"] = pr.state
            ## FIXME pr.comments line takes 91.4% time of this function, this will call API once!
            #one["comments"] = (pr.comments)
            one["created_at"] = str(pr.created_at.astimezone(tz = timezone.utc).replace(tzinfo = None))
            # set not closed pr date to 1970-01-01 for calcualte monthly stats
            one["closed_at"] = (
                str(pr.closed_at.astimezone(tz = timezone.utc).replace(tzinfo = None)) if pr.closed_at else str(pd.to_datetime(1))
            )

            # set not merged pr date to 1970-01-01 for calcualte monthly stats.
            one["merged_at"] = (
                str(pr.merged_at.astimezone(tz = timezone.utc).replace(tzinfo = None)) if pr.merged_at else str(pd.to_datetime(1))
            )
            #one["merged_by"] = str(pr.merged_by.login) if pr.merged_by else None

            #one["merged"] = bool(pr._merged.value)
            one["merged"] = True if pr.merged_at else False
            return one

        stats = self._get_results_by_threading(multi_pulls, pulls)
        stats_pd = pd.DataFrame.from_records(stats,
                       columns=[
                           "id",
                           "created_at",
                           "closed_at",
                           "merged_at",
                           "state",
            #               "comments",
                           "merged"
                       ]
                   )
        stats_pd.created_at = stats_pd.created_at.astype("datetime64[ns]")
        stats_pd.closed_at = stats_pd.closed_at.astype("datetime64[ns]", errors="ignore")
        stats_pd.merged_at = stats_pd.merged_at.astype("datetime64[ns]", errors="ignore")

        self.results["number_of_open_PRs"] = 0
        self.results["number_of_closed_PRs"] = 0
        self.results["number_of_merged_PRs"] = 0
        #self.results["PR_mergers"] = 0
        #self.results["number_of_PR_comments"] = 0  # comments from open + closed issues

#        for i in range(len(self.results)):
#            if i != len(self.results) - 1:
#                open_mask = (stats_pd.created_at >= self.results.dates[i]) & (
#                    stats_pd.created_at < self.results.dates[i + 1]
#                )
#                closed_mask = (
#                    (stats_pd.closed_at >= self.results.dates[i])
#                    & (stats_pd.closed_at < self.results.dates[i + 1])
#                    & (stats_pd.state == "closed")
#                    & (stats_pd.merged == False)
#                )  # all merged PR's state = close, so have to get rid of merged.
#                merged_mask = (
#                    (stats_pd.closed_at >= self.results.dates[i])
#                    & (stats_pd.closed_at < self.results.dates[i + 1])
#                    & (stats_pd.merged)
#                )
#            else:
#                open_mask = stats_pd.created_at >= self.results.dates[i]
#                closed_mask = (
#                    (stats_pd.closed_at >= self.results.dates[i])
#                    & (stats_pd.state == "closed")
#                    & (stats_pd.merged == False)
#                )
#                merged_mask = (stats_pd.closed_at >= self.results.dates[i]) & (
#                    stats_pd.merged
#                )
#            self.results.at[i, "number_of_open_PRs"] = len(stats_pd[open_mask])
#            self.results.at[i, "number_of_closed_PRs"] = len(stats_pd[closed_mask])
#            self.results.at[i, "number_of_merged_PRs"] = len(stats_pd[merged_mask])
#            self.results.at[i, "PR_mergers"] = len(
#                stats_pd[merged_mask].merged_by.unique()
#            )
#            self.results.at[i, "number_of_PR_comments"] = (
#                sum(stats_pd[open_mask].comments)
#                + sum(stats_pd[closed_mask].comments)
#                + sum(stats_pd[merged_mask].comments)
#            )  # num of comments on open + closed + merged PRs.

        for i in range(len(self.results)):
            if (i == 0):
                open_mask = stats_pd.created_at <= self.results.date[i]
                closed_mask = (
                        (stats_pd.closed_at <= self.results.date[i])
                        & (stats_pd.state == "closed")
                        & (stats_pd.merged == False)
                )
                merged_mask = (stats_pd.closed_at <= self.results.date[i]) & (
                    stats_pd.merged
                )
            else:
                open_mask = (stats_pd.created_at <= self.results.date[i]) & (
                        stats_pd.created_at > self.results.date[i-1]
                )
                closed_mask = (
                        (stats_pd.closed_at <= self.results.date[i])
                        & (stats_pd.closed_at > self.results.date[i-1])
                        & (stats_pd.state == "closed")
                        & (stats_pd.merged == False)
                )
                merged_mask = (
                        (stats_pd.closed_at <= self.results.date[i])
                        & (stats_pd.closed_at > self.results.date[i-1])
                        & (stats_pd.merged)
                )

            self.results.at[i, "number_of_open_PRs"] = len(stats_pd[open_mask])
            self.results.at[i, "number_of_closed_PRs"] = len(stats_pd[closed_mask])
            self.results.at[i, "number_of_merged_PRs"] = len(stats_pd[merged_mask])
#            self.results.at[i, "PR_mergers"] = len(
#                stats_pd[merged_mask].merged_by.unique()
#            )
#            self.results.at[i, "number_of_PR_comments"] = (
#                sum(stats_pd[open_mask].comments)
#                + sum(stats_pd[closed_mask].comments)
#                + sum(stats_pd[merged_mask].comments)
#            )  # num of comments on open + closed + merged PRs.
        
        csv_file_name = f"{self.repo_name.split('/')[-1]}_pr.csv"
        path = os.path.join(self.output_folder, csv_file_name)
        stats_pd.to_csv(path, index=False,
            columns=[
                "id",
                "created_at",
                "closed_at",
                "merged_at",
                "state",
#                "comments",
                "merged"
            ]
        )

def run(token_idx):
    #repo_names = get_repo_names("./data/repo_linux.csv", token_idx, len(_token))
    path = 'tokens.txt'
    tokens = []
    with open(path, 'r') as f:
        for token in f:
            #print('token = ' + str(token))
            tokens.append(str(token).strip())
    #print(tokens)
#    repo_names = get_repo_names("./data/repo_list.csv", token_idx, len(_token))
    repo_names = get_repo_names("./data/repo_list.csv", token_idx, len(tokens))
    existing_results = get_existing_results(OUTPUT_PATH)
#    existing_results = get_existing_results("./results/")
   
    val = len(repo_names)
    print(f"total repos: {val}")
    print(f"token_idx: {token_idx}")
#    token = list(_token.values())[token_idx]
    token = tokens[token_idx]
    #pdb.set_trace()
    for repo_name in sorted(repo_names):
        print(f'Repo {repo_name} fetched using token {token_idx}')
        sub_name = repo_name.split("/")[-1]
        if  sub_name in existing_results or repo_name.replace('/','-') in existing_results:
            # print(f"{repo_name} exists, skipping...")
            continue 
        if check_in_problem_repo(repo_name):
            # print(f"{repo_name} has a problem, found in problem_repo.txt, skipping...")
            continue
        miner = Miner(token, debug=False, commits_stats_from_clone=False, num_workers = 3, token_idx = token_idx)
        if miner.g.rate_limiting[0] < QUOTA_LIMIT:
            # sleep(random.choice(random_time))
            print(f"{repo_name}: token is not ready...")
            break
            # miner = Miner(token, debug=False)

        print(f"{repo_name}: start...")
        try:
            error_message = miner.get_data(repo_name)
        except Exception:
            write_problem_repo(repo_name)
            #print(f"{miner.repo_name} has errors...")
            print(f"{repo_name} has errors...")
            traceback.print_exc()
            continue



if __name__ == "__main__":
    if len(sys.argv) != 2:
        assert("Pass token index!")
    token_idx = int(sys.argv[1])
    run(token_idx)

