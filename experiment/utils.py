import pandas as pd
import os
import json 
import pdb 
import glob 
import subprocess 
import shutil

def get_repo_names(src,token_idx, token_num):
    res = []
    out = None
    if ".csv" in src:
        out = get_repo_names_csv(src)
    elif ".json" in src:
        out =  get_repo_names_json(src)
    else:
        file_format = src.split(".")[-1]
        raise ValueError(f"{file_format} format doesn't support!")
    
    for i in range(token_idx, len(out), token_num):
        res.append(out[i])
    return res
#    if len(out) < token_num:
#        return out
#    else:
#        res.append(out[token_idx])
#        for i in range(token_idx, len(out), token_num):
#            if i + token_num < len(out):
#                res.append(out[i + token_num])
#        return res

def get_repo_names_json(src):
    ret = []
    with open(src) as f:
        data_file = json.load(f)
        for node in data_file["data"]["search"]["edges"]:
            url = node["node"]["url"]
            repo = url[url.index(".com")+5:]
            ret.append(repo)
    return ret

def get_repo_names_csv(src):
    df = pd.read_csv(src, header=None)
    urls = df.iloc[:,0].apply(lambda x: x[x.index(".com")+5:])
    to_list = urls.values.tolist()
    return to_list



def get_existing_results(src="./results/"):
    files = glob.glob(src+"*.csv")
    ret = []
    for file in files:
        ret.append(file.split("/")[-1].split("_")[0])
    return ret

def get_commits_from_clone_repo(src="jagregory/abrash-black-book"):
    os.makedirs("./tmp_folder", exist_ok=True)
    os.chdir("./tmp_folder")
    folder_name = src.split("/")[-1]

    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
    full_path = "https://github.com/" + src + ".git"
    subprocess.check_call(["git", "clone","--quiet", full_path])
    
    os.chdir(f"{folder_name}")
    x = subprocess.check_output(["git", "log", "--pretty=%H_%cd_%s",
                            "--date=local", "--date=iso-local",])
    commits_list = x.decode("utf-8").split("\n")
    
    res = {}
    for comm in commits_list[:-1]:
        pdb.set_trace()
        temp = comm.split("_")
        head = temp[0]
        res[head] = temp[1:]
    
    os.chdir(f"../..")
    shutil.rmtree(f"./tmp_folder/{folder_name}")
    return res

def get_commits_stats_from_clone_repo(src):
    os.makedirs("./tmp_folder", exist_ok=True)
    os.chdir("./tmp_folder")
    folder_name = src.split("/")[-1]

    if os.path.exists(folder_name):
        shutil.rmtree(folder_name)
    full_path = "https://github.com/" + src + ".git"
    subprocess.check_call(["git", "clone","--quiet", full_path])
    
    os.chdir(f"{folder_name}")
    x = subprocess.check_output(["git", "log", "--pretty=%H_%cd_%s",
                            "--date=local", "--date=iso-local",])
    commits_list = x.decode("utf-8").split("\n")
    

    stats = []
    count = 0
    for comm in commits_list[:-1]:
        temp = comm.split("_")
        commit_id = temp[0]
        one = {"commit_id": commit_id}
        one["committed_at"] = temp[1]
        try:
            commit_message = subprocess.check_output(["git", "show", str(commit_id)])
            commits = commit_message.decode("utf-8").split("\n")
            one["committer_id"] = commits[1][8:] #Author: Andy Ross <andrew.j.ross@intel.com>, remove Author
            stats.append(one)
        except:
            print(count)
            count += 1

        

    os.chdir(f"../..")
    shutil.rmtree(f"./tmp_folder/{folder_name}")
    return stats



def check_in_problem_repo(name):
    with open("./data/problem_repo.txt", "r") as f:
        lines = f.readlines()
        if name+"\n" in set(lines):
            return True
        else:
            return False

            
def write_problem_repo(name):
    with open("./data/problem_repo.txt", "a") as f:
        f.write(name +"\n")


if __name__ == "__main__":
    # get_commits_from_clone_repo()
    # get_repo_names("./data/repo_list3.csv")
    # get_existing_results()
    # write_problem_repo("test1ee")
    check_in_problem_repo("test1ee")
