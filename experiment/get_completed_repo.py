import os

def get_files(path, suffix):
    files = []
    # r=root, d=directories, f = files
    for r, d, f in os.walk(path):
        for file in f:
            if (file.endswith(suffix)):
                files.append(os.path.join(r, file))
    return files

path = 'monthly_results'
files = get_files(path, '_monthly.csv')
print(files)

repos = [x.split('/')[1][:-12] for x in files]
print(repos)

with open('completed_repos.txt', 'w') as f:
    for repo in repos:
        f.write(repo + '\n')

all_repos = []

with open('data/repo_list.csv', 'r') as f:
    for repo in f:
        all_repos.append(repo.strip())

remaining_repos = [x for x in all_repos if x[19:].split('/')[1] not in repos and x[19:].replace('/', '-') not in repos]

with open('remaining_repos.txt', 'w') as f:
    for repo in remaining_repos:
        f.write(repo + '\n')
