from multiprocessing import Pool
#from release_runner import run
from monthly_runner_v3 import run

path = 'tokens.txt'
number_of_tokens = 0

if __name__ == '__main__':

    with open(path, 'r') as f:
        for token in f:
            number_of_tokens = number_of_tokens + 1

    with Pool(number_of_tokens) as p:
        p.map(run, range(number_of_tokens))
