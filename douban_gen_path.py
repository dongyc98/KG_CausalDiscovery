# select pos and neg pairs
# search for 2 and 3 path
import math
import os.path
from multiprocessing import Pool

import pandas as pd
from pandas import DataFrame
from py2neo import Graph


def read_pairs(path='/Users/dyc/Downloads/'):
    data_path = os.path.join(path, 'douban_sample/comments.csv')
    df = pd.read_csv(data_path)
    df_ = df[df['relation'] == 'collect'].reset_index(drop=True)
    return df_[['dbuser_id', 'movie_id', 'vote']]


def extract_path(num, df: DataFrame):
    '''
    use cypher to get the paths between some pairs and save to the file.
    :param num:
    :param df:
    :return:
    '''
    graph = Graph("http://localhost:7474", auth=("neo4j", "pwd"))
    count = 0
    with open('./douban_sample/collect01/paths' + str(num) + ".txt", 'w+') as fw:
        for idx, row in df.iterrows():
            u = row['dbuser_id']
            i = row['movie_id']
            v = row['vote']
            # query = "match (from:User{eid:'%s'}), (to:Movie{eid:'%s'}) " \
            #         "CALL apoc.algo.allSimplePaths(from, to, '>', 3) YIELD path " \
            #         "with [r in relationships(path) | type(r)] AS steps " \
            #         "RETURN steps, count(steps)" % (
            #             u, i)
            query = "match (from:User{eid:'%s'}), (to:Movie{eid:'%s'})  " \
                    "CALL apoc.algo.allSimplePaths(from, to, '>', 3) YIELD path " \
                    "with [r in relationships(path) | (case type(r) when 'collect' then r.vote>='4.0' when 'invcollect' then r.vote>='4.0' else type(r) end)] AS steps " \
                    "RETURN steps, count(steps)" % (u, i)
            try:
                result = graph.run(query)
                for line in result:
                    if len(line[0]) == 1:
                        continue
                    elif len(line[0]) == 2:
                        edge = str(line[0][0]) + '_' + str(line[0][1])
                    else:
                        edge = str(line[0][0]) + '_' + str(line[0][1]) + '_' + str(line[0][2])
                    edge_count = line[1]
                    fw.write(u + '\t' + i + '\t' + str(v) + '\t' + edge + '\t' + str(edge_count) + '\n')
                count += 1
                if count % 1 == 0:
                    print(num, ' path found ', count)
            except Exception as e:
                print(e)
    return count


if __name__ == "__main__":
    df = read_pairs(path='./')
    pool = Pool(processes=20)
    result_jobs = []
    # split into 20 part
    n = int(math.ceil(len(df) / float(20)))
    for i in range(0, len(df), n):
        uilist_part = df[i:i+n]
        result = pool.apply_async(extract_path, args=(i, uilist_part,))
        result_jobs.append(result)
    pool.close()
    pool.join()
    for i, p in enumerate(result_jobs):
        print(f"i:{i} res:{p.get()}")