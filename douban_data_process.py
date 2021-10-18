# user - reviews - movie user2movie_reviews imdb
# user - comment - movie user2movie_comment.csv douban
# user - do/wish/collect - movie user2movie.csv
# movie - actor/actress/writer/composer/editor/director/cinematographer/producer/self/production_designer/archive_footage/archive_sound - person person2movie.csv
# movie - production/distributor/other/specialEffect - company companies2movie.csv
# movie - keywords - keywords movie2keywords.csv
# movie - certificates/Sex_Nudity/Violence_Gore/Profanity/Alcohol_Drugs_Smoking/Frightening_IntenseScenes - guides movie2parentalGuide.csv
# user - fan - person user2person.csv
# genres title_type etc. movie.json
# type name ? companies.json
# birthYear deathYear primaryName primaryProfession ? person.json
import os
from multiprocessing import Pool

import pandas as pd
import random
import numpy as np
import json

from py2neo import Graph, Node


def user2movie_reviews(dataset='/Users/dyc/Downloads/ori_dataset/', topn=-30):
    '''
    ratings
    :param dataset:
    :param topn: <0 random sample; >0 topk
    :return:
    '''
    path = os.path.join(dataset, 'triplet/user2movie_reviews.csv')
    # user_id:START_ID	:TYPE	reviews_id	movie_id:END_ID
    df = pd.read_csv(path)
    group = df.groupby(['user_id:START_ID'])
    if topn < 0:
        samplen = random.sample(group.indices.keys(), -topn)
    else:
        samplen = group[':TYPE'].count().nlargest(topn).keys()
    samplen = list(samplen)
    build_df = pd.DataFrame()
    jdata = json.load(open(os.path.join(dataset, 'node/reviews.json')))
    for s in samplen:
        data = group.get_group(s)
        for idx, row in data.iterrows():
            rating = jdata[row['reviews_id']]['rating']
            tmp = {'user_id': row['user_id:START_ID'], 'relation': row[':TYPE'],
                   'reviews_id': row['reviews_id'], 'movie_id': row['movie_id:END_ID'],
                   'rating': rating}
            new = pd.DataFrame(tmp, index=[0])
            build_df = build_df.append(new, ignore_index=True)
    # user_id relation reviews_id movie_id rating
    return build_df


def user2movie_comment(dataset='/Users/dyc/Downloads/ori_dataset/', topn=-30):
    '''
    do wish collect vote
    :param dataset:
    :param topn: <0 random sample; >0 topk
    :return:
    '''

    for i in range(3):
        path = os.path.join(dataset, 'triplet/user2movie_comment'+str(i)+'.csv')
        # user_id:START_ID	:TYPE	movie_id:END_ID	comment_id
        if i == 0:
            data_df = pd.read_csv(path)
        else:
            data_df = data_df.append(pd.read_csv(path), ignore_index=True)
    path = os.path.join(dataset, 'node/comment.json')
    jdata = json.load(open(path))
    group = data_df.groupby(['user_id:START_ID'])
    if topn < 0:
        samplen = random.sample(group.indices.keys(), -topn)
    else:
        samplen = group[':TYPE'].count().nlargest(topn).keys()
    samplen = list(samplen)
    build_df = pd.DataFrame()
    for s in samplen:
        data = group.get_group(s)
        for idx, row in data.iterrows():
            comment = jdata[row['comment_id']]
            tmp = {'dbuser_id': row['user_id:START_ID'], 'relation': row[':TYPE'],
                   'comment_id': row['comment_id'], 'movie_id': row['movie_id:END_ID'],
                   'vote': comment['vote']}
            new = pd.DataFrame(tmp, index=[0])
            build_df = build_df.append(new, ignore_index=True)
    # dbuser_id relation comment_id movie_id vote
    return build_df


def person2movie(movies, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'triplet/person2movie.csv')
    df = pd.read_csv(path, header=None, names=['person_id', 'relation', 'movie_id'])
    build_df = pd.DataFrame()
    person_set = set()
    for idx, row in df.iterrows():
        if row['movie_id'] in movies:
            build_df = build_df.append(row, ignore_index=True)
            person_set.add(row['person_id'])
    # 'person_id', 'relation', 'movie_id'
    return build_df, person_set


def companies2movie(movies, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'triplet/companies2movie.csv')
    df = pd.read_csv(path, header=None, names=['company_id', 'relation', 'movie_id'])
    build_df = pd.DataFrame()
    company_set = set()
    for idx, row in df.iterrows():
        if row['movie_id'] in movies:
            build_df = build_df.append(row, ignore_index=True)
            company_set.add(row['company_id'])
    # 'company_id', 'relation', 'movie_id'
    return build_df, list(company_set)


def movie2keywords(movies, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'triplet/movie2keywords.csv')
    df = pd.read_csv(path, header=None, names=['movie_id', 'relation', 'keyword'])
    build_df = pd.DataFrame()
    keyword_set = set()
    for idx, row in df.iterrows():
        if row['movie_id'] in movies:
            build_df = build_df.append(row, ignore_index=True)
            keyword_set.add(row['keyword'])
    # 'movie_id', 'relation', 'keyword'
    return build_df, list(keyword_set)


def movie2parentalGuide(movies, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'triplet/movie2parentalGuide.csv')
    df = pd.read_csv(path, header=None, names=['movie_id', 'type', 'pa_id'])
    build_df = pd.DataFrame()
    pa_set = set()
    for idx, row in df.iterrows():
        if row['movie_id'] in movies:
            tmp = {'movie_id': row['movie_id'], 'relation': 'parent_guide',
                   'pa_type': row['type']}
            new = pd.DataFrame(tmp, index=[0])
            build_df = build_df.append(new, ignore_index=True)
            pa_set.add(row['type'])
    # 'movie_id', 'relation', 'pa_type'
    return build_df, list(pa_set)


def user2person(users, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'triplet/user2person.csv')
    df = pd.read_csv(path, header=None, names=['dbuser_id', 'relation', 'person_id'])
    build_df = pd.DataFrame()
    person_set = set()
    for idx, row in df.iterrows():
        if row['dbuser_id'] in users:
            build_df = build_df.append(row, ignore_index=True)
            person_set.add(row['person_id'])
    # 'dbuser_id', 'relation', 'person_id'
    return build_df, person_set


def movie_info(movies, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'node/movie.json')
    jdata = json.load(open(path))
    build_df = pd.DataFrame()
    build_df2 = pd.DataFrame()
    geners_set = set()
    title_set = set()
    for m in movies:
        info = jdata[m]
        genres = info['genres'].split(",")
        titleType = info['titleType']
        tmp2 = {'movie_id': m, 'titleType': titleType}
        new2 = pd.DataFrame(tmp2, index=[0])
        build_df2 = build_df2.append(new2, ignore_index=True)
        for g in genres:
            geners_set.add(g)
            title_set.add(titleType)
            tmp = {'movie_id': m, 'genres': g}
            new = pd.DataFrame(tmp, index=[0])
            build_df = build_df.append(new, ignore_index=True)
    # movie_id geners | movie_id titleType
    return build_df, list(geners_set), build_df2, list(title_set)


def person_info(persons, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'node/person.json')


def company_info(companies, dataset='/Users/dyc/Downloads/ori_dataset/'):
    path = os.path.join(dataset, 'node/companies.json')


def save_entity(l, name, build_data_path):
    with open(os.path.join(build_data_path, name + '.txt'), 'w') as f:
        for u in l:
            f.write(u + '\n')


def load_entity(name, build_data_path):
    entities = []
    with open(os.path.join(build_data_path, name + '.txt'), 'r') as f:
        for line in f:
            entities.append(line.strip())
    return entities

def build_entity(name, en_name, build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    entities = load_entity(name, build_data_path)
    count = 0
    for e in entities:
        node = Node(en_name, eid=e)
        graph.create(node)
        count = count + 1
        print(en_name, count)


def build_data(build_data_path = "./douban_sample/", dataset = './ori_dataset/'):
    print("Sample begining ...")
    # dbuser_id relation comment_id movie_id vote
    comments_ = user2movie_comment(dataset=dataset, topn=30)
    users = list(set(comments_['dbuser_id']))
    movies = list(set(comments_['movie_id']))
    print('users', len(users))
    print('movies', len(movies))
    # 'person_id', 'relation', 'movie_id'
    print('person2movie ...')
    person2movie_, person_set = person2movie(movies, dataset=dataset)
    print('person1', len(person_set))
    # 'company_id', 'relation', 'movie_id'
    print('companies2movie ...')
    companies2movie_, company_set = companies2movie(movies, dataset=dataset)
    print('companies', len(company_set))
    # 'movie_id', 'relation', 'keyword'
    print('movie2keywords ...')
    movie2keywords_, keyword_set = movie2keywords(movies, dataset=dataset)
    print('keywords', len(keyword_set))
    # 'movie_id', 'relation', 'pa_type'
    print('movie2pG ...')
    movie2parentalGuide_, pa_set = movie2parentalGuide(movies, dataset=dataset)
    print('pG', len(pa_set))
    # 'dbuser_id', 'relation', 'person_id'
    print('user2person ...')
    user2person_, person_set2 = user2person(users, dataset=dataset)
    print('person2', len(person_set2))
    # movie_id geners
    print('movieinfo ...')
    movie_info_, genres_set, movie_info_2, title_type_set = movie_info(movies, dataset=dataset)
    print('genres', len(genres_set))
    print('titleType', len(title_type_set))
    persons = list(person_set.union(person_set2))

    # save
    print('Saving ...')
    save_entity(users, 'users', build_data_path)
    save_entity(movies, 'movies', build_data_path)
    save_entity(persons, "persons", build_data_path)
    save_entity(company_set, 'companies', build_data_path)
    save_entity(keyword_set, 'keywords', build_data_path)
    save_entity(pa_set, 'parentalGuide', build_data_path)
    save_entity(genres_set, 'genres', build_data_path)
    save_entity(title_type_set, 'titleType', build_data_path)
    comments_.to_csv(os.path.join(build_data_path, 'comments.csv'), index=False)
    person2movie_.to_csv(os.path.join(build_data_path, 'person2movie.csv'), index=False)
    companies2movie_.to_csv(os.path.join(build_data_path, 'companies2movie.csv'), index=False)
    movie2keywords_.to_csv(os.path.join(build_data_path, 'movie2keywords.csv'), index=False)
    movie2parentalGuide_.to_csv(os.path.join(build_data_path, 'movie2parentalGuide.csv'), index=False)
    user2person_.to_csv(os.path.join(build_data_path, 'user2person.csv'), index=False)
    movie_info_.to_csv(os.path.join(build_data_path, 'movieinfo.csv'), index=False)
    movie_info_2.to_csv(os.path.join(build_data_path, 'movieinfo2.csv'), index=False)


def import_comment(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    comments = pd.read_csv(os.path.join(build_data_path, 'comments.csv'))
    count = 0
    for idx, row in comments.iterrows():
        dbuser_id = row['dbuser_id']
        relation = row['relation'].replace('_', "")
        movie_id = row['movie_id']
        vote = row['vote']
        query = "match (p:User),(q:Movie) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s{vote:'%s'}]->(q)," \
                "(q)-[rel2:inv%s{vote:'%s'}]->(p)" % \
                (dbuser_id, movie_id, relation, vote, relation, vote)
        try:
            graph.run(query)
            count += 1
            print('comment', count)
        except Exception as e:
            print(e)
    return count

def import_person2movie(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    p2m = pd.read_csv(os.path.join(build_data_path, 'person2movie.csv'))
    count = 0
    for idx, row in p2m.iterrows():
        person_id = row['person_id']
        relation = row['relation'].replace('_', "")
        movie_id = row['movie_id']
        query = "match (p:Person),(q:Movie) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (person_id, movie_id, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('person2movie', count)
        except Exception as e:
            print(e)
    return count

def import_companies2movie(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    c2m = pd.read_csv(os.path.join(build_data_path, 'companies2movie.csv'))
    count = 0
    for idx, row in c2m.iterrows():
        company_id = row['company_id']
        relation = row['relation'].replace('_', "")
        movie_id = row['movie_id']
        query = "match (p:Company:p),(q:Movie) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (company_id, movie_id, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('companies2movie', count)
        except Exception as e:
            print(e)
    return count


def import_movie2keywords(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    m2k = pd.read_csv(os.path.join(build_data_path, 'movie2keywords.csv'))
    count = 0
    for idx, row in m2k.iterrows():
        keyword = row['keyword']
        relation = row['relation'].replace('_', "")
        movie_id = row['movie_id']
        query = "match (p:Movie),(q:Keyword) " \
                "where p.eid=\"%s\" and q.eid=\"%s\" create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (movie_id, keyword, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('movie2keywords', count)
        except Exception as e:
            print(e)
    return count

def import_movie2parentalGuide(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    m2pG = pd.read_csv(os.path.join(build_data_path, 'movie2parentalGuide.csv'))
    count = 0
    for idx, row in m2pG.iterrows():
        pa = row['pa_type']
        relation = row['relation'].replace('_', "")
        movie_id = row['movie_id']
        query = "match (p:Movie),(q:ParentalGuide) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (movie_id, pa, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('parentalGuide', count)
        except Exception as e:
            print(e)
    return count

def import_user2person(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    u2p = pd.read_csv(os.path.join(build_data_path, 'user2person.csv'))
    count = 0
    for idx, row in u2p.iterrows():
        dbuser_id = row['dbuser_id']
        relation = row['relation'].replace('_', "")
        person_id = row['person_id']
        query = "match (p:User),(q:Person) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (dbuser_id, person_id, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('user2person', count)
        except Exception as e:
            print(e)
    return count


def import_movie_info(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    mi = pd.read_csv(os.path.join(build_data_path, 'movieinfo.csv'))
    count = 0
    for idx, row in mi.iterrows():
        genres = row['genres']
        relation = "genres"
        movie_id = row['movie_id']

        query = "match (p:Movie),(q:Genre) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (movie_id, genres, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('movieinfo', count)
        except Exception as e:
            print(e)

    return count


def import_movie_info2(build_data_path):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    mi2 = pd.read_csv(os.path.join(build_data_path, 'movieinfo2.csv'))
    count = 0
    for idx, row in mi2.iterrows():
        relation = "titleType"
        title_type = row['titleType']
        movie_id = row['movie_id']
        query = "match (p:Movie),(q:TitleType) " \
                "where p.eid='%s' and q.eid='%s' create " \
                "(p)-[rel:%s]->(q)," \
                "(q)-[rel2:inv%s]->(p)" % \
                (movie_id, title_type, relation, relation)
        try:
            graph.run(query)
            count += 1
            print('movieinfo2', count)
        except Exception as e:
            print(e)
    return count


def import_to_neo4j(build_data_path = "./douban_sample/"):
    nas = ['users', 'movies', 'persons', 'companies', 'parentalGuide', 'genres', 'titleType']
    ens = ['User', 'Movie', 'Person', 'Company', 'ParentalGuide', 'Genre', 'TitleType']
    for i in range(len(nas)):
        build_entity(nas[i], ens[i], build_data_path)


if __name__ == '__main__':
    build_data_path = "./douban_sample/"
    build_data(build_data_path="./douban_sample/", dataset='./ori_dataset/')

    import_to_neo4j(build_data_path=build_data_path)
    pool = Pool(processes=6)
    result_jobs = []
    result_jobs.append(pool.apply_async(import_comment, args=(build_data_path,)))
    result_jobs.append(pool.apply_async(import_movie_info, args=(build_data_path,)))
    result_jobs.append(pool.apply_async(import_user2person, args=(build_data_path,)))
    result_jobs.append(pool.apply_async(import_person2movie, args=(build_data_path,)))
    result_jobs.append(pool.apply_async(import_companies2movie, args=(build_data_path,)))
    result_jobs.append(pool.apply_async(import_movie_info2, args=(build_data_path,)))
    result_jobs.append(pool.apply_async(import_movie2parentalGuide, args=(build_data_path,)))
    pool.close()
    pool.join()
    for i, p in enumerate(result_jobs):
        print(f"i:{i} res:{p.get()}")