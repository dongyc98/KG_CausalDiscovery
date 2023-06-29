import pandas as pd
from py2neo import Graph


def link_prediction(user, vote, movie, P1_count, E_train_p1, P2_count, E_train_p2_0, E_train_p2_1):
    graph = Graph("http://localhost:7474", auth=("neo4j", "pwd"))

    p1 = "fan_director"
    p2 = "collect_genres_invgenres"

    query_p1 = "match (n1:User{eid:'%s'}), (n2:Movie{eid:'%s'}), (n3:Person), " \
               "p=(n1)-[:fan]->(n3)-[:director]->(n2) " \
               "return count(p)" % (user, movie)
    query_p2_1 = "match (n1:User{eid:'%s'}), (n2:Movie{eid:'%s'}), (n3:Genre), " \
                 "p=(n1)-[r:collect]->(:Movie)-[:genres]->(n3)-[:invgenres]->(n2) " \
                 "where r.vote>='4.0' " \
                 "return count(p)" % (user, movie)
    query_p2_0 = "match (n1:User{eid:'%s'}), (n2:Movie{eid:'%s'}), (n3:Genre), " \
                 "p=(n1)-[r:collect]->(:Movie)-[:genres]->(n3)-[:invgenres]->(n2) " \
                 "where r.vote<'4.0' " \
                 "return count(p)" % (user, movie)
    p1 = 0
    p2_0 = 0
    p2_1 = 0
    try:
        res = graph.query(query_p1)
    except Exception as e:
        pass
    for line in res:
        p1 = line[0]
    try:
        res = graph.query(query_p2_1)
    except Exception as e:
        pass
    for line in res:
        p2_1 = line[0]
    try:
        res = graph.query(query_p2_0)
    except Exception as e:
        pass
    for line in res:
        p2_0 = line[0]
    if p2_1 + p2_0 == 0:
        return 'delete', False
    if p1 != 0:
        score = (P1_count / (P1_count + P2_count)) * E_train_p1 + \
                (P2_count / (P1_count + P2_count)) * E_train_p2_1 * (p2_1 / (p2_0 + p2_1)) + \
                (P2_count / (P1_count + P2_count)) * E_train_p2_0 * (p2_0 / (p2_0 + p2_1))
    else:
        score = E_train_p2_1 * (p2_1 / (p2_0 + p2_1)) + \
                E_train_p2_0 * (p2_0 / (p2_0 + p2_1))
    if score > 0.5:
        pred = 1
    else:
        pred = 0
    return 'pred', pred == vote


def pre_train():
    '''
    get the constant variable in training kg
    :return:
    '''
    graph = Graph("http://localhost:7474", auth=("neo4j", "pwd"))
    query_p1 = "match (n1:User), (n2:Movie), (n3:Person), " \
               "(n1)-[:fan]->(n3)-[:director]->(n2)," \
               "(n1)-[r:collect]->(n2) " \
               "with [n1.eid, n2.eid] as he, (case when r.vote>='4.0' then 1.0 else 0.0 end) as v " \
               "return he, count(v), avg(v)"
    try:
        result = graph.query(query_p1)
    except Exception as e:
        pass
    P1_count = 0
    sum = 0
    for line in result:
        P1_count = P1_count + 1
        sum = sum + line[2]
    E_train_p1 = sum / P1_count
    print(P1_count, E_train_p1)
    query_p2_1 = "match (n1:User), (n2:Movie), (n3:Genre), " \
                 "p=(n1)-[r1:collect]->(n2), " \
                 "p2=(n1)-[r2:collect]->(:Movie)-[:genres]->(n3)-[:invgenres]->(n2) " \
                 "where r2.vote>='4.0' " \
                 "with [n1.eid, n2.eid] as he, (case when r1.vote>='4.0' then 1.0 else 0.0 end) as v " \
                 "return he, count(v), avg(v)"
    try:
        result = graph.query(query_p2_1)
    except Exception as e:
        pass
    P2_1_count = 0
    sum = 0
    for line in result:
        P2_1_count = P2_1_count + 1
        sum = sum + line[2]
    E_train_p2_1 = sum / P2_1_count
    print(P2_1_count, E_train_p2_1)
    query_p2_0 = "match (n1:User), (n2:Movie), (n3:Genre), " \
                 "p=(n1)-[r1:collect]->(n2), " \
                 "p2=(n1)-[r2:collect]->(:Movie)-[:genres]->(n3)-[:invgenres]->(n2) " \
                 "where r2.vote<'4.0' " \
                 "with [n1.eid, n2.eid] as he, (case when r1.vote>='4.0' then 1.0 else 0.0 end) as v " \
                 "return he, count(v), avg(v)"
    result = graph.query(query_p2_0)
    P2_0_count = 0
    sum = 0
    for line in result:
        P2_0_count = P2_0_count + 1
        sum = sum + line[2]
    E_train_p2_0 = sum / P2_0_count
    print(P2_0_count, E_train_p2_0)
    P2_count = P2_0_count + P2_1_count
    return P1_count, E_train_p1, P2_count, E_train_p2_0, E_train_p2_1


if __name__ == '__main__':
    num = '0'
    # test_flag = True
    # P1_count, E_train_p1, P2_count, E_train_p2_0, E_train_p2_1 = pre_train()
    # if test_flag:
    #     df = pd.read_csv('douban_setting'+str(num)+'/test_all.csv', sep='\t')
    # else:
    #     df = pd.read_csv('douban_setting' + str(num) + '/train.txt', sep='\t', header=None, names=['dbuser_id', 'vote', 'movie_id'])
    #     df = df.sample(n=7000, random_state=82, ignore_index=True)
    # count = 0
    # correct = 0
    # for idx, row in df.iterrows():
    #     u = row['dbuser_id']
    #     m = row['movie_id']
    #     v = row['vote']
    #     if test_flag:
    #         if v >= 4:
    #             s = 1
    #         else:
    #             s = 0
    #     else:
    #         if v == 'rating_high':
    #             s = 1
    #         else:
    #             s = 0
    #     cmd, pred = link_prediction(u, s, m, P1_count, E_train_p1, P2_count, E_train_p2_0, E_train_p2_1)
    #     if test_flag and cmd == 'delete':
    #         df.drop(idx, inplace=True)
    #     else:
    #         count = count + 1
    #         if pred:
    #             correct = correct + 1
    # print('Acc:', correct/count, 'correct:', correct, 'count:', count)
    # if test_flag:
    #     df.to_csv('./douban_setting'+str(num)+'/test_all.txt', sep='\t', index=False)

    P1_count, E_train_p1, P2_count, E_train_p2_0, E_train_p2_1 = pre_train()
    for rate in ['11', '46', '91']:
        df = pd.read_csv('./douban_setting'+str(num)+'/test_'+rate+'.csv', sep='\t')
        count = 0
        correct = 0
        for idx, row in df.iterrows():
            u = row['dbuser_id']
            m = row['movie_id']
            v = row['vote']
            if v >= 4:
                s = 1
            else:
                s = 0
            cmd, pred = link_prediction(u, s, m, P1_count, E_train_p1, P2_count, E_train_p2_0, E_train_p2_1)
            count = count + 1
            if pred:
                correct = correct + 1
        print(rate, ' Acc:', 1.0*correct/count, 'correct:', correct, 'count:', count)

