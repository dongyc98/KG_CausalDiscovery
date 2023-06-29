from py2neo import Graph
import pandas as pd
from itertools import combinations
import logging
from r_from_python import scci_given_data


def run_query(q, return_list=True):
    graph = Graph("http://localhost:7474", auth=("neo4j", "pwd"))
    res = graph.query(q)
    tmp_set = set()
    tmp_list = []
    for line in res:
        tmp_set.add((line[0][0], line[0][-1]))
        if return_list:
            tmp_list.append(tuple(line[0]))
    return tmp_set, tmp_list


def get_query(effect, path, large_set, dataset):
    # cause balance
    sp = path.split("_")
    if dataset == 'family':
        rs = []
        idx = 0
        while idx < len(sp):
            if sp[idx] == 'inv':
                rs.append('inv_' + sp[idx + 1])
                idx = idx + 1
            else:
                rs.append(sp[idx])
            idx = idx + 1
        sp = rs
    if len(sp) == 2:
        if path in large_set:
            return 'match (n1)-[:%s]->(n2)-[:%s]->(n3) ' \
                   'with [n1.eid, n3.eid] as es ' \
                   'return es, count(es)' % (sp[0], sp[1])
        else:
            return 'match (n1)-[:%s]->(n2)-[:%s]->(n3) ' \
                   'with [n1.eid, n2.eid, n3.eid] as es ' \
                   'return es, count(es)' % (sp[0], sp[1])
    elif len(sp) == 3:
        if path in large_set:
            return 'match (n1)-[:%s]->(n2)-[:%s]->(n3)-[:%s]->(n4) ' \
                   'with [n1.eid, n4.eid] as es ' \
                   'return es, count(es)' % (sp[0], sp[1], sp[2])
        else:
            return 'match (n1)-[:%s]->(n2)-[:%s]->(n3)-[:%s]->(n4) ' \
                   'with [n1.eid, n2.eid, n3.eid, n4.eid] as es ' \
                   'return es, count(es)' % (sp[0], sp[1], sp[2])
    else:
        raise Exception("Error")


def get_query_high_low(path, large_set, high_low_relation):
    sp = path.split("_")
    # 只考虑一个collect的path
    idx = [i for i in range(len(sp)) if sp[i] in high_low_relation]
    if len(sp) == 2:
        h = 'match (n1:User)-[r0:%s]->(n2)-[r1:%s]->(n3) ' \
            'with [n1.eid, n2.eid, n3.eid] as es ' % (sp[0], sp[1])
        hl = 'match (n1:User)-[r0:%s]->(n2)-[r1:%s]->(n3) ' \
             'with [n1.eid, n3.eid] as es ' % (sp[0], sp[1])
        m1 = 'where r%d.vote>="4.0" ' % (idx[0])
        m0 = 'where r%d.vote<"4.0" ' % (idx[0])
        t = ' return es, count(es)'
        # for i in range(1, len(idx)):
        #     m1 = m1 + ' and r%d.vote>="4.0"' % (idx[i])
        #     m0 = m0 + ' and r%d.vote<"4.0"' % (idx[i])
        if path in large_set:
            return hl + m1 + t, hl + m0 + t
        else:
            return h + m1 + t, h + m0 + t
    elif len(sp) == 3:
        h = 'match (n1:User)-[r0:%s]->(n2)-[r1:%s]->(n3)-[r2:%s]->(n4) ' \
            'with [n1.eid, n2.eid, n3.eid, n4.eid] as es ' % (sp[0], sp[1], sp[2])
        hl = 'match (n1:User)-[r0:%s]->(n2)-[r1:%s]->(n3)-[r2:%s]->(n4) ' \
             'with [n1.eid, n4.eid] as es ' % (sp[0], sp[1], sp[2])
        m1 = 'where r%d.vote>="4.0" ' % (idx[0])
        m0 = 'where r%d.vote<"4.0" ' % (idx[0])
        t = 'return es, count(es)'
        if path in large_set:
            return hl + m1 + t, hl + m0 + t
        else:
            return h + m1 + t, h + m0 + t
    else:
        raise Exception("Error")


def to_df(s, all_paths, y_1_set, l):
    L = []
    if sum(l) == 0:
        for (n1, n2) in s:
            if (n1, n2) in y_1_set:
                L.append(tuple([1] + l))
            else:
                L.append(tuple([0] + l))
    else:
        for tup in all_paths:
            n1 = tup[0]
            n2 = tup[-1]
            if (n1, n2) in s:
                if (n1, n2) in y_1_set:
                    L.append(tuple([1] + l))
                else:
                    L.append(tuple([0] + l))
    col_names = ['y', 'x'] + ['z' + str(i + 1) for i in range(len(l) - 1)]
    df = pd.DataFrame.from_records(L, columns=col_names)
    return df


def douban_gen_data(effect, cause, z_list: list, high_low_types: list, memory: dict, large_set=[], k=50,
                    high_low_relation=['collect', 'invcollect'], dataset='douban'):
    '''

    :param dataset: douban wn18 family etc.
    :param high_low_relation: like collect etc.
    :param memory: store the query result
    :param large_set: the large path set
    :param effect:
    :param cause:
    :param z_list: conditional variable
    :param high_low_types: like collect_invgenres_genre
    :param k: row < k means independent
    :return:
    '''
    logger = logging.getLogger("result")
    if effect in high_low_relation:
        q_y_1 = 'match (n1)-[r:%s]->(n3) ' \
                'where r.vote>="4.0" ' \
                'with  [n1.eid, n3.eid] as es ' \
                'return es, count(es)' % effect
        q_y_0 = 'match (n1)-[r:%s]->(n3) ' \
                'where r.vote<"4.0" ' \
                'with  [n1.eid, n3.eid] as es ' \
                'return es, count(es)' % effect
    else:
        q_y_1 = 'match (n1)-[r:%s]->(n3) ' \
              'with  [n1.eid, n3.eid] as es ' \
              'return es, count(es)' % effect
        if dataset == 'douban':
            q_y_0 = 'MATCH (a:User), (b:Movie) ' \
                'WHERE NOT (a)-[:%s]->(b) ' \
                'with [a.eid, b.eid] as es ' \
                'RETURN es, count(es)' % effect
        else:
            q_y_0 = 'MATCH (a), (b) ' \
                    'WHERE NOT (a)-[:%s]->(b) and a<>b ' \
                    'with [a.eid, b.eid] as es ' \
                    'RETURN es, 1 limit 100000' % effect

    if effect + '_set_1' not in memory.keys():
        y_1_set, _ = run_query(q_y_1, return_list=False)
        y_0_set, _ = run_query(q_y_0, return_list=False)
        memory[effect+'_set_1'] = y_1_set
        memory[effect+'_set_0'] = y_0_set
    else:
        y_1_set = memory[effect+'_set_1']
        y_0_set = memory[effect+'_set_0']

    if cause not in high_low_types:
        q_x = get_query(effect, cause, large_set=large_set, dataset=dataset)
        if cause + '_set' not in memory.keys():
            x_set, x_list = run_query(q_x)  # x0
            memory[cause + '_set'] = x_set
            memory[cause + '_list'] = x_list
        else:
            x_set = memory[cause + '_set']
            x_list = memory[cause + '_list']
    else:
        q_x_1, q_x_0 = get_query_high_low(cause, large_set=large_set, high_low_relation=high_low_relation)
        if cause + '_set_1' not in memory.keys():
            x_set_1, x_list_1 = run_query(q_x_1)
            x_set_0, x_list_0 = run_query(q_x_0)
            memory[cause + '_set_1'] = x_set_1
            memory[cause + '_list_1'] = x_list_1
            memory[cause + '_set_0'] = x_set_0
            memory[cause + '_list_0'] = x_list_0
        else:
            x_set_1 = memory[cause + '_set_1']
            x_list_1 = memory[cause + '_list_1']
            x_set_0 = memory[cause + '_set_0']
            x_list_0 = memory[cause + '_list_0']
        x_list = x_list_0 + x_list_1

    if len(z_list) == 0:
        all_paths = x_list
        if cause not in high_low_types:
            # Y:1 X:1
            s1 = y_1_set.intersection(x_set)
            # Y:0 X:1
            s2 = y_0_set.intersection(x_set)
            # Y:1 X:0
            s3 = y_1_set.difference(x_set)
            # Y:0 X:0
            s4 = y_0_set.difference(x_set)
        else:
            # Y:1 X:1
            s1 = y_1_set.intersection(x_set_1)
            # Y:0 X:1
            s2 = y_0_set.intersection(x_set_1)
            # Y:1 X:0
            s3 = y_1_set.intersection(x_set_0)
            # Y:0 X:0
            s4 = y_0_set.intersection(x_set_0)
        cb_df = pd.DataFrame()

        l1 = to_df(s1, all_paths, y_1_set, [1])
        l1 = l1.append(to_df(s2, all_paths, y_1_set, [1]))

        l2 = to_df(s3, all_paths, y_1_set, [0])
        l2 = l2.append(to_df(s4, all_paths, y_1_set, [0]))
        min_num = min(len(l1), len(l2))
        cb_df = cb_df.append(l1.sample(n=min_num), ignore_index=True)
        cb_df = cb_df.append(l2.sample(n=min_num), ignore_index=True)
        if len(cb_df) < k:
            logger.critical(" ".join([effect, cause, "<50"]))
            return 0
        elif len(cb_df.value_counts(["x", "y"])) == 1:
            logger.critical(" ".join([effect, cause, "XYConst", str(cb_df.value_counts(["x", "y"]).keys()[0])]))
            return 0
        else:
            scci = scci_given_data(cb_df, 'y', 'x', [])
            logger.critical(" ".join([effect, cause, "SCCI", str(scci)]))
            return scci
    z_dict = {}
    all_paths = []
    all_paths.extend(x_list)
    for i in range(len(z_list)):  # z1 z2 ...
        if z_list[i] not in high_low_types:
            if z_list[i] + '_set' not in memory.keys():
                tmp_set, tmp_list = run_query(
                    get_query(effect, z_list[i], large_set=large_set, dataset=dataset))
                memory[z_list[i] + '_set'] = tmp_set
                memory[z_list[i] + '_list'] = tmp_list
            else:
                tmp_set = memory[z_list[i] + '_set']
                tmp_list = memory[z_list[i] + '_list']
            z_dict['z' + str(i + 1)] = (tmp_set, tmp_list)
            all_paths.extend(tmp_list)
        else:
            q_z_1, q_z_0 = get_query_high_low(z_list[i], large_set=large_set, high_low_relation=high_low_relation)
            if z_list[i] + '_set_1' not in memory.keys():
                tmp_set_1, tmp_list_1 = run_query(q_z_1)
                tmp_set_0, tmp_list_0 = run_query(q_z_1)
                memory[z_list[i] + '_set_1'] = tmp_set_1
                memory[z_list[i] + '_list_1'] = tmp_list_1
                memory[z_list[i] + '_set_0'] = tmp_set_0
                memory[z_list[i] + '_list_0'] = tmp_list_0
            else:
                tmp_set_1 = memory[z_list[i] + '_set_1']
                tmp_list_1 = memory[z_list[i] + '_list_1']
                tmp_set_0 = memory[z_list[i] + '_set_0']
                tmp_list_0 = memory[z_list[i] + '_list_0']
            z_dict['z' + str(i + 1) + '_1'] = (tmp_set_1, tmp_list_1)
            z_dict['z' + str(i + 1) + '_0'] = (tmp_set_0, tmp_list_0)
            all_paths.extend(tmp_list_1)
            all_paths.extend(tmp_list_0)

    independent_result = []
    for com_length in range(len(z_list) + 1):
        for com in combinations(range(1, len(z_list) + 1), com_length):  # value=1 count number
            if cause not in high_low_types:
                # Y:1 X:1 Z1: Z2:
                s1 = y_1_set.intersection(x_set)
                # Y:0 X:1 Z1: Z2:
                s2 = y_0_set.intersection(x_set)
                # Y:1 X:0 Z1: Z2:
                s3 = y_1_set.difference(x_set)
                # Y:0 X:0 Z1: Z2:
                s4 = y_0_set.difference(x_set)
            else:
                # Y:1 X:1 Z1: Z2:
                s1 = y_1_set.intersection(x_set_1)
                # Y:0 X:1 Z1: Z2:
                s2 = y_0_set.intersection(x_set_1)
                # Y:1 X:0 Z1: Z2:
                s3 = y_1_set.intersection(x_set_0)
                # Y:0 X:0 Z1: Z2:
                s4 = y_0_set.intersection(x_set_0)
            l = []
            for z_num in range(1, len(z_list) + 1):
                if z_num in com:
                    l.append(1)
                    if z_list[z_num - 1] not in high_low_types:
                        s1 = s1.intersection(z_dict['z' + str(z_num)][0])
                        s2 = s2.intersection(z_dict['z' + str(z_num)][0])
                        s3 = s3.intersection(z_dict['z' + str(z_num)][0])
                        s4 = s4.intersection(z_dict['z' + str(z_num)][0])
                    else:
                        s1 = s1.intersection(z_dict['z' + str(z_num) + '_1'][0])
                        s2 = s2.intersection(z_dict['z' + str(z_num) + '_1'][0])
                        s3 = s3.intersection(z_dict['z' + str(z_num) + '_1'][0])
                        s4 = s4.intersection(z_dict['z' + str(z_num) + '_1'][0])
                else:
                    l.append(0)
                    if z_list[z_num - 1] not in high_low_types:
                        s1 = s1.difference(z_dict['z' + str(z_num)][0])
                        s2 = s2.difference(z_dict['z' + str(z_num)][0])
                        s3 = s3.difference(z_dict['z' + str(z_num)][0])
                        s4 = s4.difference(z_dict['z' + str(z_num)][0])
                    else:
                        s1 = s1.intersection(z_dict['z' + str(z_num) + '_0'][0])
                        s2 = s2.intersection(z_dict['z' + str(z_num) + '_0'][0])
                        s3 = s3.intersection(z_dict['z' + str(z_num) + '_0'][0])
                        s4 = s4.intersection(z_dict['z' + str(z_num) + '_0'][0])
            cb_df = pd.DataFrame()

            l1 = to_df(s1, all_paths, y_1_set, [1] + l)
            l1 = l1.append(to_df(s2, all_paths, y_1_set, [1] + l))

            l2 = to_df(s3, all_paths, y_1_set, [0] + l)
            l2 = l2.append(to_df(s4, all_paths, y_1_set, [0] + l))
            min_num = min(len(l1), len(l2))
            cb_df = cb_df.append(l1.sample(n=min_num), ignore_index=True)
            cb_df = cb_df.append(l2.sample(n=min_num), ignore_index=True)

            if len(cb_df) < k:
                logger.critical(" ".join([effect, cause, ",".join(z_list), str(l), "<50"]))
                independent_result.append(0)
            elif len(cb_df.value_counts(["x", "y"])) == 1:
                logger.critical(" ".join([effect, cause, ",".join(z_list), str(l), "XYConst",
                                          str(cb_df.value_counts(["x", "y"]).keys()[0])]))
                independent_result.append(0)
            else:
                scci = scci_given_data(cb_df, 'y', 'x', ['z' + str(i + 1) for i in range(len(z_list))])
                logger.critical(" ".join([effect, cause, ",".join(z_list), str(l), "SCCI", str(scci)]))
                independent_result.append(
                    scci_given_data(cb_df, 'y', 'x', ['z' + str(i + 1) for i in range(len(z_list))]))
            # print(com)
    # print(independent_result)
    return sum(independent_result)


if __name__ == "__main__":
    memory = {}
    # res = douban_gen_data(effect='collect', cause='collect_invactor_composer',
    #                       z_list=['wish_invactor_actor', 'wish_invdirector_writer'],
    #                       high_low_types=['collect_invactor_composer', 'collect'], memory=memory)
    # res = douban_gen_data(effect='collect', cause='collect_invactor_composer',
    #                       z_list=[],
    #                       high_low_types=['collect_invactor_composer', 'collect'], memory=memory)

    res = douban_gen_data(effect='collect', cause='collect_invwriter_writer',
                        z_list=['collect_invdirector_writer', 'collect_invwriter_director'],
                        high_low_types=['collect_invdirector_writer', 'collect', 'collect_invwriter_writer', 'collect_invwriter_director'],
                        large_set=['collect_invdirector_writer', 'collect_invwriter_writer', 'collect_invwriter_director'],
                        high_low_relation=['collect'],
                        memory=memory)
    print(res)
