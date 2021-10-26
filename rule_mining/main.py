import logging
from itertools import combinations

from py2neo import Graph

from gen_data import douban_gen_data
from path_store import *


def filter_wish(all_paths):
    l = []
    for p in all_paths:
        if p.find('wish') == -1:
            l.append(p)
    return l


def filter_paths_num(all_paths, dataset, k=1000, large_size=1e5):
    graph = Graph("http://localhost:7474", auth=("neo4j", "qwer1234"))
    l = []
    large = []
    for p in all_paths:
        path_rels = p.split('_')
        if dataset == 'family':
            rs = []
            idx = 0
            while idx < len(path_rels):
                if path_rels[idx] == 'inv':
                    rs.append('inv_' + path_rels[idx + 1])
                    idx = idx + 1
                else:
                    rs.append(path_rels[idx])
                idx = idx + 1
            path_rels = rs
        if len(path_rels) == 1:
            qs = "match p=(n1)-[:%s]->(n2) return count(p)" % (path_rels[0])
        elif len(path_rels) == 2:
            qs = "match p=(n1)-[:%s]->(n2)-[:%s]->(n3) return count(p)" % (path_rels[0], path_rels[1])
        else:
            qs = "match p=(n1)-[:%s]->(n2)-[:%s]->(n3)-[:%s]->(n4) return count(p)" % (path_rels[0], path_rels[1], path_rels[2])
        res = graph.query(qs)
        for line in res:
            if line[0] >= k:
                l.append(p)
            if line[0] > large_size:
                large.append(p)
    return l, large


def main(effect, all_path_types: list, memory: dict, high_low_types=['collect'], without_wish=False, high_low_relations=['collect', 'invcollect'],
         k=1000, large_size=1e5, dataset='douban'):
    '''

    :param dataset:
    :param large_size: larger than size will not be data expansion
    :param k: smaller than k is filtered
    :param effect:
    :param all_path_types: all possible cause path types
    :param memory: to store the query result. quick.
    :param high_low_types: like paths contain 'collect' in douban dataset.
    :param without_wish: whether filter wish in douban dataset
    :param high_low_relations: like collect, if not, regard collect as saw.
    :return:
    '''
    logger = logging.getLogger("result")
    print("Origin paths:", len(all_path_types))
    if without_wish:
        causes = filter_wish(all_path_types)
    else:
        causes = all_path_types.copy()
    if 'filtered_causes' not in memory.keys():
        causes, large_set = filter_paths_num(causes, dataset=dataset, k=k, large_size=large_size)
        memory['filtered_causes'] = causes.copy()
        memory['filtered_large_set'] = large_set.copy()
    else:
        causes = memory['filtered_causes']
        large_set = memory['filtered_large_set']
    print("Total paths:", len(causes), "Large paths:", len(large_set))
    logger.critical("Total paths: " + str(len(causes)) + " Large paths: " + str(len(large_set)))
    n = len(causes)
    mins = {}
    for p in causes:
        mins[p] = 1e5
    causes_tmp = causes.copy()
    for cond in range(n):
        causes = causes_tmp.copy()
        if (cond+2) > len(causes):
            for r in causes:
                print(" ".join([effect, r, '-1', str(mins[r])]))
                logger.critical(" ".join([effect, r, '-1', str(mins[r])]))
            break
        for r in causes:
            if r == effect:
                continue
            if cond == 0:
                scci = douban_gen_data(effect, r, [], high_low_types, memory, large_set, high_low_relation=high_low_relations, dataset=dataset)
                if scci == 0:
                    print(" ".join([effect, r, str(cond)]))
                    logger.critical(" ".join([effect, r, str(cond)]))
                    causes_tmp.remove(r)
                    continue
                else:
                    mins[r] = min(mins[r], scci)
            else:
                causes_new = [c for c in causes if c not in [r, effect]]
                for com in combinations(causes_new, cond):
                    scci = douban_gen_data(effect, r, list(com), high_low_types, memory, large_set, high_low_relation=high_low_relations, dataset=dataset)
                    if scci == 0:
                        print(" ".join([effect, r, " ".join(list(com)), str(cond)]))
                        logger.critical(" ".join([effect, r, " ".join(list(com)), str(cond)]))
                        causes_tmp.remove(r)
                        break
                    else:
                        mins[r] = min(mins[r], scci)


if __name__ == '__main__':
    logging.basicConfig(level=logging.ERROR, filename='result4.log', filemode='a+',
                        format="%(message)s")
    dataset = 'family'
    if dataset == 'douban':
        all_path_types = douban_paths()
        # all_path_types = all_path_types + ',wish_parentguide_invparentguide,collect_parentguide_invparentguide'
        all_path_types = all_path_types.split(',')
        final_rules = "wish_invwish_wish,collect_invproducer_producer,collect_invdirector_director,collect_inveditor_editor,collect_invwriter_writer,collect_invdirector_writer,collect_invcinematographer_cinematographer,collect_invwriter_director"
        final_rules = final_rules.split(',')

        high_low_types = [r for r in all_path_types if 'collect' in r]
        memory = {}
        effect = "collect"
        # main(effect, final_rules, memory, high_low_types, without_wish=False, high_low_relations=['collect', 'invcollect'])
        main(effect, final_rules, memory, high_low_types=[], without_wish=False, high_low_relations=[])
    elif dataset == 'wn18':
        memory = {}
        all_path_types = wn18_2hop() + ',' + wn18_3hop()
        all_path_types = all_path_types.split(',')
        for i in range(18):  # r0 - r17
            effect = 'r'+str(i)
            main(effect, all_path_types, memory, high_low_types=[], high_low_relations=[], k=200, dataset='wn18')
    elif dataset == 'family':
        memory = {}
        all_path_types = family_paths()
        all_path_types = all_path_types.split(',')
        relation_types = ['wife', 'husband', 'father', 'mother', 'son', 'daughter', 'uncle', 'aunt', 'nephew', 'niece',
                          'brother', 'sister']
        for effect in relation_types:
            main(effect, all_path_types, memory, high_low_types=[], high_low_relations=[], k=500, dataset='family')


