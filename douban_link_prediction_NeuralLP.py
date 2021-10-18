from collections import defaultdict


def read_file(path):
    '''
    read the Neural_LP probs file and get the link prediction accuracy.
    :param path:
    :return:
    '''
    user2cand = dict()
    with open(path, 'r') as f:
        for line in f.readlines():
            if not line.strip().startswith('inv'):
                continue
            spt = line.strip().split(",")
            if spt[0].endswith("high"):
                v = '1'
            else:
                v = '0'
            m = spt[1]
            u = spt[2]
            cand_set = [(s.split("|")[0], float(s.split("|")[1])) for s in spt[3:] if s.startswith("tt") and float(s.split("|")[1])>0]
            if u not in user2cand.keys():
                user2cand[u] = {'1': [], '0': [], 'Truth1': [], 'Truth0': []}
            user2cand[u][v].extend(cand_set)
            user2cand[u]['Truth'+v].append(m)
    count = 0
    correct = 0
    for u in user2cand.keys():
        L1 = [m for m, _ in sorted(user2cand[u]['1'], key=lambda x: x[1], reverse=True)]
        L0 = [m for m, _ in sorted(user2cand[u]['1'], key=lambda x: x[1], reverse=True)]
        for m in user2cand[u]['Truth1']:
            count = count + 1
            if (m in L1) and (m not in L0):
                correct = correct + 1
                continue
            if (m in L1) and (m in L0):
                if L1.index(m) <= L0.index(m):
                    correct = correct + 1
        for m in user2cand[u]['Truth0']:
            count = count + 1
            if (m in L0) and (m not in L1):
                correct = correct + 1
                continue
            if (m in L0) and (m in L1):
                if L0.index(m) <= L1.index(m):
                    correct = correct + 1
    print('Acc:', 1.0*correct/count, 'correct:', correct, 'count:', count)



if __name__ == '__main__':
    path = "./exps/douban_setting2/test_preds_and_probs.txt"
    # path = "./douban_setting2/sample.txt"
    read_file(path)