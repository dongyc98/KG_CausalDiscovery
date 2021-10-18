import pandas as pd
import os

def path_to_data(data_path='./douban_sample/collect01/'):
    '''
    from the searched path file to generate the training file.
    merge the collect path with 0 (rating < 4) and 1 (rating >= 4)
    :param data_path:
    :return:
    '''
    df = pd.read_csv(os.path.join(data_path, 'all_path01.txt'), delimiter='\t', header=None)
    # print(df)
    group = df.groupby([0, 1, 2])
    build_df = pd.DataFrame()
    for tup, data in group:
        user, item, rating = tup
        if rating >= 4.0:
            vote = 1
        else:
            vote = 0
        # vote = round(rating)
        tmp = {}
        tmp['rating'] = vote
        for idx, row in data.iterrows():
            sp = row[3].split('_')
            tf_count = 0
            tf_idx = 0
            for i in range(len(sp)):
                if sp[i] in ['True', 'False']:
                    tf_count = tf_count + 1
                    tf_idx = i
            if tf_count == 3: # collect invcollect collect
                continue
            elif tf_count == 1:
                if sp[tf_idx] == "True":
                    sp[tf_idx] = "collect"
                    tmp["_".join(sp)] = 1
                else:
                    sp[tf_idx] = "collect"
                    tmp["_".join(sp)] = 0
            else:
                tmp[row[3]] = 1
        new = pd.DataFrame(tmp, index=[0])

        build_df = build_df.append(new, ignore_index=True)
    # sample neg
    # pos = build_df[build_df['rating'] != 0]
    # neg = build_df[build_df['rating'] == 0]
    # neg_ = neg.sample(n=len(pos), replace=False, random_state=10086).reset_index(drop=True)
    # new_data = pd.concat([pos, neg_], ignore_index=True)
    # new_data.to_csv(os.path.join(data_path, 'sample_data3.csv'), index=False)

    collect_df = build_df.filter(regex="collect")
    not_collect_df = build_df.filter(items=[col for col in build_df.columns if 'collect' not in col])
    not_collect_df = not_collect_df.fillna(0)
    final_df = pd.concat([not_collect_df, collect_df], axis=1)
    final_df.to_csv(os.path.join(data_path, 'sample_data_collect_na.csv'), index=False)


if __name__ == '__main__':
    path_to_data()