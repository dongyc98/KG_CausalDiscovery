# KG_CausalDiscovery

## 项目数据处理和算法代码

douban_data_process.py 从原始数据采样生成需要的数据，并且导入数据库

douban_gen_path.py 对所有的pair多线程搜索path，保存到文件

douban_exp_gen_data.py 从搜索出的path文件，构建将collect融合后（01表示高低分）的用于rule mining的数据

rec_path_fast2.R 根据上个文件的数据进行rule mining。

douban_link_prediction.py 针对fan，genre两个rule，生成预测准确率结果

douban_link_prediction_NeuralLP.py 对NeuralLP的结果分析预测准确率

## 新流程的Rule Mining代码在rule mining文件夹中

r_from_python.py 通过python调用R的scci函数，给定dataframe和作为effect、cause、z的列名称，计算在给定z的情况下effect和cause的条件独立性情况。

gen_data.py 其中douban_gen_data函数，输入effect、cause和条件变量列表z_list，以及高低分类型的所有path名称（用于判断不同的搜索命令），和空的memory字典（用于记录查询结果）。对于 x，y|Z=z，有三种情况判断独立 1.样本数低于50； 2.x, y取定值； 3.检验独立。在z所有取值组合都判断独立的情况下， 给出独立的结果。scci=0

main.py 主函数，输入所有的path类型和effect变量，会枚举所有的cause变量和条件变量，调用douban_gen_data进行判断，如果判断独立，那么从整体的候选集合中去掉，直到终止，将结果输入到log文件中。