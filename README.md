# KG_CausalDiscovery

## 项目数据处理和Rule Mining代码

douban_data_process.py 从原始数据采样生成需要的数据，并且导入数据库

douban_gen_path.py 对所有的pair多线程搜索path，保存到文件

douban_exp_gen_data.py 从搜索出的path文件，构建将collect融合后（01表示高低分）的用于rule mining的数据

rec_path_fast2.R 根据上个文件的数据进行rule mining。

douban_link_prediction.py 针对fan，genre两个rule，生成预测准确率结果

douban_link_prediction_NeuralLP.py 对NeuralLP的结果分析预测准确率

