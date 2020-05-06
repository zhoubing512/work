# -*- coding: utf-8 -*-
"""
Created on Thu Apr  9 23:23:36 2020

@author: ShiningLu
"""


import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
import matplotlib.pyplot as plt
from sklearn.preprocessing import StandardScaler
from sklearn.preprocessing import MinMaxScaler
data = pd.read_table('E:\\Desktop\\zb_work\\10类特征用户进行聚类\\zb_10part_cluster_data.txt',sep=',',header=None)
data.columns = ['phone_no','education','finance_pay','online_shopping','fast_mail','vedio_target','wzry_user','stay_work_yidi','tvb','child_to_family','student_wb_use','back_deyang','dm_user']
data0 = data.drop(['phone_no'],axis=1)

ss = StandardScaler()
data1 = ss.fit_transform(data0)


kmeans=KMeans(n_clusters=6)   #n_clusters:number of cluster  
kmeans.fit(data0)  
 
print(kmeans.labels_)
sum(kmeans.labels_)                        #显示每个样本所属的簇
print(kmeans.cluster_centers_)                #4个中心点的坐标
print(kmeans.inertia_)                        #用来评估簇的个数是否合适，代表所有点到各自中心的距离和，距离越小说明簇分的越好，选取临界点的簇个数
r1 = pd.Series(kmeans.labels_).value_counts()
print(r1)                                  #统计每个类别下样本个数
print(kmeans.values)



# '利用SSE选择k'
SSE = []  # 存放每次结果的误差平方和
for k in range(1, 12):
    estimator = KMeans(n_clusters=k)  # 构造聚类器
    estimator.fit(data0)
    SSE.append(estimator.inertia_)

X = range(1, 12)
plt.xlabel('k')
plt.ylabel('SSE')
plt.plot(X, SSE, 'o-')
plt.show()