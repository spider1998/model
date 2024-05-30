# cython: language_level=3
import numpy as np
from pyswarm import pso
from keras.models import Sequential
from keras.layers import Dense, LSTM
from vmdpy import VMD
from sklearn.preprocessing import MinMaxScaler
import sys
import os
import json
import argparse
import sqlite3
import pymysql as mysql

# 去掉warning
import warnings
warnings.filterwarnings('ignore')

import logging

log_file_path = 'vmd_apso_lstm.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class VMD_APSO_LSTM:
    
    def __init__(self, args) -> None:
        self.args = args
        self.data = None
        self.length = 0
        self.predict_length = 1
        self.look_back = 10

        self.vmd_alpha = 3000  # 需要调整的参数
        self.vmd_tau = 1
        self.vmd_K = 7  # 要提取的模式数
        self.vmd_DC = 0
        self.vmd_init = 1
        self.vmd_tol = 1e-7

        # 从config文件中读取数据库参数
        self.config = None
        self.read_config()
        self.result = None

    # 读取参数文件
    def read_config(self):
        with open(self.args.config, 'r') as f:
            self.config = json.load(f)

    # 从数据库中读取数据
    def read_data_from_db(self):
        if "db" not in self.config.keys():
            print("config db is error")
            logging.error("config db is error")
            exit(1)
        
        # 数据库参数
        self.db_config = self.config['db']['values']

        address = self.db_config['address']
        port = self.db_config['port']
        user = self.db_config['user']
        password = self.db_config['password']
        database = self.db_config['database'] 

        # 建立数据库连接
        conn = mysql.connect(host=address, port=port, user=user, password=password, database=database)
        cursor = conn.cursor()

        table_name = self.config['db']['in']['values']
        date = self.args.date

        # 取对应type的数据, time列小于date的数据
        sql = "SELECT (VALUE) from {} where type = '{}' and time < '{}'".format(table_name, self.args.type, date)
        cursor.execute(sql)
        self.data = []
        self.data = [float(row[0]) for row in cursor.fetchall()]

        self.length = len(self.data)

        # 关闭数据库连接
        cursor.close()
        conn.close()
    
    # 写入数据到数据库
    def write_data_to_db(self):
        if "db" not in self.config.keys():
            print("config db is error")
            logging.error("config db is error")
            exit(1)
        
        # 数据库参数
        self.db_config = self.config['db']['values']

        address = self.db_config['address']
        port = self.db_config['port']
        user = self.db_config['user']
        password = self.db_config['password']
        database = self.db_config['database'] 

        # 建立数据库连接
        conn = mysql.connect(host=address, port=port, user=user, password=password, database=database)
        cursor = conn.cursor()

        table_name = self.config['db']['out']['values']
        batch = self.args.batch
        date = self.args.date
        _type = self.args.type
        value = self.result[1]

        # value 取前三位小数
        value = round(value, 3)

        sql = "INSERT INTO {} (TYPE, TIME, VALUE, BATCH) VALUES ({}, {}, {}, '{}')".format(table_name, _type, date, value, batch)
        cursor.execute(sql)
        conn.commit()

        # 关闭数据库连接
        cursor.close()
        conn.close()

    # 从sqlite中读取数据
    def read_data_from_sqlite(self):
        # 连接数据库文件
        conn = sqlite3.connect(self.config['sqlite']['values']['path'])

        table_name = self.config['sqlite']['in']['values']
        date = self.args.date

        # 读取数据
        # 取对应type的数据, time列小于date的数据
        sql = "SELECT * from {} where type = '{}' and time < '{}'".format(table_name, self.args.type, date)
        # sql = "SELECT * from {} where type = '{}'".format(table_name, self.args.type)
        cursor = conn.execute(sql)
        self.data = []
        for row in cursor:
            self.data.append(row[3])

        self.length = len(self.data)

        # 关闭数据库连接
        cursor.close()
        conn.close()
        

    # 向sqlite中写入数据
    def write_data_to_sqlite(self):
        # 连接数据库文件
        conn = sqlite3.connect(self.config['sqlite']['values']['path'])
        cursor = conn.cursor()

        table_name = self.config['sqlite']['out']['values']

        # max_id
        sql = "select max(ID) from {}".format(table_name)
        cursor.execute(sql)
        tmp = cursor.fetchall()
        max_id = int(tmp[0][0]) if tmp[0][0] else 0
        id = max_id + 1
        timex = self.result[0]
        value = float(self.result[1])
        _type = int(self.args.type)

        # value 取前两位小数
        value = round(value, 3)

        print("id: {}, type: {}, time: {}, value: {}".format(id, _type, timex, value))

        # 存储数据
        sql = f"INSERT INTO {table_name} (ID, TYPE, TIME, VALUE) VALUES ({id}, {_type}, '{timex}', {value})"

        cursor.execute(sql)
        conn.commit()

        # 关闭数据库连接
        cursor.close()
        conn.close()


    # vmd分解
    def vmd_decompose(self):
        u, u_hat, omega = VMD(self.data, self.vmd_alpha, self.vmd_tau, self.vmd_K, self.vmd_DC, self.vmd_init, self.vmd_tol)
        return u, u_hat, omega
    
    # vmd合并 返回加权平均模态
    def vmd_reconstruct(self, modes):
        modes = np.sum(modes, axis=0)

        return modes

    # 创建训练集和测试集
    def create_dataset(self, data):
        look_back = self.look_back
        X, Y = [], []
        for i in range(len(data)-look_back):
            X.append(data[i:(i+look_back)])
            Y.append(data[i+look_back])
        return np.array(X), np.array(Y)

    # 预测时构建数据集
    def create_dataset_predict(self, data):
        look_back = self.look_back
        X, Y = [], []
        for i in range(look_back, len(data)):
            X.append(data[i-look_back:i])
        return np.array(X), np.array(Y)
    
    # 时间序列预测结果
    def predict(self, model, X, Y, batch_size):
        Y_pred = model.predict(X, batch_size=batch_size)
        return Y, Y_pred


    # 模型定义
    def create_model(self, lstm_units, input_length):
        model = Sequential()
        model.add(LSTM(lstm_units, input_shape=(input_length, 1)))
        model.add(Dense(1, activation='sigmoid'))

        # 时间序列预测任务
        model.compile(loss='mean_squared_error', optimizer='adam')
        return model



    # 定义适应度函数，返回模型在测试集上的准确率
    def fitness_function(self, params, X_train, y_train, X_val, y_val, batch_size, input_length):
        # print(batch_size, input_length, params)
        # 解析参数
        learning_rate = float(params[0])
        epochs = int(params[1])
        lstm_units = int(params[2])
        batch_size = batch_size
        input_length = input_length

        model = self.create_model(lstm_units, input_length)

        # 训练模型
        model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, verbose=0)
        
        # 在测试集上评估模型MSE
        Y_test, Y_test_pred = self.predict(model, X_val, y_val, batch_size)
        mse = np.mean(np.power(Y_test - Y_test_pred, 2))

        # 返回在测试集上的mse,最小化MSE
        return mse
    
    # apso
    def apso(self, X_train, y_train, X_val, y_val, batch_size, input_length, param_ranges):
        lb = []
        ub = []
        for r in param_ranges:
            lb = np.append(lb, r[0])
            ub = np.append(ub, r[1])

            # 使用APSO算法搜索最优参数
        best_params, best_fitness = pso(self.fitness_function, lb = lb, ub = ub, swarmsize=5, maxiter=10, debug=True, args=(X_train, y_train, X_val, y_val, batch_size, input_length))

        # 输出最优参数和对应的准确率
        print('Best parameters:', best_params)
        print('Best MSE:', best_fitness)

        return best_params
    
    # 训练
    def train(self):
        is_use_apso = self.args.is_use_apso
        is_save = self.args.is_save
        N = self.length
        data = np.array(self.data)
        raw_data = data
        output_path = self.config["VMD_APSO_LSTM"][self.args.type]["path"]["values"]

        u, u_hat, omega = self.vmd_decompose()

        Y_test_all, Y_test_pred_all = [], []


        size = (0.7, 0.1, 0.2)
        params = [[0.0878169084,    100.0,          26.9215994, 10, 4],
                [ 0.1,            94.59478797,    30.23627443, 10, 4],
                [1.00000000e-03,  9.25498616e+01, 3.14512071e+01, 10, 4],
                [7.56982211e-02,  8.69808005e+01, 2.94155693e+01, 10, 4],
                [ 0.1,            92.84340612,    32.       , 10, 4],
                [ 0.09706563,     79.9580571,     27.58093663, 10, 4],
                [ 0.05725051,     44.21705189,    30.86399933, 10, 4]]
        


        # 选择其中一个模式进行预测
        index = 0
        for data in u:
            data = data.reshape(-1, 1)

            train_size, val_size, test_size = int(N * size[0]), int(N * size[1]), int(N * size[2])
            # 归一化
            scaler = MinMaxScaler(feature_range=(0, 1))
            data = scaler.fit_transform(data)
            train_data, val_data, test_data = data[:train_size], data[train_size:train_size+val_size], data[train_size+val_size:]

            params_path = os.path.join(output_path, str(index) + '_params.txt')
            # with open(params_path) as f:
            #     test_params = f.read()

            # 读取参数
            # input_length = int(float(test_params.split(',')[3].strip()))
            # batch_size = int(float(test_params.split(',')[4].strip()))
            input_length = 10
            batch_size = 4

            look_back = input_length  # 每个输入序列的长度为10
            X_train, y_train = self.create_dataset(train_data)
            X_val, y_val = self.create_dataset(val_data)
            X_test, y_test = self.create_dataset(test_data)

            # 将输入序列转换成3D格式，以便LSTM模型能够接受
            X_train = np.reshape(X_train, (X_train.shape[0], X_train.shape[1], 1))
            X_val = np.reshape(X_val, (X_val.shape[0], X_val.shape[1], 1))

            if is_use_apso:
                # 定义要调整的参数范围
                param_ranges = [
                    (0.001, 0.1),  # 学习率
                    (10, 100),  # 迭代次数
                    (4, 32)  # LSTM单元数量
                ]
                best_params = self.apso(X_train, y_train, X_val, y_val, batch_size, input_length, param_ranges)
                best_params = best_params.tolist()
            else:
                best_params = params[index]

            # 构建最优模型
            learning_rate = best_params[0]
            epochs = int(best_params[1])
            lstm_units = int(best_params[2])
            

            model = self.create_model(lstm_units, input_length)

            # 训练模型
            model.fit(X_train, y_train, epochs=epochs, batch_size=batch_size, verbose=0)


            # 在测试集上评估模型MSE
            Y_test, Y_test_pred = self.predict(model, X_test, y_test, batch_size)
            mse = np.mean(np.power(Y_test - Y_test_pred, 2))

            if is_save:
                # 保存模型
                weight_path = os.path.join(output_path, str(index) + '_weight.h5')
                model.save(weight_path)

                best_params.append(look_back)
                best_params.append(batch_size)
                # 保存最优参数, 不要括号
                with open(params_path, 'w') as f:
                    best_params = str(best_params).replace('[', '').replace(']', '')
                    f.write(str(best_params))

            # 还原原始数据范围
            Y_test_pred = scaler.inverse_transform(Y_test_pred)
            Y_test_pred_all.append(Y_test_pred)

            index += 1

        # 将所有vmd模式的预测结果相加
        Y_test_pred_all = np.array(Y_test_pred_all)
        Y_test_pred = self.vmd_reconstruct(Y_test_pred_all)

        Y_test = raw_data[-len(Y_test_pred):]
        Y_test = Y_test.reshape(-1, 1)
        
        mae = np.mean(np.abs(Y_test - Y_test_pred))
        me = np.mean(Y_test - Y_test_pred)
        rmse = np.sqrt(np.mean(np.power(Y_test - Y_test_pred, 2)))
        NSE = 1 - np.sum(np.power(Y_test - Y_test_pred, 2)) / np.sum(np.power(Y_test - np.mean(Y_test_pred), 2))
        PBIAS = np.sum(Y_test_pred - Y_test) / np.sum(Y_test)
        
        print("MAE: %.4f, ME: %.4f, RMSE: %.4f, NSE: %.4f, PBIAS: %.4f" % (mae, me, rmse, NSE, PBIAS))
        logging.info("MAE: %.4f, ME: %.4f, RMSE: %.4f, NSE: %.4f, PBIAS: %.4f" % (mae, me, rmse, NSE, PBIAS))
        return mae, me, rmse, NSE, PBIAS

    # 推理
    def inference(self):
        data_infer_all, data_real_all = [], []
        K = self.vmd_K
        path = self.config["VMD_APSO_LSTM"][self.args.type]["path"]['values']
        data = np.array(self.data)

        # 计算vmd

        u, _, _ = self.vmd_decompose()

        for i in range(K):
            data = u[i]
            data = data.reshape(-1, 1)

            # 读取参数
            param_path = os.path.join(path, str(i) + '_params.txt')
            with open(param_path, 'r') as f:
                best_params = f.read()

            learning_rate = float(best_params.split(',')[0].strip())
            epochs = int(float(best_params.split(',')[1].strip()))
            lstm_units = int(float(best_params.split(',')[2].strip()))
            input_length = int(float(best_params.split(',')[3].strip()))
            batch_size = int(float(best_params.split(',')[4].strip()))

            # 创建模型
            model = self.create_model(lstm_units, input_length)

            weight_path = os.path.join(path, str(i) + '_model.h5')
            model.load_weights(weight_path)

            # 归一化
            scaler = MinMaxScaler(feature_range=(0, 1))
            data = scaler.fit_transform(data)

            X_test, Y_test = self.create_dataset_predict(data)

            _, data_infer = self.predict(model, X_test, Y_test, batch_size)
            
            # 还原原始数据范围
            data_infer = scaler.inverse_transform(data_infer)
            data_infer_all.append(data_infer)

        data_infer_all = np.array(data_infer_all)

        # 将所有vmd模式的预测结果相加
        predict_data = self.vmd_reconstruct(data_infer_all)

        if predict_data[-1] < 0:
            predict_data[-1] = 0

        next_data = predict_data[-1]

        self.result = (self.args.date, next_data[0])
        return next_data


if __name__ == "__main__":
    # Add the arguments from the command line
    pass