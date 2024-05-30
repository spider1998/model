# 使用arima预测时间序列
import numpy as np
import sys
from pmdarima.arima import auto_arima
import json
import argparse
import sqlite3
import pymysql as mysql

import logging

log_file_path = 'arima.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class ARIMA: 

    def __init__(self, args):
        self.args = args
        self.config = None
        self.read_config()

        self.data = None
        self.result = None
        self.length = 0

    def read_config(self):
        # 读取配置文件
        with open(self.args.config, 'r') as f:
            self.config = json.load(f)

      # 从数据库中读取数据
    def read_data_from_db(self):
        if "db" not in self.config.keys():
            logging.error("config db is error")
            print("config db is error")
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
            logging.error("config db is error")
            print("config db is error")
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
    
    def read_data_from_sqlite(self):
        # 从sqlite中读取数据
        # 连接数据库文件
        conn = sqlite3.connect(self.config['sqlite']['values']['path'])

        table_name = self.config['sqlite']['in']['values']
        date = self.args.date

        # 读取数据
        # 取对应type的数据
        sql = "SELECT * from {} where type = '{}' and time < '{}'".format(table_name, self.args.type, date)

        cursor = conn.execute(sql)
        self.data = []
        for row in cursor:
            self.data.append(row[3])

        # print(self.data)
        self.length = len(self.data)
        self.data = np.array(self.data)
        # 关闭数据库连接
        cursor.close()
        conn.close()

    def write_data_to_sqlite(self):
        # 将数据写入sqlite
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
    

    def arima_predict(self, data, output_length=1):
        # data 为一维数组
        # input_length 为输入数据长度
        # output_length 为输出数据长度
        # 禁用输出
        model = auto_arima(data, start_p=1, start_q=1,
                        test='adf',
                        max_p=3, max_q=3, m=1,
                        start_P=0, seasonal=True,
                        d=None, D=1, trace=True,
                        error_action='ignore',
                        suppress_warnings=True,
                        stepwise=True)
        
        model.fit(data)

        forecast = model.predict(n_periods=output_length)

        self.result = (self.args.date, forecast[0])
        
        return forecast


if __name__ == "__main__":
    # Add the arguments from the command line
    pass