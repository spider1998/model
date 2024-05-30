# 新安江模型计算径流量
import json
import numpy as np
import sqlite3
import argparse
import datetime
import pymysql as mysql
import math
from scipy import interpolate
from collections import defaultdict

import logging
import os

log_file_path = 'XAJ_PR_REAL.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class XAJ_PR():
    def __init__(self, args):
        # type表示河流类型，1为dongpu，2为dafangying
        self.ins = args

        self.config_path = args.config
        self.type = args.type
        self.data = {}
        self.length = None
        self.result_xaj = []
        self.result_pr = []
        self.result_pr_r = []
        self.result = []
        self.length = 0

        # 计算水位所需数据
        self.inflow = []
        self.outflow = []

        self.z_table = []     # 水位表
        self.v_table = []     # 库容表

        self.config = None

        # 对应的面雨量和蒸散数据
        self.P0, self.E0 = None, None

        self.load_config()

    def reset_args(self, args):
        self.ins = args

    def load_config(self):
        # 从配置文件中读取配置
        with open(self.config_path, 'r', encoding="utf-8") as f:
            self.config = json.load(f)

    # 判断大洪水: 当前时刻及之前23个时刻降水量之和大于100mm，或者当前时刻及之前23个时刻中至少有一个时段降水量大于45mm。当前时刻为rainfull[23]
    # 小洪水：当前时刻及之前23个时刻降水量之和大于26mm且小于100。
    # 非洪水：认为当前时刻及之前23个时刻降水量之和小于26mm。
    def judge_flood_type(self):
        nowtime_index = self.ins.n1
        # 若降雨量存在
        if self.data['rainfull'] != [] or len(self.data['rainfull']) > 0:
            if self.type == "0":
                # 判断是否大洪水
                if sum(self.data['rainfull'][nowtime_index-24:nowtime_index]) >= 100 or max(self.data['rainfull'][nowtime_index-24:nowtime_index]) >= 45:
                    return 2
                elif sum(self.data['rainfull'][nowtime_index-24:nowtime_index]) >= 26 and sum(self.data['rainfull'][nowtime_index-24:nowtime_index]) < 100:
                    return 1
                else:
                    return 0
            else:
                if sum(self.data['rainfull'][nowtime_index-24:nowtime_index]) >= 20:
                    return 1
                else:
                    return 0

    # 从监控数据库中读取数据
    def load_data_from_monitor_db(self):
        if "db" not in self.config.keys():
            print("config db is error")
            logging.error("config db is error")
            exit(1)

        try:
            # 数据库参数
            self.db_config = self.config['db']['values']

            address = self.db_config['address']
            port = self.db_config['port']
            user = self.db_config['user']
            password = self.db_config['password']
            database = self.db_config['database_monitor']

            # 入参
            start_time = self.ins.start

            # 向前取n1个小时的数据
            pre_start_time = (datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") -
                              datetime.timedelta(hours=self.ins.n1)).strftime("%Y-%m-%d %H:%M:%S")
            # end_time = self.ins.end
            _n1 = self.ins.n1 + 1
            _n2 = self.ins.n2

            PPTN_table_name = self.config['db']['in']['values']['point_rainfall']
            tablename_zv = self.config['db']['in']['values']['zv']
            tablename_version_zv = self.config['db']['in']['values']['zv_version']

            PPTN_surface = self.config['db']['in']['values']['surface_pptn']

            try:
                conn = mysql.connect(
                    host=address, port=port, user=user, password=password, database=database)
            except BaseException as e:
                print("connect to mysql error: {}".format(e))
                logging.error("connect to mysql error: {}".format(e))
                exit(1)

            # 获取游标
            cursor = conn.cursor()

            # 获取当前zv table的 ID 和 ENABLE字段
            sql = "SELECT ID, ENABLE FROM {}".format(tablename_version_zv)

            cursor.execute(sql)

            tmp = cursor.fetchall()

            # 找到ENABLE为0时，ID的值
            self.zv_type = "0"
            for row in tmp:
                if row[1] == 0:
                    self.zv_type = str(row[0])
                    break

            print("zv type: {}".format(self.zv_type))

            # 获取z和v
            sql = "SELECT (RZ) FROM {} WHERE WATER_TYPE = '{}' and ZV_TYPE = '{}'".format(
                tablename_zv, self.ins.type, self.zv_type)
            cursor.execute(sql)

            self.z_table = [float(row[0]) for row in cursor.fetchall()]

            sql = "SELECT (W) FROM {} WHERE WATER_TYPE = '{}' and ZV_TYPE = '{}'".format(
                tablename_zv, self.ins.type, self.zv_type)
            cursor.execute(sql)

            self.v_table = [float(row[0]) for row in cursor.fetchall()]

            # 获取点雨量数据
            sql = "select * from {} where TM between '{}' and '{}' order by TM".format(
                PPTN_table_name, pre_start_time, start_time)

            cursor.execute(sql)

            # 读取数据
            tmp = cursor.fetchall()

            point_rainfall_result = defaultdict(list)
            point_rainfall_tmp = defaultdict(list)

            for row in tmp:
                point_rainfall_tmp[row[0]].append((row[1], row[2]))

            # 根据pre_start_time和start_time计算长度(小时)
            pre_start_time = datetime.datetime.strptime(
                pre_start_time, "%Y-%m-%d %H:%M:%S")

            # start_time向后推n2个小时
            end_time = datetime.datetime.strptime(
                start_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=_n2)

            # 交换
            start_time, end_time = pre_start_time, end_time

            length = int((end_time - start_time).total_seconds() / 60 / 60) + 1

            print(start_time, end_time)
            for station in point_rainfall_tmp.keys():
                time_list = [str(x[0].strftime("%Y-%m-%d %H:%M:%S"))
                             for x in point_rainfall_tmp[station]]
                for i in range(length):
                    timex = start_time + datetime.timedelta(minutes=i * 60)
                    timex = str(timex.strftime("%Y-%m-%d %H:%M:%S"))

                    if timex not in time_list:
                        point_rainfall_result[station].append(0)
                    else:
                        index = time_list.index(timex)
                        if point_rainfall_tmp[station][index][1] == None:
                            point_rainfall_result[station].append(0)
                        else:
                            point_rainfall_result[station].append(
                                point_rainfall_tmp[station][index][1])

            self.data["point_rainfall"] = point_rainfall_result

            # 获取前20天的面雨量数据
            pa_start_time =  datetime.datetime.strptime(self.ins.start, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(days=21)
            pa_end_time =  datetime.datetime.strptime(self.ins.start, "%Y-%m-%d %H:%M:%S")

            print(pa_start_time, pa_end_time)

            sql = "select * from {} where WATER_TYPE = '{}' and TM between '{}' and '{}' order by TM".format(
                PPTN_surface, self.ins.type, pa_start_time, pa_end_time)
            
            cursor.execute(sql)

            tmp = cursor.fetchall()

            # 一天的开始为9点，结束为第二天的8点
            surface_pptn_day = []

            day_rainfall = 0

            # 董铺水库大于等于13h无降雨、大房郢水库大于等于10h无降雨，则一场降雨截至。
            end_index = len(tmp) - 1
            for i in range(len(tmp) - 1, 13, -1):
                # 董铺水库， 连续13h无降雨
                if self.ins.type == "0":
                    if sum([x[3] for x in tmp[i-13:i]]) == 0:
                        end_index = i
                        break
                # 大房郢水库，连续10h无降雨
                else:
                    if sum([x[3] for x in tmp[i-10:i]]) == 0:
                        end_index = i
                        break
            
            index = 0
            for row in tmp:
                if index > end_index:
                    break
                if row[1].hour == 9:
                    surface_pptn_day.append(day_rainfall)
                    day_rainfall = 0
                day_rainfall += float(row[3])
                index += 1

            # 将最后一天的降雨量加入
            # if tmp[index - 1][1].hour != 8:
            #     surface_pptn_day.append(day_rainfall)

            # 若不足20个，前面补0
            if len(surface_pptn_day) < 20:
                surface_pptn_day = [0] * (20 - len(surface_pptn_day)) + surface_pptn_day

            self.data["day_rainfall"] = surface_pptn_day

            # 关闭数据库
            cursor.close()
            conn.close()

        except Exception as e:
            print("load data from db error")
            print(e)
            logging.error("load data from db error {}".format(e))

        print("load data from db monitor success")
        logging.info("load data from db monitor success")

    # 从数据库中读取数据

    def load_data_from_db(self):
        if "db" not in self.config.keys():
            print("config db is error")
            logging.error("config db is error")
            exit(1)

        try:
            # 数据库参数
            self.db_config = self.config['db']['values']

            address = self.db_config['address']
            port = self.db_config['port']
            user = self.db_config['user']
            password = self.db_config['password']
            database = self.db_config['database']

            # 入参
            start_time = self.ins.start

            # 向前取n1个小时的数据
            pre_start_time = (datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") -
                              datetime.timedelta(hours=self.ins.n1)).strftime("%Y-%m-%d %H:%M:%S")
            # end_time = self.ins.end
            _batch = self.ins.batch
            _type = self.ins.type
            _n1 = self.ins.n1 + 1
            _n2 = self.ins.n2

            rainfall_table_name = self.config['db']['in']['values']['rainfall']
            evaporation_table_name = self.config['db']['in']['values']['evaporation']
            inflow_table_name = self.config['db']['in']['values']['inflow']
            in_outflow_table_name = self.config['db']['in']['values']['in-outflow']
            # 建立数据库连接
            try:
                conn = mysql.connect(
                    host=address, port=port, user=user, password=password, database=database)
            except BaseException as e:
                print("connect to mysql error: {}".format(e))
                exit(1)

            # 获取游标
            cursor = conn.cursor()

            print(pre_start_time, start_time)

            # 获取inflow和outflow
            sql = "SELECT (INFLOW) FROM {} WHERE TM BETWEEN '{}' and '{}' and BATCH = '{}' and TYPE = '{}' order by TM".format(
                in_outflow_table_name, pre_start_time, start_time, _batch, _type)
            cursor.execute(sql)

            tmp = cursor.fetchall()
            self.data["inflow_x"] = [float(x[0]) for x in tmp]

            sql = "SELECT (OUTFLOW) FROM {} WHERE TM BETWEEN '{}' and '{}' and BATCH = '{}' and TYPE = '{}' order by TM".format(
                in_outflow_table_name, pre_start_time, start_time, _batch, _type)
            cursor.execute(sql)

            tmp = cursor.fetchall()
            self.data["outflow_x"] = [float(x[0]) for x in tmp]

            # 获取z和v
            # sql = "SELECT (Z) FROM {} WHERE TYPE = '{}'".format(
            #     tablename_zv, self.ins.type)
            # cursor.execute(sql)

            # self.z_table = [float(row[0]) for row in cursor.fetchall()]

            # sql = "SELECT (STORAGE) FROM {} WHERE TYPE = '{}'".format(
            #     tablename_zv, self.ins.type)
            # cursor.execute(sql)

            # self.v_table = [float(row[0]) for row in cursor.fetchall()]

            # 获取数据
            # sql = "select (value) from {} where TIME between '{}' and '{}' and BATCH = '{}' and TYPE = '{}'".format(rainfall_table_name, start_time, end_time, _batch, _type)
            # 获取start之前n1个数据
            sql = "select (VALUE) from {} where TIME BETWEEN '{}' and '{}' and BATCH = '{}' and TYPE = '{}' order by TIME desc limit {}".format(
                rainfall_table_name, pre_start_time, start_time, _batch, _type, _n1)
            cursor.execute(sql)

            # 读取数据
            tmp = cursor.fetchall()
            self.data["rainfull"] = [x[0] for x in tmp]
            # 反向
            self.data["rainfull"].reverse()

            # 若长度未到达n1, 则前面补全为n1
            if len(self.data["rainfull"]) < _n1:
                self.data["rainfull"] = [
                    0] * (_n1 - len(self.data["rainfull"])) + self.data["rainfull"]

            # 读取蒸发数据
            # sql = "select (value) from {} where TIME between '{}' and '{}' and BATCH = '{}' and TYPE = '{}'".format(evaporation_table_name, start_time, end_time, _batch, _type)
            # 获取start之前n1个数据
            sql = "select (VALUE) from {} where TIME BETWEEN '{}' and '{}' and BATCH = '{}' and TYPE = '{}' order by TIME desc limit {}".format(
                evaporation_table_name, pre_start_time, start_time, _batch, _type, _n1)
            cursor.execute(sql)

            # 读取数据
            tmp = cursor.fetchall()
            self.data["evaporation"] = [x[0] for x in tmp]

            # 反向
            self.data["evaporation"].reverse()

            # 若长度未到达n1, 则前面补全为n1
            if len(self.data["evaporation"]) < _n1:
                self.data["evaporation"] = [
                    0] * (_n1 - len(self.data["evaporation"])) + self.data["evaporation"]

            # 读取实测数据
            # sql = "select (value) from {} where TIME between '{}' and '{}' and TYPE = '{}'".format(inflow_table_name, start_time, end_time, _type)
            # 获取 start_time 之前n1个数据
            sql = "select * from {} where TIME BETWEEN '{}' and '{}' and TYPE = '{}' and BATCH = '{}' order by TIME limit {}".format(
                inflow_table_name, pre_start_time, start_time, _type, "10000", _n1)
            cursor.execute(sql)

            # 读取数据
            tmp = cursor.fetchall()

            length = int((datetime.datetime.strptime(
                start_time, "%Y-%m-%d %H:%M:%S") - datetime.datetime.strptime(pre_start_time, "%Y-%m-%d %H:%M:%S")).total_seconds() / 60 / 60) + 1

            self.data['inflow'] = []

            # convert datetime.datetime to str
            exist_time = [x[1] for x in tmp]
            exist_time = [str(x.strftime("%Y-%m-%d %H:%M:%S"))
                          for x in exist_time]

            for i in range(length):
                timex = datetime.datetime.strptime(
                    pre_start_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=i)
                timex = str(timex.strftime("%Y-%m-%d %H:%M:%S"))

                if timex not in exist_time:
                    self.data['inflow'].append(-1)
                else:
                    index = exist_time.index(timex)
                    self.data['inflow'].append(float(tmp[index][2]))

            # 实测降雨后补72个-1
            for i in range(72):
                self.data['inflow'].append(-1)

            # 若数据长度没有达到n1+n2+1, 则后面全补全为0
            if len(self.data['rainfull']) < _n1 + _n2:
                self.data['rainfull'] += [0] * \
                    (_n1 + _n2 - len(self.data['rainfull']))
            if len(self.data['evaporation']) < _n1 + _n2:
                self.data['evaporation'] += [0] * \
                    (_n1 + _n2 - len(self.data['evaporation']))
            # if len(self.data['inflow']) < _n1 + _n2 + 1:
            #     self.data['inflow'] += [0] * (_n1 + _n2 - len(self.data['inflow']))

            # 若数据为空则，补全为0
            if len(self.data['inflow_x']) == 0:
                self.data['inflow_x'] = [0]
            if len(self.data['outflow_x']) == 0:
                self.data['outflow_x'] = [0]

            if len(self.data['inflow_x']) < _n1 + _n2:
                self.data['inflow_x'] += [self.data["inflow_x"][-1]
                                          ] * (_n1 + _n2 - len(self.data['inflow_x']))
            if len(self.data['outflow_x']) < _n1 + _n2:
                self.data['outflow_x'] += [self.data["outflow_x"]
                                           [-1]] * (_n1 + _n2 - len(self.data['outflow_x']))

            # 数据长度
            self.length = len(self.data['rainfull'])

            self.data['inflow'] = self.data['inflow'][:len(
                self.data['rainfull'])]

            # 计算从start_time之前47小时的降雨数据之和
            pre_start_time_47 = (datetime.datetime.strptime(
                start_time, "%Y-%m-%d %H:%M:%S") - datetime.timedelta(hours=47)).strftime("%Y-%m-%d %H:%M:%S")
            sql = "select (VALUE) from {} where TIME BETWEEN '{}' and '{}' and BATCH = '{}' and TYPE = '{}' order by TIME limit {}".format(
                rainfall_table_name, pre_start_time_47, start_time, _batch, _type, 48)
            cursor.execute(sql)

            # 读取数据
            tmp = cursor.fetchall()
            sum_of_rainfall = sum([x[0] for x in tmp])

            self.data['sum_of_rainfall'] = sum_of_rainfall

            # 关闭数据库
            cursor.close()
            conn.close()

        except Exception as e:
            print("load data from db error")
            print(e)
            logging.error("load data from db error {}".format(e))

        print("load data from db success")
        logging.info("load data from db success")

    # 给数据库中写入数据

    def write_data_to_db(self):
        if "db" not in self.config.keys():
            print("config db is error")
            logging.error("config db is error for write data to db")
            exit(1)

        try:
            # 数据库参数
            self.db_config = self.config['db']['values']

            address = self.db_config['address']
            port = self.db_config['port']
            user = self.db_config['user']
            password = self.db_config['password']
            database = self.db_config['database']

            # 建立数据库连接
            conn = mysql.connect(host=address, port=port,
                                 user=user, password=password, database=database)

            table_name = self.config['db']['out']['values']

            # 写入数据 保存self.result_xaj中的数据
            cursor = conn.cursor()

            batch = self.ins.batch
            _type = self.ins.type
            _zv_type = str(self.zv_type)

            for i in range(len(self.result_xaj)):
                timex = self.result_xaj[i][0]
                QX = float(self.result_xaj[i][1])
                QP = float(self.result_pr[i][1])
                PR_R = float(self.result_pr_r[i])
                ZX_R = float(self.result_xaj[i][2])
                ZX_A = float(self.result_xaj[i][3])
                ZP_R = float(self.result_pr[i][2])
                ZP_A = float(self.result_pr[i][3])

                # QX QP 保留小点后三位
                QX = round(QX, 4)
                QP = round(QP, 4)
                PR_R = round(PR_R, 3)
                ZX_R = round(ZX_R, 3)
                ZX_A = round(ZX_A, 3)
                ZP_R = round(ZP_R, 3)
                ZP_A = round(ZP_A, 3)

                if QX < 0:
                    QX = 0
                if QP < 0:
                    QP = 0
                if PR_R < 0:
                    PR_R = 0

                # sql = "INSERT INTO {} (TIME, BATCH, QX, QP, PR_R, TYPE) values ('{}', '{}', '{}', {}, {}, {})".format(table_name, timex, batch, QX, QP, PR_R, _type)

                # 更新式插入
                sql = "INSERT INTO {} (TIME, BATCH, QX, QP, PR_R, ZX_R, ZX_A, ZP_R, ZP_A, TYPE, ZV_TYPE) VALUES ('{}', '{}', {}, {}, {}, {}, {}, {}, {}, {}, '{}') ON DUPLICATE KEY UPDATE QX = {}, QP = {}, PR_R = {}, ZX_A = {}, ZP_A = {}".format(
                    table_name, timex, batch, QX, QP, PR_R, ZX_R, ZX_A, ZP_R, ZP_A, _type, _zv_type, QX, QP, PR_R, ZX_A, ZP_A)

                cursor.execute(sql)

            conn.commit()

            cursor.close()
            conn.close()
        except Exception as e:
            print("write data to db error")
            print(e)
            logging.error("write data to db error {}".format(e))
            exit(0)

        print("write data to db success")
        logging.info("write data to db success")

    # 从SQLite中读取数据

    def load_data_from_sqlite(self):
        if "sqlite" not in self.config.keys():
            print("config sqlite is error")
            exit(1)

        # 数据库参数
        self.sqlite_config = self.config['sqlite']['path']['values']

        start_time = self.ins.start
        end_time = self.ins.end
        batch = self.ins.batch
        type = self.ins.type

        rainfall_table_name = self.config['sqlite']['in']['values']['rainfall']
        evaporation_table_name = self.config['sqlite']['in']['values']['evaporation']
        inflow_table_name = self.config['sqlite']['in']['values']['inflow']

        # 连接数据库
        conn = sqlite3.connect(self.sqlite_config)
        cursor = conn.cursor()

        # 读取降雨数据
        sql = "select (value) from {} where TIME between '{}' and '{}' and BATCH = {} and TYPE = '{}'".format(
            rainfall_table_name, start_time, end_time, batch, type)
        cursor.execute(sql)

        # 读取数据
        tmp = cursor.fetchall()
        self.data["rainfull"] = [x[0] for x in tmp]

        # 读取蒸发数据
        sql = "select (value) from {} where TIME between '{}' and '{}' and TYPE = '{}'".format(
            evaporation_table_name, start_time, end_time, type)
        cursor.execute(sql)

        # 读取数据
        tmp = cursor.fetchall()
        self.data["evaporation"] = [x[0] for x in tmp]

        # 读取实测数据
        sql = "select (value) from {} where TIME between '{}' and '{}' and TYPE = '{}'".format(
            inflow_table_name, start_time, end_time, type)
        cursor.execute(sql)

        # 读取数据
        tmp = cursor.fetchall()
        self.data["inflow"] = [x[0] for x in tmp]

        # 数据长度
        self.length = len(self.data['rainfull'])

        # 关闭数据库
        cursor.close()
        conn.close()

    # 给SQLite中写入数据

    def write_data_to_sqlite(self):
        # 将数据保存到sqlite中
        if "sqlite" not in self.config.keys():
            print("config sqlite is error")
            exit(1)

        # 数据库参数
        self.sqlite_config = self.config['sqlite']['path']['values']

        # 连接数据库
        conn = sqlite3.connect(self.sqlite_config)
        cursor = conn.cursor()

        table_name = self.config['sqlite']['out']['values']

        # 获取最大ID
        sql = "select max(ID) from {}".format(table_name)
        cursor.execute(sql)
        tmp = cursor.fetchall()
        max_id = int(tmp[0][0]) if tmp[0][0] else 0

        batch = self.ins.batch

        # 保存self.result_xaj中的数据
        for i in range(len(self.result_xaj)):
            id = max_id + i + 1
            timex = self.result_xaj[i][0]
            QX = float(self.result_xaj[i][1])
            QP = float(self.result_pr[i][1])

            # 规定小数点后第3位
            QX = round(QX, 4)
            QP = round(QP, 4)

            print("id: {}, timex: {}, QX: {}, QP: {}".format(id, timex, QX, QP))
            sql = f"insert into '{table_name}' (ID, BATCH, TIME, QX, QP) values ({id}, '{batch}', '{timex}', {QX}, {QP})"
            # 更新式插入

            cursor.execute(sql)
            # 提交事务
        conn.commit()

        # 关闭数据库
        cursor.close()
        conn.close()

    # PR模型计算时确定暴雨中心, 统计暴雨中心时是统计各个测站预报时刻及之前23个时刻降雨之和.

    def PR_rainstorm_center(self, index):
        # 计算各个测站在index时刻及之前23个时刻降雨之和
        year = self.ins.start.split(" ")[0].split("-")[0]

        # 2018年之前(包括2018)使用v1版本，否则使用v2版本
        version_year = "v1"
        if int(year) <= 2018:
            version_year = "v1"
        else:
            version_year = "v2"
        
        upstream_station = self.config['STCD']['values'][str(self.ins.type)][version_year]['upstream']
        downstream_station = self.config['STCD']['values'][str(self.ins.type)][version_year]['downstream']

        station_rainfall_sum = {}

        for station in upstream_station:
            station_rainfall_sum[station] = sum(
                self.data['point_rainfall'][station][index - 23:index + 1])
        
        for station in downstream_station:
            station_rainfall_sum[station] = sum(
                self.data['point_rainfall'][station][index - 23:index + 1])

        # 暴雨中心为降雨量最大的测站
        rainstorm_center = max(station_rainfall_sum,
                               key=station_rainfall_sum.get)

        # 若在下游测站返回0，否则返回1
        if rainstorm_center in downstream_station:
            return 0
        else:
            return 1

    # PR模型计算

    def PR_calculate(self):
        N = self.length
        E0 = self.data['evaporation']
        P0 = self.data['rainfull']
        # E_sum = np.sum(E0[47:(47+24)])
        # K = 1 - 0.95 * E_sum / 80

        # TODO: 使用新的K值计算
        revisor_type = "dongpu" if self.ins.type == "0" else "dafangying"
        # 开始时刻+47小时，获取这个时刻的月份
        k_month = int((datetime.datetime.strptime(self.ins.start, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(hours=47)).month)
        # 保证k_month在1-12之间
        k_month = min(max(k_month, 1), 12)
        K = self.config["PR"]["values"][revisor_type]["K"][k_month-1]
        print("k_month: ", k_month, "K: ", K)

        kc = self.ins.p_kc
        area = self.ins.p_area
        FR0 = self.ins.p_FR0
        S0 = self.ins.p_S0
        QI0 = self.ins.p_QI0
        QG0 = self.ins.p_QG0
        SM = self.ins.p_SM
        EX = self.ins.p_EX
        KI = self.ins.p_KI
        KG = self.ins.p_KG
        CI = self.ins.p_CI
        CG = self.ins.p_CG
        UH = self.ins.p_UH

        # zeros_juzhen = [0] * 480
        P_set = np.array(self.data["day_rainfall"])[-20:]
        Pa = 0
        doc = []
        R = [0] * N
        
        for day in range(1, len(P_set)):
            Pa = Pa + K ** (day + 1) * P_set[len(P_set) - day - 1]
        
        # 判断Pa是否位nan
        if math.isnan(Pa):
            Pa = 0

        if Pa > 80:
            Pa = 80
        
        SUM_P = 0

        for t in range(N):
            P = float(self.data['rainfull'][t])
            SUM_P += P
            # for hour in range(1, 480):
                # Pa = Pa + K ** hour * P_set[480 - hour + t - 1]
            '''
                董铺上游=0.00059199399469861*（P+Pa）^2.15219493931108
                董铺下游=0.00174276402839024*（P+Pa）^1.92834705920637
                大房郢上游=0.000780474937164339*（P+Pa）^2.04252570988374
                大房郢下游=0.00047030380416462*（P+Pa）^2.20305737583984
            '''
            # 判断暴雨中心
            if self.PR_rainstorm_center(t) == 1:
                if self.ins.type == "0":        # 董铺上游
                    R[t] = 0.00059199399469861 * (SUM_P + Pa) ** 2.15219493931108
                else:                           # 大房郢上游
                    R[t] = 0.000780474937164339 * (SUM_P + Pa) ** 2.04252570988374
            else:
                if self.ins.type == "0":
                    R[t] = 0.000866974103358191 * (SUM_P + Pa) ** 2.17009103644037
                else:
                    R[t] = 0.00047030380416462 * (SUM_P + Pa) ** 2.20305737583984

        
            # 计算净雨量
            if t == 0:
                net_rainfall = 0
            else:
                net_rainfall = R[t] - R[t-1]

            
            E = float(self.data['evaporation'][t])
            EP = kc * E
            PE = max(P - EP, 0)


            # 计算三水源划分
            if PE == 0:
                FR = 0
            else:
                FR = net_rainfall/PE

            RS, RI, RG, S0, FR0 = self.XAJ_three_source(
                FR, net_rainfall, PE, SM, EX, S0, FR0, KI, KG)

            # 更新数据
            doc.append([RS, RI, RG])

        # 汇流计算
        doc = np.array(doc)

        # 计算径流量
        Q = self.XAJ_confluence(doc, area, CI, CG, UH, QI0, QG0)

        # 实时矫正
        Q = self.realtime_correct(Q, "PR")

        # 修正模块
        Q = self.correct_water_module(Q)

        # 计算水位, 单降雨水位；全部水位
        inflowx = [0] * self.length
        outflowx = [0] * self.length

        Z_rainfall = self.cal_water_z(Q, inflowx, outflowx)

        inflow_x = self.data["inflow_x"]
        outflow_x = self.data["outflow_x"]

        Z_all = self.cal_water_z(Q, inflow_x, outflow_x)

        start_time = self.ins.start
        for i in range(len(Q)):
            # 每次加10分钟
            time_index = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                minutes=60*i) - datetime.timedelta(minutes=60*(int(self.ins.n1)))

            time_index = str(time_index.strftime("%Y-%m-%d %H:%M:%S"))
            self.result_pr.append((time_index, Q[i], Z_rainfall[i], Z_all[i]))

        self.result_pr_r = R

        return self.result_pr

    # XAJ模型计算
    def XAJ_calculate(self):

        # XAJ算法参数
        area = self.ins.x_area
        WU = self.ins.x_WU
        WL = self.ins.x_WL
        WD = self.ins.x_WD
        FR0 = self.ins.x_FR0
        S0 = self.ins.x_S0
        QI0 = self.ins.x_QI0
        QG0 = self.ins.x_QG0
        SM = self.ins.x_SM
        EX = self.ins.x_EX
        KI = self.ins.x_KI
        KG = self.ins.x_KG
        CI = self.ins.x_CI
        CG = self.ins.x_CG
        UH = self.ins.x_UH

        doc = []

        print(self.length)

        for i in range(self.length):
            P = self.data['rainfull'][i]
            E = self.data['evaporation'][i]

            # 计算时段蒸发量
            PE, EU, EL, ED = self.XAJ_Ep_calculate(P, E, WU, WL)
            # print("PE: {}, EU: {}, EL: {}, ED: {}".format(PE, EU, EL, ED))

            # 计算蓄满产流
            R, FR = self.XAJ_W0_calculate(PE, WU, WL, WD)
            # print("R: {}, FR: {}".format(R, FR))

            # 计算三水源划分
            RS, RI, RG, S0, FR0 = self.XAJ_three_source(
                FR, R, PE, SM, EX, S0, FR0, KI, KG)
            # print("RS: {}, RI: {}, RG: {}, S0: {}, FR0: {}".format(RS, RI, RG, S0, FR0))

            # 更新土壤含水量
            WU, WL, WD = self.XAJ_update_soil_water(
                P, EU, EL, ED, R, WU, WL, WD)

            # print("WU: {}, WL: {}, WD: {}".format(WU, WL, WD))

            # 更新数据
            doc.append([RS, RI, RG])

        # doc to numpy
        doc = np.array(doc)

        # 计算径流量
        Q = self.XAJ_confluence(doc, area, CI, CG, UH, QI0, QG0)

        # 实时矫正
        Q = self.realtime_correct(Q, "XAJ")

        # 修正模块
        Q = self.correct_water_module(Q)

        # 计算水位, 单降雨水位；全部水位
        inflowx = [0] * self.length
        outflowx = [0] * self.length

        Z_rainfall = self.cal_water_z(Q, inflowx, outflowx)

        inflow_x = self.data["inflow_x"]
        outflow_x = self.data["outflow_x"]

        Z_all = self.cal_water_z(Q, inflow_x, outflow_x)

        start_time = self.ins.start
        for i in range(len(Q)):
            # 每次加60分钟, start_time是一个字符串
            time_index = datetime.datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(
                minutes=60*i) - datetime.timedelta(minutes=60*(int(self.ins.n1)))
            time_index = str(time_index.strftime("%Y-%m-%d %H:%M:%S"))
            self.result_xaj.append((time_index, Q[i], Z_rainfall[i], Z_all[i]))

        # print(self.result_xaj)
        return self.result_xaj

    # XAJ时段蒸发量计算
    def XAJ_Ep_calculate(self, P, E, WU, WL, EU=0):
        EP = self.ins.x_kc * E

        if WU + P >= EP:
            EU = EP
            EL = 0
            ED = 0
        elif WU + P < EP and WL >= self.ins.x_c * self.ins.x_WLM:
            EU = WU + P
            EL = (EP - EU) * WL / self.ins.x_WLM
            ED = 0
        elif WU + P < EP and self.ins.x_c * (EP - EU) <= WL < self.ins.x_c * self.ins.x_WLM:
            EU = WU + P
            EL = self.ins.x_c * (EP - EU)
            ED = 0
        else:
            EU = WU + P
            EL = WL
            ED = self.ins.x_c * (EP - EU) - EL
        PE = max(P - EU - EL - ED, 0)

        return PE, EU, EL, ED

    # XAJ蓄满产流计算
    def XAJ_W0_calculate(self, PE, WU, WL, WD):
        WMM = self.ins.x_WM * (self.ins.x_B + 1)
        W0 = WU + WL + WD
        A = WMM * (1 - (1 - W0/self.ins.x_WM)**(1/(1+self.ins.x_B)))
        if PE + A < WMM:
            R = PE - self.ins.x_WM * \
                ((1 - A/WMM)**(1+self.ins.x_B) - (1 - (PE+A)/WMM)**(1+self.ins.x_B))
        else:
            R = PE - (self.ins.x_WM - W0)
        if PE == 0:
            FR = 0
        else:
            FR = R / PE

        return R, FR

    # XAJ三水源划分

    def XAJ_three_source(self, FR, R, PE, SM, EX, S0, FR0, KI, KG):
        MS = SM * (EX + 1)
        if FR == 0:
            AU = 0
            RS = 0
            S = 0
            RI = 0
            RG = 0
        else:
            AU = MS * (1 - (1 - (S0*FR0/FR) / SM)**(1/(1+EX)))
            # 若为复数，只取实数部分
            if AU.imag != 0:
                AU = AU.real
            if PE + AU < MS:
                RS = FR * (PE + S0*FR0/FR - SM + SM*(1 - (PE+AU)/MS)**(EX+1))
            else:
                RS = FR * (PE + S0*FR0/FR - SM)
            S = S0*FR0/FR + (R - RS)/FR
            RI = KI * S * FR
            RG = KG * S * FR

        S0 = S * (1 - KI * KG)
        FR0 = FR

        return RS, RI, RG, S0, FR0

    # 更新土壤含水量
    def XAJ_update_soil_water(self, P, EU, EL, ED, R, WU, WL, WD):

        WU = WU + P - EU - R
        WL = WL - EL
        WD = WD - ED

        if WU > self.ins.x_WUM:
            WU = self.ins.x_WUM
            WL = WL + WU - self.ins.x_WUM

        if WL > self.ins.x_WLM:
            WL = self.ins.x_WLM
            WD = WD + WL - self.ins.x_WLM

        if WD > self.ins.x_WM - self.ins.x_WUM - self.ins.x_WLM:
            WD = self.ins.x_WM - self.ins.x_WUM - self.ins.x_WLM

        return WU, WL, WD

    # XAJ汇流计算
    def XAJ_confluence(self, doc, area, CI, CG, UH, QI0, QG0):
        N = self.length
        tmp_doc = np.zeros((N, 9))
        tmp_doc[:, 0] = doc[:, 0]
        tmp_doc[:, 1] = doc[:, 1]
        tmp_doc[:, 2] = doc[:, 2]
        doc = tmp_doc

        # 汇流
        temp = np.convolve(doc[:, 0], UH)
        doc[:, 3] = temp[:N]

        for t in range(N):
            QI = (1 - CI) * doc[t, 1] * area / 3.6 + CI * QI0
            QG = (1 - CG) * doc[t, 2] * area / 3.6 + CG * QG0
            doc[t, 4] = QI
            doc[t, 5] = QG
            QI0 = QI
            QG0 = QG
        doc[:, 6] = doc[:, 3] + doc[:, 4] + doc[:, 5]  # 总流量

        return doc[:, 6]

    # 洪峰位置平滑校正（前五个时刻+洪峰时刻+后五个时刻）
    def peak_smooth_correct(self, Q):
        # 洪峰时刻
        peak_time = 0
        max_Q = max(Q)
        for i in range(len(Q)):
            if Q[i] == max_Q:
                peak_time = i
                break

        '''
            大房郢的洪峰左右各调整1个时段数据。
            董铺的小洪水洪峰左右各调整2个时段；大洪水各调整3个时段。
        '''
        if self.type == 0:
            smoth_start = max(0, peak_time - 3)
            smoth_end = min(len(Q), peak_time + 3)
        else:
            if self.flood_flag == 1:
                smoth_start = max(0, peak_time - 4)
                smoth_end = min(len(Q), peak_time + 4)
            elif self.flood_flag == 2:
                smoth_start = max(0, peak_time - 5)
                smoth_end = min(len(Q), peak_time + 5)

        old_q = Q.copy()
        # 平滑校正, peak_time时刻的流量值不变， 五点加法平滑
        for i in range(smoth_start + 2, smoth_end - 2):
            if i == peak_time:
                continue
            Q[i] = (old_q[i-2] + old_q[i-1] + old_q[i] +
                    old_q[i+1] + old_q[i+2]) / 5

        return Q

    # 实时校正修正值
    def realtime_correct_plus_value(self, Q, model, correct_type):
        if self.type == "0":
            major_support = self.config[model]["values"]["support"]["dongpu"][correct_type]["major"]
            minor_support = self.config[model]["values"]["support"]["dongpu"][correct_type]["minor"]

            # 大洪水修正后预报的流量中给最大的流量（即洪峰）值+x1，相应小洪水洪峰+x2，然后再作为最终修正后的流量输出
            if self.flood_flag == 2:
                max_Q = max(Q)
                for i in range(len(Q)):
                    if Q[i] == max_Q:
                        Q[i] += major_support
            elif self.flood_flag == 1:
                max_Q = max(Q)
                for i in range(len(Q)):
                    if Q[i] == max_Q:
                        Q[i] += minor_support
        else:
            support = self.config[model]["values"]["support"]["dafangying"][correct_type]["major"]
            if self.flood_flag == 1:
                max_Q = max(Q)
                for i in range(len(Q)):
                    if Q[i] == max_Q:
                        Q[i] = Q[i] + support

        # smoth
        if self.flood_flag != 0:
            Q = self.peak_smooth_correct(Q)

        return Q

    # 实时矫正模块
    def realtime_correct(self, Q, model="XAJ"):
        # 如果无实测流量，则不进行实时矫正
        if len(Q) < 5 or "inflow" not in self.data.keys() or self.data["inflow"] == None or len(self.data["inflow"]) < 5:
            # 大洪水未修正预报的流量中给最大的流量（即洪峰）值（+220），相应小洪水洪峰（+88），然后再作为最终未修正后的流量输出。
            if sum(self.data["rainfull"][:47]) != 0:
                Q = self.realtime_correct_plus_value(Q, model, "uncorrect")
            return Q

        # 若有连续的5个时刻的实测流量，则进行实时矫正
        cnt = 0
        now_index = 0
        real_Q = self.data["inflow"]
        correct_Q = Q

        while now_index < len(self.data["inflow"]):

            if self.data["inflow"][now_index] != -1:
                cnt += 1

            # 只需要连续的四个不为-1的值
            if self.data["inflow"][now_index] == -1 and cnt < 4:
                cnt = 0

            if cnt >= 4:
                # 计算前5个时段的矫正流量
                correct_Q[now_index - 4] = Q[now_index - 4]
                correct_Q[now_index - 3] = Q[now_index - 3] + \
                    (real_Q[now_index - 4] - correct_Q[now_index - 4])
                # 参数为1 0.6 0.4
                correct_Q[now_index - 2] = Q[now_index - 2] + 0.6 * \
                    (real_Q[now_index - 3] - correct_Q[now_index - 3]) + \
                    0.4 * (real_Q[now_index - 4] - correct_Q[now_index - 4])
                # 参数为1 0.5 0.3 0.2
                correct_Q[now_index - 1] = Q[now_index - 1] + 0.5 * (real_Q[now_index - 2] - correct_Q[now_index - 2]) + 0.3 * (
                    real_Q[now_index - 3] - correct_Q[now_index - 3]) + 0.2 * (real_Q[now_index + 0] - correct_Q[now_index + 0])
                # 参数为1 0.4 0.3 0.2 0.1
                correct_Q[now_index] = Q[now_index] + 0.4 * (real_Q[now_index - 1] - correct_Q[now_index - 1]) + 0.3 * (
                    real_Q[now_index - 2] - correct_Q[now_index - 2]) + 0.2 * (real_Q[now_index - 3] - correct_Q[now_index - 3]) + 0.1 * (real_Q[now_index - 4] - correct_Q[now_index - 4])

                # 计算后续时段的矫正流量，参数为1 0.2 0.3 0.3 0.1 0.1
                visit_i = 0
                for i in range(now_index + 1, len(real_Q)):
                    visit_i = i
                    if real_Q[i] == -1:
                        now_index = i - 1
                        cnt = 0
                        break
                    correct_Q[i] = Q[i] + 0.2 * (real_Q[i-1] - correct_Q[i-1]) + 0.3 * (real_Q[i-2] - correct_Q[i-2]) + 0.3 * (
                        real_Q[i-3] - correct_Q[i-3]) + 0.1 * (real_Q[i-4] - correct_Q[i-4]) + 0.1 * (real_Q[i-5] - correct_Q[i-5])

                if visit_i == len(real_Q) - 1:
                    now_index = i
                    cnt = 0

            now_index += 1

        if sum(self.data["rainfull"][:47]) != 0:
            correct_Q = self.realtime_correct_plus_value(
                correct_Q, model, "correct")

        return correct_Q

        # 流量过程修正模块
    def correct_water_module(self, Q):
        '''
            修正规则：
            实时预报部分: 实时预报start-48降雨之和小于5,则各时刻流量值为预报值减去14.48,减后数值若小于0则赋值为0。
            若:
                预报时刻及之前48小时降雨之和小于5,则流量过程修正方法:
                各时刻流量值为预报值减去14.48,减后数值若小于0则赋值为0。
            反之：
                1. 预报流量过程中某一时刻若大于等于30，不修正，若小于30，且下时刻降雨为0，则下时刻流量值为上时刻预报流量*0.97）
                2. 预报流量某一时刻流量若小于17，且下时刻降雨为0，则下时刻流量值为上时刻预报流量*0.92，
                3. 流量若小于6,且下时刻降雨为0,则下时刻流量为上时刻预报流量*0.85;
                4. 若小于2,且下时刻降雨为0,则下时刻流量值为上时刻预报流量*0.8;
                5. 小于0.6,且下时刻降雨为0,则下时刻流量值为0。
        '''

        # 针对连续48小时不降雨流量不为0的修正
        sum_of_rainfall = sum(self.data["rainfull"][0:(self.ins.n1 + 1)])

        if sum_of_rainfall < 5:
            for i in range(len(Q)):
                Q[i] = Q[i] - 15     # modify 14.49 to 15
                if Q[i] < 0:
                    Q[i] = 0

        # 针对全部预报流量进行修正
        for i in range(0, len(Q) - 1):
            if self.data["rainfull"][i + 1] < 0.0001:
                if Q[i] < 0.6:
                    Q[i + 1] = 0
                elif Q[i] < 2:
                    Q[i + 1] = Q[i] * 0.8
                elif Q[i] < 6:
                    Q[i + 1] = Q[i] * 0.85
                elif Q[i] < 17:
                    Q[i + 1] = Q[i] * 0.92
                elif Q[i] < 30:
                    Q[i + 1] = Q[i] * 0.97      # Add new rule
                else:
                    continue

        return Q

    # 计算水位模块
    def cal_water_z(self, Q, inflow, outflow):
        start_z = self.ins.z
        length = len(Q)
        Q = list(Q)

        # 计算实际入库流量， 单位立方米每秒
        real_Q = [0] * length
        for i in range(self.ins.n1 + 1, length):
            real_Q[i] = Q[i] + float(inflow[i]) - float(outflow[i])

        # 计算当前时段入库水量，时间间隔为1小时
        water_in = [0] * length

        for i in range(self.ins.n1 + 1, length):
            if i == 0:
                water_in[i] = real_Q[i] * 3600 / 10000
            else:
                water_in[i] = water_in[i-1] + real_Q[i] * 3600 / 10000

        # 根据起调水位获取当前库容, 从水位-库容表中查询
        # 不同水位下的库容插值，vi=v_inter(z[i])，一维线性插值
        v_inter = interpolate.interp1d(
            self.z_table, self.v_table, kind='linear')
        # 不同库容下的水位插值，zi=z_inter(v[i])，一维线性插值
        z_inter = interpolate.interp1d(
            self.v_table, self.z_table, kind='linear')

        start_v = v_inter(start_z)

        # 计算当前时段库容, 库容单位为万立方米
        water_v = [0] * length
        for i in range(self.ins.n1 + 1, length):
            water_v[i] = start_v + water_in[i]

        # 计算各时段水位
        water_z = [-999] * length
        water_z[self.ins.n1] = start_z
        for i in range(self.ins.n1 + 1, length):
            # 最大水位范围
            if water_v[i] > max(self.v_table):
                water_z[i] = max(self.z_table)
            else:
                water_z[i] = float(z_inter(water_v[i]))

        return water_z


if __name__ == "__main__":
    # Add the arguments from the command line
    pass