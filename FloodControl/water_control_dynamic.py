# 防洪调度
import os
import argparse
import json
import sqlite3
import math
from scipy import interpolate
import pymysql as mysql
import datetime
import logging
import sys

log_file_path = 'water_control_dynamic.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class FloodControlStatic:
    def __init__(self, args) -> None:
        self.args = args
        self.config = None
        self.z_table = []     # 水位表
        self.v_table = []     # 库容表

        self.flood_value = []  # 洪水过程
        self.flood_time = []  # 洪水过程时间

        self.supply_value = []  # 供水过程

        self.limit_capacity_dataset = None  # 汛限库容
        self.limit_level_dataset = None     # 汛限水位

        self.result = {}

        self.load_config()

    def update(self, args):
        self.args = args
    

    def load_config(self):
        # 从配置文件中读取配置
        with open(self.args.config, 'r', encoding="utf-8") as f:
            self.config = json.load(f)

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

            tablename_zv = self.config['db']['in']['values']['zv']
            tablename_version_zv = self.config['db']['in']['values']['zv_version']

            tablename_info = self.config['db']['in']['values']['reservoir_info']

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
                tablename_zv, self.args.type, self.zv_type)
            cursor.execute(sql)

            self.z_table = [float(row[0]) for row in cursor.fetchall()]

            sql = "SELECT (W) FROM {} WHERE WATER_TYPE = '{}' and ZV_TYPE = '{}'".format(
                tablename_zv, self.args.type, self.zv_type)
            cursor.execute(sql)

            self.v_table = [float(row[0]) for row in cursor.fetchall()]

            # 将库容从立方米转换为万立方米
            self.v_table = [i/10000 for i in self.v_table]

            # 获取汛线水位
            sql = "SELECT (LIMIT_LEVEL) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, self.args.type)

            cursor.execute(sql)

            self.limit_level_dataset = float(cursor.fetchall()[0][0])

            # 获取汛线库容
            sql = "SELECT (LIMIT_CAPACITY) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, self.args.type)
            
            cursor.execute(sql)

            self.limit_capacity_dataset = float(cursor.fetchall()[0][0])/10000

            # 关闭数据库
            cursor.close()
            conn.close()

        except Exception as e:
            print("load data from db error")
            logging.error("load data from db error")
            print(e)

        print("load data from db monitor success")
        logging.info("load data from db monitor success")

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
        conn = mysql.connect(host=address, port=port,
                             user=user, password=password, database=database)
        cursor = conn.cursor()

        # 读取设计表的对于洪水过程
        tablename_flood = self.config['db']['in']['values']['rdfh']

        # 读取供水表
        tablename_supply = self.config['db']['in']['values']['supply']

        # 读取实时流量过程
        tablename_realtime = self.config['db']['in']['values']['realtime']

        # 获取实时流量过程
        _type = self.args.type
        start = self.args.start
        end = self.args.end
        batch = self.args.batch
        flow = self.args.flow

        sql = "SELECT ({}) FROM {} WHERE TYPE = '{}' AND BATCH = '{}' AND TIME BETWEEN '{}' AND '{}'".format(
            flow, tablename_realtime, _type, batch, start, end)
        cursor.execute(sql)

        tmp = cursor.fetchall()
        realtime_flood_value = [float(row[0]) for row in tmp]

        # 获取max_peak
        if len(realtime_flood_value) == 0:
            print("realtime flood value is empty")
            logging.error("realtime flood value is empty")
            exit(1)

        max_peak = max(realtime_flood_value)
        self.args.scene = get_type(max_peak, self.args.type)

        # 获取典型洪水过程, 获取列名为self.args.scene的列
        sql = "SELECT * FROM {} WHERE TYPE = '{}' AND SCENE = '{}'".format(
            tablename_flood, self.args.type, self.args.scene)
        cursor.execute(sql)

        self.flood_value = []
        self.flood_time = []

        for row in cursor.fetchall():
            self.flood_value.append(float(row[3]))
            self.flood_time.append(float(row[2]))

        # 获取供水过程
        sql = "SELECT Q FROM {} WHERE BATCH = '{}' AND TYPE = '{}'".format(
            tablename_supply, batch, _type)
        cursor.execute(sql)
        supply_value = [float(row[0]) for row in cursor.fetchall()]

        # 将flood_value的值替换为实时流量过程的值
        if len(realtime_flood_value) < len(self.flood_value):
            for i in range(len(realtime_flood_value)):
                self.flood_value[i] = realtime_flood_value[i]
        else:
            self.flood_value = realtime_flood_value
            self.flood_time = [int(i)
                               for i in range(1, len(self.flood_value) + 1)]

        # 若供水过程的长度小于洪水过程的长度,则补充供水过程,后面补充为0
        if len(supply_value) < len(self.flood_value):
            for i in range(len(supply_value), len(self.flood_value)):
                supply_value.append(0)

        self.z_table = list(self.z_table)
        self.v_table = list(self.v_table)
        self.flood_value = list(self.flood_value)
        self.flood_time = list(self.flood_time)
        self.supply_value = list(supply_value)

        # print("flood_value: ", self.flood_value)
        # print("flood_time: ", self.flood_time)
        # print("supply_value: ", self.supply_value)

        cursor.close()
        conn.close()

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
        conn = mysql.connect(host=address, port=port,
                             user=user, password=password, database=database)
        cursor = conn.cursor()

        table_name = self.config['db']['out']['values']['dynamic']

        batch = self.args.batch
        scene = self.args.scene
        flag = self.args.flag
        _type = self.args.type
        stcd = self.args.stcd
        unitname = self.args.unitname
        fymdh = self.args.fymdh
        iymdh = self.args.iymdh
        start = self.args.start
        uuid = self.args.uuid
        _zv_type = str(self.zv_type)

        # flow
        flow = self.args.flow

        # 插入数据
        for i in range(len(self.result['z'])):
            zi = self.result['z'][i]
            vi = self.result['v'][i]
            qi1 = self.result['q1'][i]
            qi2 = self.result['q2'][i]

            # 保留三位小数
            zi = round(zi, 3)
            vi = round(vi, 3)
            qi1 = round(qi1, 4)
            qi2 = round(qi2, 4)

            timex = datetime.datetime.strptime(
                start, "%Y-%m-%d %H:%M:%S") + datetime.timedelta(minutes=60*i)

            # out water building
            building_i = str(self.result['out_water_building'][i])

            # sql = "INSERT INTO {} VALUES ({}, {}, {}, {})".format(table_name, zi, vi, qi)
            sql = "INSERT INTO {} (STCD, UNITNAME, PLCD, FYMDH, IYMDH, YMDH, Z, W, OTQ1, OTQ2, TYPE, FLAG, SCENE, UUID, BUILDING, ZV_TYPE, FLOW) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', '{}') ON DUPLICATE KEY UPDATE Z = {}, W = {}, OTQ1 = {}, OTQ2 = {}, FLAG={}, SCENE='{}'".format(
                table_name, stcd, unitname, batch, fymdh, iymdh, timex, zi, vi, qi1, qi2, _type, 0, scene, uuid, building_i, _zv_type, flow, zi, vi, qi1, qi2, 0, scene)
            # print(sql)
            cursor.execute(sql)

            if flag == 1:
                qi1_new = self.result['q1_new'][i]
                qi2_new = self.result['q2_new'][i]

                # 保留三位小数
                qi1_new = round(qi1_new, 3)
                qi2_new = round(qi2_new, 3)

                sql = "INSERT INTO {} (STCD, UNITNAME, PLCD, FYMDH, IYMDH, YMDH, Z, W, OTQ1, OTQ2, TYPE, FLAG, SCENE, UUID, BUILDING, ZV_TYPE, FLOW) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}', '{}', '{}', '{}') ON DUPLICATE KEY UPDATE Z = {}, W = {}, OTQ1 = {}, OTQ2 = {}, FLAG={}, SCENE='{}'".format(
                    table_name, stcd, unitname, batch, fymdh, iymdh, timex, zi, vi, qi1, qi2, _type, 1, scene, uuid, building_i, _zv_type, flow, zi, vi, qi1, qi2, 1, scene)

                # sql = "INSERT INTO {} (STCD, UNITNAME, PLCD, FYMDH, IYMDH, YMDH, Z, W, OTQ1, OTQ2, TYPE, FLAG, SCENE, UUID) VALUES ('{}', '{}', '{}', '{}', '{}', '{}', {}, {}, {}, {}, {}, {}, '{}', '{}') ON DUPLICATE KEY UPDATE Z = {}, W = {}, OTQ1 = {}, OTQ2 = {}, FLAG={}, SCENE='{}'".format(table_name, stcd, unitname, batch, fymdh, iymdh, timex, zi, vi, qi1_new, qi2_new, _type, 1, scene, uuid, zi, vi, qi1_new, qi2_new, 1, scene)

                cursor.execute(sql)

        conn.commit()
        cursor.close()
        conn.close()

    def read_data_from_sqlite(self):
        # 读取数据
        conn = sqlite3.connect(self.config['sqlite']['path']['values'])
        cursor = conn.cursor()

        # 读取水位和库容表
        tablename_zv = self.config['sqlite']['in']['values']['zv']

        # 读取设计表的对于洪水过程
        tablename_flood = self.config['sqlite']['in']['values']['rdfh']

        # 实时洪水表

        # 获取z和v
        sql = "SELECT (Z) FROM {} WHERE TYPE = '{}'".format(
            tablename_zv, self.args.type)
        cursor.execute(sql)

        self.z_table = [float(row[0]) for row in cursor.fetchall()]

        sql = "SELECT (STORAGE_B) FROM {} WHERE TYPE = '{}'".format(
            tablename_zv, self.args.type)
        cursor.execute(sql)

        self.v_table = [float(row[0]) for row in cursor.fetchall()]

        # 获取典型洪水过程, 获取列名为self.args.scene的列
        sql = "SELECT * FROM {} WHERE TYPE = '{}' AND SCENE = '{}'".format(
            tablename_flood, self.args.type, self.args.scene)
        cursor.execute(sql)

        self.flood_value = []
        self.flood_time = []

        for row in cursor.fetchall():
            self.flood_value.append(float(row[3]))
            self.flood_time.append(float(row[2]))

        self.z_table = list(self.z_table)
        self.v_table = list(self.v_table)
        self.flood_value = list(self.flood_value)
        self.flood_time = list(self.flood_time)

        cursor.close()
        conn.close()

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

        table_name = self.config['sqlite']['out']['values']['dynamic']

        # 获取最大ID
        sql = "select max(ID) from {}".format(table_name)
        cursor.execute(sql)
        tmp = cursor.fetchall()
        max_id = int(tmp[0][0]) if tmp[0][0] else 0

        # 插入数据
        for i in range(len(self.result['z'])):
            max_id += 1
            zi = self.result['z'][i]
            vi = self.result['v'][i]
            qi = self.result['q'][i]
            sql = "INSERT INTO {} VALUES ({}, {}, {}, {})".format(
                table_name, max_id, zi, vi, qi)
            cursor.execute(sql)

        conn.commit()
        cursor.close()
        conn.close()

    # 小洪水dongpu
    def cal_flood_dongpu_minor(self, z11, v11, Q, tguo):
        v_inter = interpolate.interp1d(z11, v11, kind='linear', fill_value="extrapolate")  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear', fill_value="extrapolate")
        z = self.z
        v = [float(v_inter(z[i])) for i in range(len(z))]  # 根据水位插值库容
        q = [float(self.minor_q[0])] * len(Q)
        max_out_user = self.args.max_out_users

        for i in range(len(Q) - 1):
            v_next = ((Q[i] + Q[i + 1]) / 2 * 1 * 3600 - (q[i] +
                      q[i + 1]) / 2 * 1 * 3600 + v[i] * 100000000) / 100000000
            z_next = float(z_inter(v_next))

            # 检查是否超过用户输入的max值
            if q[i] >= max_out_user:
                q[i] = max_out_user

                v_next = ((Q[i] + Q[i + 1]) / 2 * 1 * 3600 - (q[i] +
                                                              q[i + 1]) / 2 * 1 * 3600 + v[i] * 100000000) / 100000000
                z_next = float(z_inter(v_next))

            v.append(v_next)
            z.append(z_next)

        z_limmit = self.args.water_level_for_minor_flood
        if z_next >= z_limmit:
            q[i:] = Q[i:]  # 当水位大于等于27.9米时,从该时刻开始,每个小时的泄量等于对应每个小时的入库流量

        return z, v, q

    # 小洪水dafangying
    def cal_flood_dafangying_minor(self, z11, v11, Q, tguo):
        v_inter = interpolate.interp1d(z11, v11, kind='linear', fill_value="extrapolate")  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear', fill_value="extrapolate")
        z = self.z
        v = [float(v_inter(z[i])) for i in range(len(z))]  # 根据水位插值库容
        q = [float(self.minor_q[0])] * len(Q)
        max_out_user = self.args.max_out_users

        for i in range(len(Q) - 1):
            v_next = ((Q[i] + Q[i + 1]) / 2 * 1 * 3600 - (q[i] +
                      q[i + 1]) / 2 * 1 * 3600 + v[i] * 100000000) / 100000000
            z_next = float(z_inter(v_next))

            # 检查是否超过用户输入的max值
            if q[i] >= max_out_user:
                q[i] = max_out_user

                v_next = ((Q[i] + Q[i + 1]) / 2 * 1 * 3600 - (q[i] +
                                                              q[i + 1]) / 2 * 1 * 3600 + v[i] * 100000000) / 100000000
                z_next = float(z_inter(v_next))

            v.append(v_next)
            z.append(z_next)

        z_limmit = self.args.water_level_for_minor_flood
        if z_next >= z_limmit:
            q[i:] = Q[i:]  # 当水位大于等于27.4米时,从该时刻开始,每个小时的泄量等于对应每个小时的入库流量

        return z, v, q

    # 正常场次洪水dongpu
    def cal_flood_dongpu(self, z11, v11, Q, tguo):
        v_inter = interpolate.interp1d(z11, v11, kind='linear', fill_value="extrapolate")  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear', fill_value="extrapolate")

        # q-v 曲线
        z_qz = self.config['setting']['QZ_table']["values"]["dongpu"]["Z"]
        q_qz = self.config['setting']['QZ_table']["values"]["dongpu"]["Q"]

        # 不同水位下的泄量插值,qi=q_inter(z[i]),一维线性插值
        q_inter_z = interpolate.interp1d(z_qz, q_qz, kind='linear', fill_value="extrapolate")
        # 不同泄量下的水位插值,zi=z_inter(q[i]),一维线性插值
        z_inter_q = interpolate.interp1d(q_qz, z_qz, kind='linear', fill_value="extrapolate")

        z = self.z
        v = [float(v_inter(z[i])) for i in range(len(z))]  # 根据水位插值库容
        q = [float(q_inter_z(z[i])) for i in range(len(z))]  # 根据水位插值泄量

        z_output = [float(self.args.z),]

        a = self.z[0]
        b = float(v_inter(a))
        c = float(q_inter_z(a))

        max_out_user = self.args.max_out_users

        t = []
        # 把时间间隔算出来
        for j in range(len(tguo) - 1):
            t.append(tguo[j + 1] - tguo[j])

        # 根据洪水历时,计算过程
        for i in range(len(Q) - 1):
            # 初始化一个q2,这个是试算的起点值q2很重要,不能初始为上一个值q1,因为q2不是一定大于q1的,过了峰值后就下降了
            q.append(31)
            z.append(a)
            v.append(b)  # 初始化一个库容值
            

            # 检查是否超过用户输入的max值
            if q[i] >= max_out_user:
                q[i] = max_out_user
                v[i] = ((Q[i - 1] + Q[i]) * t[i - 1] * 3600 / 2 - (q[i - 1] + q[i]) * t[i - 1] * 3600 / 2 + v[i - 1] * 100000000) / 100000000  # 通过水量平衡算出v2
                z[i] = float(z_inter(v[i]))
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i + 1] = float(v_inter(z[i + 1]))
            else:
                z[i] = float(z_inter_q(q[i]))
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i] = float(v_inter(z[i]))
                v[i + 1] = float(v_inter(z[i + 1]))

            # （Q1 + Q2）*t / 2 -（q1 + q2）*t / 2 = v2 - v1
            while (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] - q[i + 1] > 0.01 and \
                    (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] >= q[i + 1]:  # 这里的条件判断非常重要,影响了整个取值和精度
                q[i + 1] += 0.01  # 每次q2加0.01,也说明了取值精度
                # z[i + 1] = 2.4411 * math.log(q[i + 1]) + 16.996
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i + 1] = float(v_inter(z[i + 1]))

            z_output.append(a)
            z_output[i + 1] = float(z_inter(v[i + 1]))

        return z_output, v, q

    # 正常场次洪水dafangying
    def cal_flood_dafangying(self, z11, v11, Q, tguo):
        # print(z11, v11, Q, tguo)
        v_inter = interpolate.interp1d(z11, v11, kind='linear', fill_value="extrapolate")  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear', fill_value="extrapolate")

        # q-v 曲线
        z_qz = self.config['setting']['QZ_table']["values"]["dafangying"]["Z"]
        q_qz = self.config['setting']['QZ_table']["values"]["dafangying"]["Q"]

        # 不同水位下的泄量插值,qi=q_inter(z[i]),一维线性插值
        q_inter_z = interpolate.interp1d(z_qz, q_qz, kind='linear', fill_value="extrapolate")
        # 不同泄量下的水位插值,zi=z_inter(q[i]),一维线性插值
        z_inter_q = interpolate.interp1d(q_qz, z_qz, kind='linear', fill_value="extrapolate")

        z = self.z
        v = [float(v_inter(z[i])) for i in range(len(z))]  # 根据水位插值库容
        q = [float(q_inter_z(z[i])) for i in range(len(z))]  # 根据水位插值泄量

        a = self.z[0]
        b = float(v_inter(a))
        c = float(q_inter_z(a))

        z_output = [float(self.args.z),]

        max_out_user = self.args.max_out_users

        print(z, v, q)

        t = []
        # 把时间间隔算出来
        for j in range(len(tguo) - 1):
            t.append(tguo[j + 1] - tguo[j])

       # 根据洪水历时,计算过程
        for i in range(len(Q) - 1):
            # 初始化一个q2,这个是试算的起点值q2很重要,不能初始为上一个值q1,因为q2不是一定大于q1的,过了峰值后就下降了
            q.append(31)

            z.append(a)  # 初始化一个水位z值
            v.append(b)  # 初始化一个库容值

            # 检查是否超过用户输入的max值
            if q[i] >= max_out_user:
                q[i] = max_out_user
                v[i] = ((Q[i - 1] + Q[i]) * t[i - 1] * 3600 / 2 - (q[i - 1] + q[i]) * t[i - 1] * 3600 / 2 + v[i - 1] * 100000000) / 100000000  # 通过水量平衡算出v2
                z[i] = float(z_inter(v[i]))
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i + 1] = float(v_inter(z[i + 1]))
            else:
                z[i] = float(z_inter_q(q[i]))
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i] = float(v_inter(z[i]))  # 通过水位z 插值得到库容v,把np.array转换为float类型值
                v[i + 1] = float(v_inter(z[i + 1]))

            while (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] - q[i + 1] > 0.01 and \
                    (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] >= q[i + 1]:  # 这里的条件判断非常重要,影响了整个取值和精度,注意和计算值有关
                q[i + 1] += 0.01  # 每次q2加0.01,也说明了取值精度
                # z[i + 1] = 2.6934 * math.log(q[i + 1]) + 13.658
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i + 1] = float(v_inter(z[i + 1]))

            z_output.append(a)
            z_output[i + 1] = float(z_inter(v[i + 1]))

        return z_output, v, q

    def get_zvq(self, q_now, z_now, Q_now, Q_next, z11, v11):
        v_inter = interpolate.interp1d(z11, v11, kind='linear', fill_value="extrapolate")  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear', fill_value="extrapolate")

        q_now = q_now
        q_next = 10

        z_next = 28
        z_next = 2.4411 * math.log(q_now) + 16.996
        v_next = 0.77
        v_now = float(v_inter(z_now))
        v_next = float(v_inter(z_next))

        # （Q1 + Q2）*t / 2 -（q1 + q2）*t / 2 = v2 - v1
        while (Q_now + Q_next) + 2 * 100000000 * (v_now - v_next) / 3600 - q_now - q_next > 0.01 and \
                (Q_now + Q_next) + 2 * 100000000 * (v_now - v_next) / 3600 - q_now >= q_next:  # 这里的条件判断非常重要,影响了整个取值和精度
            q_next += 0.01  # 每次q2加0.01,也说明了取值精度
            z_next = 2.4411 * math.log(q_next) + 16.996
            v_next = float(v_inter(z_next))

        return z_next, v_next, q_next

    # 基础防洪调度，只使用流量
    def base_distribution_dp(self, qi):
        out_water_building_item = ""

        if qi <= 43:
            out_water_building_item = "泄洪涵洞泄洪"
        elif qi <= 130:
            out_water_building_item = "泄洪涵洞、深孔闸泄洪"
        elif qi <= 311:
            out_water_building_item = "泄洪涵洞、深孔闸、溢洪道泄洪"
        else:
            out_water_building_item = "关闭泄洪涵洞、深孔闸、溢洪道泄洪"

        return out_water_building_item

    def base_distrtibution_dfy(self, qi):
        out_water_building_item = ""

        if qi <= 63:
            out_water_building_item = "泄洪涵洞泄洪"
        elif qi <= 144:
            out_water_building_item = "泄洪涵洞、溢洪道泄洪"
        elif qi <= 888:
            out_water_building_item = "泄洪涵洞、溢洪道、非常溢洪道泄洪"
        else:
            out_water_building_item = "非常溢洪道自溃坝自溃泄洪"

        return out_water_building_item
        
    def run(self):
        self.z = [float(self.args.z),]  # 起调水位
        self.v = [float(self.args.v),]  # 起调库容
        if self.args.scene == "<5":
            self.q = [0,]
            self.minor_q = [float(self.args.out_for_minor_flood),]  # 小洪水泄洪流量
        else:
            self.q = [float(self.args.q),]  # 起调流量
            self.minor_q = [0,]  # 小洪水泄洪流量

        # 若数据为空,则返回
        if self.z_table is None or self.v_table is None or self.z is None or self.v is None or self.q is None or self.flood_value is None or self.flood_time is None:
            print("Data is None")
            logging.error("Data is None")
            exit(0)

        if len(self.z_table) == 0 or len(self.v_table) == 0 or len(self.z) == 0 or len(self.v) == 0 or len(self.q) == 0 or len(self.flood_value) == 0 or len(self.flood_time) == 0:
            print("Data is None")
            logging.error("Data is None")
            exit(0)

        if self.args.type == "0":
            if self.args.scene == "<5":
                z, v, q = self.cal_flood_dongpu_minor(
                    self.z_table, self.v_table, self.flood_value, self.flood_time)
            else:
                z, v, q = self.cal_flood_dongpu(
                    self.z_table, self.v_table, self.flood_value, self.flood_time)
        elif self.args.type == "1":
            if self.args.scene == "<5":
                z, v, q = self.cal_flood_dafangying_minor(
                    self.z_table, self.v_table, self.flood_value, self.flood_time)
            else:
                z, v, q = self.cal_flood_dafangying(
                    self.z_table, self.v_table, self.flood_value, self.flood_time)
        else:
            print("Wrong type")
            logging.error("Wrong type")
            return

        # 错峰调度
        '''
        当南淝河防洪紧张时候，启动错峰调度，错峰调度时，下泄流量q设置为0，需要重新计算库容v和水位z。南淝河防洪紧张的判断条件为东门站水位。
        
        一、董铺水库
        1、水库水位介于汛限水位28m～20年一遇水位29.65m之间，下游无错峰要求时，全开泄洪洞、正常溢洪道深孔闸（以下简称“深孔闸”）泄洪。
        当南淝河防洪紧张时，水库为下游河道错峰。
        涨水情况下，若东门站水位≥12.5m，则完全关闭泄洪洞和深孔闸，待东门站水位<12.5m时再全开泄洪洞和深孔闸泄洪。
        落水情况下，若东门站水位≥13.5m，则完全关闭泄洪洞和深孔闸，待东门站水位<13.5m时，全开泄洪洞、深孔闸泄洪。
        2、水库水位介于20年一遇水位29.65m～100年一遇水位30.4m之间，下游无错峰要求时，完全开启泄洪洞、深孔闸泄洪。
        当南淝河防洪紧张时，水库为下游河道错峰。
        涨水情况下，若东门站水位≥14.5m，则完全关闭泄洪洞和深孔闸，待东门站水位<14.5m时再全开泄洪洞和深孔闸泄洪。
        落水情况下，若东门站水位≥15.0m，则完全关闭泄洪洞和深孔闸，待东门站水位<15.0m时，全开泄洪洞、深孔闸泄洪。
        3、水库水位在100年一遇水位30.4m以上时，按水库自身防洪安全进行调节，全开泄洪洞、深孔闸泄洪。董铺库水位超过31.4m时，泄洪洞关闭，深孔闸泄洪，溢洪道敞泄。
        
        二、大房郢水库
        1、大房郢库水位介于汛限水位27.5m~20年一遇水位29.35m，全开泄洪洞、正常溢洪道泄洪。
        当南淝河防洪紧张时，水库为下游河道错峰。
        涨水情况下，若东门站水位≥12.5m，则完全关闭泄洪洞和正常溢洪道，待东门站水位<12.5m时，再全开泄洪洞和正常溢洪道泄洪。
        落水情况下，若东门站水位≥13.5m，则完全关闭泄洪洞和正常溢洪道，待东门站水位<13.5m时，先全开泄洪洞泄洪，待东门站水位<12.5m时，再全开正常溢洪道泄洪。
        2、当大房郢水库水位介于20年一遇水位29.35m ~300年一遇水位30.45m，下游无错峰要求时，应全开泄洪洞、正常溢洪道泄洪。
        当南淝河防洪紧张时，水库为下游河道错峰。
        涨水情况下，若东门站水位≥14.5m，则完全关闭泄洪洞和正常溢洪道，待东门站水位<14.5m时，再全开泄洪洞和正常溢洪道泄洪。
        落水情况下，若东门站水位≥15.0m，则完全关闭泄洪洞和正常溢洪道，待东门站水位<15.0m时，先全开泄洪洞泄洪，待东门站水位<14.5m时，通过控制正常溢洪道的开启，使大房郢水库最大泄量不超过200m3/s。
        3、当大房郢水库水位在300年一遇水位30.45m以上时，按水库自身防洪安全进行调节，全开泄洪洞、正常溢洪道泄洪。
        4、库水位高于31.3m时，非常溢洪道自溃坝自溃泄洪。
        '''

        self.dongmen_z = self.args.dongmen_z.split(',')

        # 判断是否不进行错峰调度
        is_not_mis_peak = (len(self.dongmen_z) == 2 and self.dongmen_z[0] == -1 and self.dongmen_z[1] == -1)

        # 补全东门站数据长度为输出序列长度 + 1，补全为东门站最后一个数据
        self.dongmen_z = self.dongmen_z + \
            [self.dongmen_z[-1]] * (len(q) - len(self.dongmen_z) + 1)
        
        # 转为float
        self.dongmen_z = [float(i) for i in self.dongmen_z]


        # 泄水建筑物开启情况
        out_water_building = []

        '''
        1、董铺水库
        （1）下泄流量≤43m3/s以下时,显示“泄洪涵洞泄洪”；
        （2）下泄流量为(43, 130]m3/s时,显示“泄洪涵洞、深孔闸泄洪”；
        （3）下泄流量为(130, 311]m3/s时,显示“泄洪涵洞、深孔闸、溢洪道泄洪”；
        （4）下泄流量≥311m3/s时,显示“关闭泄洪涵洞,深孔闸、溢洪道泄洪”。
        2、大房郢水库
        （1）下泄流量≤63m3/s时,显示“泄洪涵洞泄洪”；
        （2）下泄流量为(63, 144]m3/s时,显示“泄洪涵洞、溢洪道泄洪”；
        （3）下泄流量≥144m3/s时,显示“泄洪涵洞、溢洪道、非常溢洪道泄洪”；
        （4）下泄流量≥888m3/s时,显示“非常溢洪道自溃坝自溃泄洪”。 
        '''


        Q = self.flood_value
        z_inter = interpolate.interp1d(
            self.v_table, self.z_table, kind='linear', fill_value="extrapolate")  #
        

        if self.args.type == "0":
            for i in range(len(q)):
                # 判断是否需要错峰调度
                '''
                1、错峰规则补充一条
                董铺：
                ①预报洪峰流量小于602m3/s时，董铺水位在25.5m-28m时，和水位在28m-29.65m时采用相同错峰规则。
                ②预报洪峰流量大于等于602m3/s时，董铺水位在25.5m-28m时，不考虑为下游错峰。
                大房郢：
                ①预报洪峰流量小于931m3/s时，大房郢水位在25.0m-27.5m时，和水位在27.5m-29.35m时采用相同错峰规则。
                ②预报洪峰流量大于等于931m3/s时，大房郢水位在25.0m-27.5m时，不考虑为下游错峰。
                '''

                # 判断涨水落水情况
                # is_up = self.dongmen_z[0] < self.dongmen_z[1]

                is_up = self.dongmen_z[i] < self.dongmen_z[i + 1]
                if (z[i] < 25.5):
                    out_water_building.append(self.base_distribution_dp(q[i]))
                elif (25.5 < z[i] <= 28):
                    max_peak = max(q)
                    if max_peak < 602:
                        if is_not_mis_peak == False and ((is_up == True and self.dongmen_z[i + 1] >= 12.5) or \
                            (is_up == False and self.dongmen_z[i + 1] >= 13.5)):
                            out_water_building.append("关闭泄洪涵洞、深孔闸")
                            # 更新下泄流量q
                            q[i] = 0
                        else:
                            out_water_building.append(self.base_distribution_dp(q[i]))
                    else:
                        out_water_building.append(self.base_distribution_dp(q[i]))
                elif (28 < z[i] <= 29.65):
                    if is_not_mis_peak == False and ((is_up == True and self.dongmen_z[i + 1] >= 12.5) or \
                            (is_up == False and self.dongmen_z[i + 1] >= 13.5)):
                        out_water_building.append("关闭泄洪涵洞、深孔闸")
                        # 更新下泄流量q
                        q[i] = 0
                    else:
                        out_water_building.append(self.base_distribution_dp(q[i]))

                elif (29.65 < z[i] <= 30.4):
                    if is_not_mis_peak == False and ((is_up == True and self.dongmen_z[i + 1] >= 14.5) or \
                            (is_up == False and self.dongmen_z[i + 1] >= 15.0)):
                        out_water_building.append("关闭泄洪涵洞、深孔闸")
                        # 更新下泄流量q
                        q[i] = 0
                    else:
                        out_water_building.append(self.base_distribution_dp(q[i]))
                        
                elif 30.45 < z[i] <= 31.45:
                    out_water_building.append(self.base_distribution_dp(q[i]))
                elif 31.45 < z[i] <= 34.5:
                    out_water_building.append("泄洪涵洞关闭，深孔闸泄洪，溢洪道敞泄")
                else:
                    out_water_building.append("错误，超过水库水坝高度！")

                # 更新库容和下泄流量
                if i < len(q) - 1:
                    if q[i] - 0.001 < 0.01:
                        v_next = ((Q[i] + Q[i + 1]) / 2 * 1 * 3600 - (q[i] +
                      q[i + 1]) / 2 * 1 * 3600 + v[i] * 100000000) / 100000000
                        z_next = float(z_inter(v_next))
                        z[i + 1] = z_next
                        v[i + 1] = v_next
                    else:
                        q_now = q[i]
                        z_now = z[i]
                        z_next, v_next, q_next = self.get_zvq(
                            q_now, z[i], Q[i], Q[i + 1], self.z_table, self.v_table)
                        z[i + 1] = z_next
                        v[i + 1] = v_next
                        # q[i + 1] = q_next

        elif self.args.type == "1":
            for i in range(len(q)):
                # 判断涨水落水情况
                is_up = self.dongmen_z[i] < self.dongmen_z[i + 1]

                # 判断是否需要错峰调度
                if (z[i] < 25.0):
                    out_water_building.append(self.base_distrtibution_dfy(q[i]))
                elif (25.0 < z[i] <= 27.5):
                    max_peak = max(q)
                    if max_peak < 931:
                        if is_not_mis_peak == False and ((is_up == True and self.dongmen_z[i + 1] >= 12.5) or \
                            (is_up == False and self.dongmen_z[i + 1] >= 13.5)):
                            out_water_building.append("关闭泄洪涵洞、正常溢洪道")
                            # 更新下泄流量q
                            q[i] = 0
                        else:
                            out_water_building.append(self.base_distrtibution_dfy(q[i]))
                    else:
                        out_water_building.append(self.base_distrtibution_dfy(q[i]))

                elif (27.5 < z[i] <= 29.35):
                    if is_not_mis_peak == False and ((is_up == True and self.dongmen_z[i + 1] >= 12.5) or \
                            (is_up == False and self.dongmen_z[i + 1] >= 13.5)):
                        out_water_building.append("关闭泄洪洞、正常溢洪道")
                        # 更新下泄流量q
                        q[i] = 0
                    else:
                        out_water_building.append(self.base_distrtibution_dfy(q[i]))

                elif (29.35 < z[i] <= 30.45):
                    if is_not_mis_peak == False and ((is_up == True and self.dongmen_z[i + 1] >= 14.5) or \
                            (is_up == False and self.dongmen_z[i + 1] >= 15.0)):
                        out_water_building.append("关闭泄洪涵洞、正常溢洪道")
                        # 更新下泄流量q
                        q[i] = 0
                    else:
                        out_water_building.append(self.base_distrtibution_dfy(q[i]))

                elif 30.45 < z[i] <= 31.3:
                    out_water_building.append(self.base_distrtibution_dfy(q[i]))
                elif 31.3 < z[i] <= 34.5:
                    out_water_building.append("非常溢洪道自溃坝自溃泄洪")
                else:
                    out_water_building.append("错误，超过水库水坝高度！")

                # 更新库容和下泄流量
                if i < len(q) - 1:
                    if q[i] - 0.001 < 0.01:
                        v_next = ((Q[i] + Q[i + 1]) / 2 * 1 * 3600 - (q[i] +
                      q[i + 1]) / 2 * 1 * 3600 + v[i] * 100000000) / 100000000
                        z_next = float(z_inter(v_next))

                        z[i + 1] = z_next
                        v[i + 1] = v_next
                    else:
                        q_now = q[i]
                        z_now = z[i]
                        z_next, v_next, q_next = self.get_zvq(
                            q_now, z_now, Q[i], Q[i + 1], self.z_table, self.v_table)
                        z[i + 1] = z_next
                        v[i + 1] = v_next

        self.result['out_water_building'] = out_water_building

        max_out_culvert = float(self.args.max_out_culvert)

        # 涵洞泄量 若总泄量小于涵洞泄量,则涵洞泄量为总泄量
        # q1 = [max_out_culvert] * len(q)
        q1 = [max_out_culvert if q[i] > max_out_culvert else q[i]
              for i in range(len(q))]
        # 溢洪道泄量 = 总泄量 - 涵洞泄量.
        q2 = [q[i] - q1[i] if q[i] > q1[i] else 0 for i in range(len(q))]

        # 供水时优先减去溢洪道泄量, 不够时减去涵洞泄量
        if self.args.flag == 1:
            # 新的溢洪道泄量
            q1_new = q1
            q2_new = q2

            for i in range(len(q2)):
                if self.supply_value[i] >= q2[i]:
                    self.supply_value[i] -= q2[i]
                    q2_new[i] = 0
                    # 再减去涵洞泄量
                    if self.supply_value[i] >= q1[i]:
                        self.supply_value[i] -= q1[i]
                        q1_new[i] = 0
                    else:
                        q1_new[i] -= self.supply_value[i]
                        self.supply_value[i] = 0
                else:
                    q2_new[i] -= self.supply_value[i]
                    self.supply_value[i] = 0

            self.result['q1_new'] = q1_new
            self.result['q2_new'] = q2_new

        self.result['z'] = z
        self.result['v'] = v
        self.result['q'] = q
        self.result['q1'] = q1
        self.result['q2'] = q2

        # 在结果的长度大于72个数据点之后,若水位小于limmit_z,则截断后面的数据,第一个小于的水位也要保存
        
        if self.args.type == "0":
            self.args.limmit_z = 28
        elif self.args.type == "1":
            self.args.limmit_z = 27.5

        if len(z) > 72:
            for i in range(72, len(z)):
                if z[i] < self.args.limmit_z:
                    self.result['z'] = z[:i + 1]
                    self.result['v'] = v[:i + 1]
                    self.result['q'] = q[:i + 1]
                    self.result['q1'] = q1[:i + 1]
                    self.result['q2'] = q2[:i + 1]
                    break

        return z, v, q

# 根据洪峰判断调度类型
def get_type(max_peak, type):
    x = max_peak
    if type == '0':
        scenes = {
            (0, 160): "<5",
            (160, 384.5): "5",
            (384.5, 525): "10",
            (525, 686): "20",
            (686, 843): "50",
            (843, 977): "100",
            (977, 1070): "200",
            (1070, 1862.5): "300",
            (1862.5, 2762.5): "500",
            (2762.5, 3370): "1000",
            (3370, 4750): "10000",
            (4750, 5662): "PMF",
            (5662, sys.float_info.max): "Over PMF"
        }
        for key in scenes:
            if key[0] <= x < key[1]:
                return scenes[key]

        print("Too large flood with max peak: ", max_peak)
        logging.error("Too large flood with max peak: " + str(max_peak))
        exit(0)

    elif type == '1':
        scenes = {
            (0, 160): "<5",
            (160, 466.5): "5",
            (466.5, 774): "20",
            (774, 1042): "100",
            (1042, 1812): "300",
            (1812, 2605.5): "500",
            (2605.5, 3179.5): "1000",
            (3179.5, 4454.5): "10000",
            (4454.5, 5290): "PMF",
            (5290, sys.float_info.max): "Over PMF"
        }
        for key in scenes:
            if key[0] <= x < key[1]:
                return scenes[key]

        print("Too large flood with max peak: ", max_peak)
        logging.error("Too large flood with max peak: " + str(max_peak))
        # exit(0)

    else:
        print("Wrong type")
        logging.error("Wrong type")
        exit(0)


if __name__ == "__main__":
    # Add the arguments from the command line
    pass