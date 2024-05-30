# 防洪调度
import os
import argparse
import json
import sqlite3
import math
from scipy import interpolate
import pymysql as mysql
import logging

log_file_path = 'water_control_staic.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

class FloodControlStatic:
    def __init__(self, args) -> None:
        self.args = args
        # self.z = [float(self.args.z),]  # 起调水位
        # self.v = [float(self.args.v),]  # 起调库容
        # self.q = [float(self.args.q),]  # 起调流量

        self.config = None
        self.z_table = []     # 水位表
        self.v_table = []     # 库容表
        self.limit_capacity_dataset = None  # 汛限库容
        self.limit_level_dataset = None     # 汛限水位

        self.flood_value = []  # 洪水过程
        self.flood_time = []  # 洪水过程时间

        self.result = {}

        self.load_config()
    
    def update(self, args):
        self.args = args
        self.z = [float(self.args.z),]
        self.v = [float(self.args.v),]
        self.q = [float(self.args.q),]


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
            print(e)
            logging.error("load data from db error, {}".format(e))

        print("load data from db monitor success")

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

        # 读取水位和库容表
        tablename_zv = self.config['db']['in']['values']['zv']

        # 读取设计表的对于洪水过程
        tablename_flood = self.config['db']['in']['values']['rdfh']

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

        table_name = self.config['db']['out']['values']['static']

        batch = self.args.batch
        scene = self.args.scene
        _type = self.args.type
        _zv_type = str(self.zv_type)

        # 插入数据
        for i in range(len(self.z)):
            zi = self.result['z'][i]
            vi = self.result['v'][i]
            # qi = self.result['q'][i]
            q1i = self.result['q1'][i]
            q2i = self.result['q2'][i]

            building_i = str(self.result['out_water_building'][i])

            # 小数点后3位
            zi = round(zi, 3)
            vi = round(vi, 3)
            # qi = round(qi, 3)
            q1i = round(q1i, 3)
            q2i = round(q2i, 3)

            # sql = "INSERT INTO {} VALUES ({}, {}, {}, {})".format(table_name, zi, vi, qi)
            sql = "INSERT INTO {} (Z, V, Q1, Q2, BATCH, SCENE, TYPE, TIME_SERIES, BUILDING, ZV_TYPE) VALUES ({}, {}, {}, {}, '{}', {}, {}, {}, '{}', '{}') ON DUPLICATE KEY UPDATE Z = {}, V = {}, Q1 = {}, Q2 = {}".format(
                table_name, zi, vi, q1i, q2i, batch, scene, _type, (i + 1), building_i, _zv_type, zi, vi, q1i, q2i)

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
            logging.error("config sqlite is error")
            exit(1)

        # 数据库参数
        self.sqlite_config = self.config['sqlite']['path']['values']

        # 连接数据库
        conn = sqlite3.connect(self.sqlite_config)
        cursor = conn.cursor()

        table_name = self.config['sqlite']['out']['values']['static']

        # 获取最大ID
        sql = "select max(ID) from {}".format(table_name)
        cursor.execute(sql)
        tmp = cursor.fetchall()
        max_id = int(tmp[0][0]) if tmp[0][0] else 0

        # 插入数据
        for i in range(len(self.z)):
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

    def cal_flood_dongpu(self, z11, v11, z, v, q, Q, tguo):
        v_inter = interpolate.interp1d(z11, v11, kind='linear')  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear')

        # q-v 曲线
        z_qz = self.config['setting']['QZ_table']["values"]["dongpu"]["Z"]
        q_qz = self.config['setting']['QZ_table']["values"]["dongpu"]["Q"]

        # 不同水位下的泄量插值,qi=q_inter(z[i]),一维线性插值
        q_inter_z = interpolate.interp1d(z_qz, q_qz, kind='linear')
        # 不同泄量下的水位插值,zi=z_inter(q[i]),一维线性插值
        z_inter_q = interpolate.interp1d(q_qz, z_qz, kind='linear')

        z = self.z
        v = [float(v_inter(z[i])) for i in range(len(z))]  # 根据水位插值库容
        q = [float(q_inter_z(z[i])) for i in range(len(z))]  # 根据水位插值泄量

        z_output = [float(self.args.z),]

        max_out_user = self.args.max_out_users

        t = []
        # 把时间间隔算出来
        for j in range(len(tguo) - 1):
            t.append(tguo[j + 1] - tguo[j])

        # 根据洪水历时，计算过程
        for i in range(len(Q) - 1):
            # 初始化一个q2，这个是试算的起点值q2很重要，不能初始为上一个值q1，因为q2不是一定大于q1的，过了峰值后就下降了
            q.append(31)

            # 检查是否超过用户输入的max值
            if q[i] >= max_out_user:
                q[i] = max_out_user

            z.append(28)  # 初始化一个水位z值
            z[i] = float(z_inter_q(q[i]))
            z[i + 1] = float(z_inter_q(q[i + 1]))
            v.append(0.77)  # 初始化一个库容值
            v[i] = float(v_inter(z[i]))  # 通过水位z 插值得到库容v  ，把np.array转换为float类型值
            v[i + 1] = float(v_inter(z[i + 1]))

            # （Q1 + Q2）*t / 2 -（q1 + q2）*t / 2 = v2 - v1
            while (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] - q[i + 1] > 0.01 and \
                    (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] >= q[i + 1]:  # 这里的条件判断非常重要，影响了整个取值和精度
                q[i + 1] += 0.01  # 每次q2加0.01，也说明了取值精度
                # z[i + 1] = 2.4411 * math.log(q[i + 1]) + 16.996
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i + 1] = float(v_inter(z[i + 1]))

            z_output.append(28)
            z_output[i + 1] = float(z_inter(v[i + 1]))

        return z_output, v, q

    def cal_flood_dafangying(self, z11, v11, z, v, q, Q, tguo):
        v_inter = interpolate.interp1d(z11, v11, kind='linear')  # 插值函数
        # 不同库容下的水位插值,zi=z_inter(v[i]),一维线性插值
        z_inter = interpolate.interp1d(v11, z11, kind='linear')

        # q-v 曲线
        z_qz = self.config['setting']['QZ_table']["values"]["dafangying"]["Z"]
        q_qz = self.config['setting']['QZ_table']["values"]["dafangying"]["Q"]

        # 不同水位下的泄量插值,qi=q_inter(z[i]),一维线性插值
        q_inter_z = interpolate.interp1d(z_qz, q_qz, kind='linear')
        # 不同泄量下的水位插值,zi=z_inter(q[i]),一维线性插值
        z_inter_q = interpolate.interp1d(q_qz, z_qz, kind='linear')

        z = self.z
        v = [float(v_inter(z[i])) for i in range(len(z))]  # 根据水位插值库容
        q = [float(q_inter_z(z[i])) for i in range(len(z))]  # 根据水位插值泄量

        z_output = [float(self.args.z),]

        max_out_user = self.args.max_out_users

        t = []
        # 把时间间隔算出来
        for j in range(len(tguo) - 1):
            t.append(tguo[j + 1] - tguo[j])

       # 根据洪水历时，计算过程

        for i in range(len(Q) - 1):
            # 初始化一个q2，这个是试算的起点值q2很重要，不能初始为上一个值q1，因为q2不是一定大于q1的，过了峰值后就下降了
            q.append(31)

            # 检查是否超过用户输入的max值
            if q[i] >= max_out_user:
                q[i] = max_out_user

            z.append(27.5)  # 初始化一个水位z值
            # 拟合的下泄曲线公式 y = 2.6934ln(x) + 13.658
            z[i] = float(z_inter_q(q[i]))
            z[i + 1] = float(z_inter_q(q[i + 1]))
            v.append(0.5477)  # 初始化一个库容值
            v[i] = float(v_inter(z[i]))  # 通过水位z 插值得到库容v，把np.array转换为float类型值
            v[i + 1] = float(v_inter(z[i + 1]))

            while (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] - q[i + 1] > 0.01 and \
                    (Q[i] + Q[i + 1]) + 2 * 100000000 * (v[i] - v[i + 1]) / (t[i] * 3600) - q[i] >= q[i + 1]:  # 这里的条件判断非常重要，影响了整个取值和精度，注意和计算值有关
                q[i + 1] += 0.01  # 每次q2加0.01，也说明了取值精度
                # z[i + 1] = 2.6934 * math.log(q[i + 1]) + 13.658
                z[i + 1] = float(z_inter_q(q[i + 1]))
                v[i + 1] = float(v_inter(z[i + 1]))

            z_output.append(0)
            z_output[i + 1] = float(z_inter(v[i + 1]))

        return z_output, v, q

    def run(self):
        # 若数据为空，则返回
        if self.z_table is None or self.v_table is None or self.z is None or self.v is None or self.q is None or self.flood_value is None or self.flood_time is None:
            print("Data is None")
            logging.error("Data is None")
            exit(0)

        if len(self.z_table) == 0 or len(self.v_table) == 0 or len(self.z) == 0 or len(self.v) == 0 or len(self.q) == 0 or len(self.flood_value) == 0 or len(self.flood_time) == 0:
            print("Data is None")
            logging.error("Data is None")
            exit(0)

        if self.args.type == "0":
            z, v, q = self.cal_flood_dongpu(
                self.z_table, self.v_table, self.z, self.v, self.q, self.flood_value, self.flood_time)
        elif self.args.type == "1":
            z, v, q = self.cal_flood_dafangying(
                self.z_table, self.v_table, self.z, self.v, self.q, self.flood_value, self.flood_time)
        else:
            print("Wrong type")
            logging.error("Wrong type")
            return

        max_out_culvert = float(self.args.max_out_culvert)

        # 涵洞泄量 若总泄量小于涵洞泄量，则涵洞泄量为总泄量
        # q1 = [max_out_culvert] * len(q)
        q1 = [max_out_culvert if q[i] > max_out_culvert else q[i]
              for i in range(len(q))]
        # 溢洪道泄量 = 总泄量 - 涵洞泄量.
        q2 = [q[i] - q1[i] if q[i] > q1[i] else 0 for i in range(len(q))]

        self.result['z'] = z
        self.result['v'] = v
        self.result['q'] = q
        self.result['q1'] = q1
        self.result['q2'] = q2

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
        （4）下泄流量≥888m3/s时，显示“非常溢洪道自溃坝自溃泄洪”。 

        '''

        if self.args.type == "0":
            for i in range(len(q)):
                if q[i] <= 43:
                    out_water_building.append("泄洪涵洞泄洪")
                elif q[i] <= 130:
                    out_water_building.append("泄洪涵洞、深孔闸泄洪")
                elif q[i] <= 311:
                    out_water_building.append("泄洪涵洞、深孔闸、溢洪道泄洪")
                else:
                    out_water_building.append("关闭泄洪涵洞、深孔闸、溢洪道泄洪")
        elif self.args.type == "1":
            for i in range(len(q)):
                if q[i] <= 63:
                    out_water_building.append("泄洪涵洞泄洪")
                elif q[i] <= 144:
                    out_water_building.append("泄洪涵洞、溢洪道泄洪")
                elif q[i] <= 888:
                    out_water_building.append("泄洪涵洞、溢洪道、非常溢洪道泄洪")
                else:
                    out_water_building.append("非常溢洪道自溃坝自溃泄洪")

        self.result['out_water_building'] = out_water_building

        return z, v, q


if __name__ == "__main__":
    # Add the arguments from the command line
    pass