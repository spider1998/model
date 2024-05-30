import numpy as np
import argparse
import json
import pymysql as mysql
import logging

log_file_path = 'optimize_supply.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class TypicalSupply:
    def __init__(self, args):
        self.args = args
        self.input = {}
        self.load_config()
        self.result = []
        self.plan = "2"

        self.limit_capacity_dp = 7735
        self.limit_level_dp = 28

        self.limit_capacity_dfy = 5850
        self.limit_level_dfy = 27.5

        self.dead_capacity_dp = 237
        self.dead_level_dp = 18.5

        self.dead_capacity_dfy = 240
        self.dead_level_dfy = 18


    def load_config(self):
        # 从配置文件中读取配置
        with open(self.args.config, 'r', encoding="utf-8") as f:
            self.config = json.load(f)

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

            # 获取汛线水位
            sql = "SELECT (LIMIT_LEVEL) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 0)

            cursor.execute(sql)

            self.limit_level_dp = float(cursor.fetchall()[0][0])

            # 获取汛线库容
            sql = "SELECT (LIMIT_CAPACITY) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 0)
            
            cursor.execute(sql)

            self.limit_capacity_dp = float(cursor.fetchall()[0][0])

            # 获取汛线水位
            sql = "SELECT (LIMIT_LEVEL) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 1)
            
            cursor.execute(sql)

            self.limit_level_dfy = float(cursor.fetchall()[0][0])

            # 获取汛线库容
            sql = "SELECT (LIMIT_CAPACITY) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 1)
            
            cursor.execute(sql)

            self.limit_capacity_dfy = float(cursor.fetchall()[0][0])

            # 获取死水位
            sql = "SELECT (DEAD_LEVEL) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 0)
            
            cursor.execute(sql)

            self.dead_level_dp = float(cursor.fetchall()[0][0])

            # 获取死库容
            sql = "SELECT (DEAD_CAPACITY) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 0)
            
            cursor.execute(sql)

            self.dead_capacity_dp = float(cursor.fetchall()[0][0])

            # 获取死水位
            sql = "SELECT (DEAD_LEVEL) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 1)


            cursor.execute(sql)

            self.dead_level_dfy = float(cursor.fetchall()[0][0])

            # 获取死库容
            sql = "SELECT (DEAD_CAPACITY) FROM {} WHERE WATER_TYPE = '{}'".format(
                tablename_info, 1)
            
            cursor.execute(sql)

            self.dead_capacity_dfy = float(cursor.fetchall()[0][0])

            cursor.close()
            conn.close()

        except Exception as e:
            print(e)
            logging.error(e)

    def load_data_from_yearly_db(self):
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

            # 建立数据库连接
            conn = mysql.connect(host=address, port=port,
                                user=user, password=password, database=database)
            cursor = conn.cursor()

            table_input = self.config['db']['in']['values']["yearly_input"]

            # 读取数据, 按照BATCH和TYPE读取

            _batch = self.args.yearly_batch
            _type = self.args.type

            sql = "SELECT * FROM {} WHERE BATCH='{}' AND TYPE={}".format(
                table_input, _batch, _type)
            
            cursor.execute(sql)

            # 取出年净流量和年份
            tmp = cursor.fetchall()
            tmp = tmp[-1]

            self.input["inflow_yearly"] = tmp[3]


            cursor.close()
            conn.close()

        except Exception as e:
            print(e)
            logging.error(e)

        return self.input

    def load_data_from_db(self):
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

        table_input = self.config['db']['in']['values']["input"]

        # 读取数据, 按照BATCH和TYPE读取
        _batch = self.args.batch
        _type = self.args.type
        _month = self.args.month

        # 如果month为-1，则读取全部月份, 否则读取对应月份

        if _month == -1:
            sql = "SELECT * FROM {} WHERE BATCH='{}' AND TYPE={}".format(
                table_input, _batch, _type)
        else:
            sql = "SELECT * FROM {} WHERE BATCH='{}' AND TYPE={} AND MONTH={}".format(
                table_input, _batch, _type, _month)

        cursor.execute(sql)

        # 表格式：
        '''
        `ID` int NOT NULL AUTO_INCREMENT,
        `BATCH` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '批次',
        `MONTH` varchar(255) COLLATE utf8mb4_general_ci DEFAULT NULL COMMENT '月份',
        `INFLOW` double DEFAULT NULL COMMENT '来水',
        `NEED` double DEFAULT NULL COMMENT '需水',
        `EVAPORATION` double DEFAULT NULL COMMENT '蒸发',
        `LEAKAGE` double DEFAULT NULL COMMENT '渗漏',
        `LOSS` double DEFAULT NULL COMMENT '供损',
        `TYPE` int DEFAULT NULL COMMENT '水库类型',
        '''

        tmp = cursor.fetchall()

        self.input["inflow"] = [x[3] for x in tmp]
        self.input["need"] = [x[4] for x in tmp]
        self.input["evaporation"] = [x[5] for x in tmp]
        self.input["leakage"] = [x[6] for x in tmp]
        self.input["loss"] = [x[7] for x in tmp]
        self.input["month"] = [x[2] for x in tmp]

        # 若month不为-1, 补全数据
        if _month != -1:
            for i in range(12):
                if i + 1 not in [(int(self.args.month) % 100)]:
                    self.input["inflow"].insert(i, 0)
                    self.input["need"].insert(i, 0)
                    self.input["evaporation"].insert(i, 0)
                    self.input["leakage"].insert(i, 0)
                    self.input["loss"].insert(i, 0)
        else:
            # 若数据为空，则补全为0
            if len(self.input["inflow"]) == 0:
                self.input["inflow"] = [0] * 12

            if len(self.input["need"]) == 0:
                self.input["need"] = [0] * 12

            if len(self.input["evaporation"]) == 0:
                self.input["evaporation"] = [0] * 12

            if len(self.input["leakage"]) == 0:
                self.input["leakage"] = [0] * 12

            if len(self.input["loss"]) == 0:
                self.input["loss"] = [0] * 12

            for i in range(12):
                self.input["month"].append(i + 1)

        cursor.close()
        conn.close()

        return self.input

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

        table_output = self.config['db']['out']['values']

        # 写入数据
        _batch = self.args.batch
        _type = self.args.type

        print(self.result)

        for item in self.result:
            Z = float(item["z"])
            THROW = float(item["qishui"])
            RB = float(item["rb"])
            VG = float(item["vg"])
            RATE = float(item["rate"])
            RATE_B = float(item["rate_b"])
            MONTH = int(item["month"])
            PLAN = str(self.plan)
            STRATEGY = str(self.args.strategy)

            # 小数点后3位
            Z = round(Z, 3)
            THROW = round(THROW, 3)
            RB = round(RB, 3)
            VG = round(VG, 3)
            RATE = round(RATE, 3)
            RATE_B = round(RATE_B, 3)

            # print(table_output, _batch, MONTH, Z, THROW, RB, VG, RATE, _type, PLAN)

            # sql = "INSERT INTO {} (BATCH, MONTH, Z, THROW, RB, VG, RATE, RATE_B, TYPE, PLAN) VALUES ('{}', {}, {}, {}, {}, {}, {}, {}, {}, '{}') ON DUPLICATE KEY UPDATE Z = {}, THROW = {}, RB = {}, VG = {}, RATE = {}, RATE_B = {}".format(table_output, _batch, MONTH, Z, THROW, RB, VG, RATE, RATE_B, _type, PLAN, Z, THROW, RB, VG, RATE, RATE_B)

            # no rate and rate b
            sql = "INSERT INTO {} (BATCH, MONTH, Z, THROW, RB, VG, TYPE, PLAN, STRATEGY) VALUES ('{}', {}, {}, {}, {}, {}, {}, '{}', '{}') ON DUPLICATE KEY UPDATE Z = {}, THROW = {}, RB = {}, VG = {}".format(
                table_output, _batch, MONTH, Z, THROW, RB, VG, _type, PLAN, STRATEGY, Z, THROW, RB, VG)

            # print(sql)
            cursor.execute(sql)

        conn.commit()

        cursor.close()
        conn.close()

    def get_cs_R_Com(self, Zan, Van, Zb, Da, Db, Dc, Dd, Ds, Win, a, b, c, d, e, f, g, Zqitiao, Vqitaio, year, Zxunxian, Vxunxian, Zsishui, Vsishui):
        Z = Zqitiao
        V = Vqitaio
        alpha = 0.05
        Ra = [0] * year * 12
        Rb = [0] * year * 12
        counta = 0
        Vg = [0] * year * 12
        qishui = [0] * year * 12
        return_z = [0] * year * 12

        if self.args.month == -1:
            start, end = 0, 12
        else:
            _month = (int(self.args.month) % 100)
            start, end = _month - 1, _month

        for i in range(year):
            for j in range(start, end):
                V = V + Win[i * 12 + j]
                Z = a * (V ** 6) + b * (V ** 5) + c * (V ** 4) + \
                    d * (V ** 3) + e * (V ** 2) + f * V + g
                Vg[i * 12 + j] = Da[i * 12 + j]

                if Z >= Zb[j]:              # 月初水位高于补水水位，不补水
                    # 进行供水
                    V = V - Da[i * 12 + j] - Dd[i * 12 + j] - \
                        Ds[i * 12 + j] - Dc[i * 12 + j]
                    Z = a * (V ** 6) + b * (V ** 5) + c * (V ** 4) + \
                        d * (V ** 3) + e * (V ** 2) + f * V + g

                    if Z < Zan:
                        Rb[i * 12 + j] = Van - V
                        V = V + Rb[i * 12 + j]
                        Z = a * (V ** 6) + b * (V ** 5) + c * (V ** 4) + \
                            d * (V ** 3) + e * (V ** 2) + f * V + g
                        Ra[i * 12 + j] = 1
                        qishui[i * 12 + j] = 0
                    elif Z > Zxunxian[j]:           # 供完水之后，如水位过量产生弃水
                        qishui[i * 12 + j] = V - Vxunxian[j]
                        V = Vxunxian[j]
                        Z = Zxunxian[j]
                        Rb[i * 12 + j] = 0
                    else:
                        qishui[i * 12 + j] = 0
                        Rb[i * 12 + j] = 0
                    return_z[i * 12 + j] = Z

                else:                               # 月初水位低于补水水位，当月补水
                    V = V + Db[j]
                    Z = a * (V ** 6) + b * (V ** 5) + c * (V ** 4) + \
                        d * (V ** 3) + e * (V ** 2) + f * V + g

                    # 进行供水
                    V = V - Da[i * 12 + j] - Dd[i * 12 + j] - \
                        Ds[i * 12 + j] - Dc[i * 12 + j]
                    Z = a * (V ** 6) + b * (V ** 5) + c * (V ** 4) + \
                        d * (V ** 3) + e * (V ** 2) + f * V + g

                    if Z < Zan:
                        Rb[i * 12 + j] = Van - V + Db[j]
                        V = Van
                        Z = a * (V ** 6) + b * (V ** 5) + c * (V ** 4) + \
                            d * (V ** 3) + e * (V ** 2) + f * V + g
                        Ra[i * 12 + j] = 1
                        qishui[i * 12 + j] = 0
                    elif Z > Zxunxian[j]:           # 供完水之后，如水位过量产生弃水
                        qishui[i * 12 + j] = V - Vxunxian[j]
                        V = Vxunxian[j]
                        Z = Zxunxian[j]
                        Rb[i * 12 + j] = Db[j]
                    else:
                        qishui[i * 12 + j] = 0
                        Rb[i * 12 + j] = Db[j]

                    return_z[i * 12 + j] = Z

        bzv = (counta / (year * 12 + 1)) * 100
        rxa = np.mean(Ra) * 100
        rxb = np.sum(Rb)

        result = {
            "rxa": rxa,
            "rxb": rxb,
            "z": return_z,
            "qishui": qishui,
            "bzv": bzv,
            "rb": Rb,
            "vg": Vg,
        }

        print(result)

        return result

    def get_plan(self, inflow_year):

        MAX_INT = 0x7fffffff

        boundery_dfy = {
            "1": [7000, MAX_INT],       # 7000以上
            "2": [5000, 7000],   # 5000-7000
            "3": [4200, 5000],
            "4": [2800, 4200],
            "5": [0, 2800],
        }

        boundery_dp = {
            "1": [7000, MAX_INT],
            "2": [5000, 7000],
            "3": [3500, 5000],
            "4": [2500, 3500],
            "5": [0, 2500],
        }

        if self.args.type == 0:
            boundery = boundery_dp
        elif self.args.type == 1:
            boundery = boundery_dfy
        else:
            print("type error")
            logging.error("type error")
            exit(1)

        selected_plan = "0"
        if self.args.strategy == "long-series":
            selected_plan = "0"
        elif self.args.strategy == "typical-year":
            # select paln id by inflow_year
            for key, b in boundery.items():
                if inflow_year >= b[0] and inflow_year < b[1]:
                    selected_plan = key
                    break
        else:
            print("strategy error")
            logging.error("strategy error")
            exit(1)

        return selected_plan

    def get_paln_data(self, pv, Zqitiao, Zan):
        # 获取对应方案的补水水位、限制水位、补水水量
        if self.args.type == 0:
            Zb = self.config["setting"]["dongpu"]["plan"][pv]["supply_water_level"]
            Db = self.config["setting"]["dongpu"]["plan"][pv]["supply_water_volume"]

            a = self.config["setting"]["params"]["values"]["dp_a"]
            b = self.config["setting"]["params"]["values"]["dp_b"]
            c = self.config["setting"]["params"]["values"]["dp_c"]
            d = self.config["setting"]["params"]["values"]["dp_d"]
            e = self.config["setting"]["params"]["values"]["dp_e"]
            f = self.config["setting"]["params"]["values"]["dp_f"]
            g = self.config["setting"]["params"]["values"]["dp_g"]

            aa = self.config["setting"]["params"]["values"]["dp_aa"]
            bb = self.config["setting"]["params"]["values"]["dp_bb"]
            cc = self.config["setting"]["params"]["values"]["dp_cc"]
            dd = self.config["setting"]["params"]["values"]["dp_dd"]
            ee = self.config["setting"]["params"]["values"]["dp_ee"]

            Zxuanxian = self.config["setting"]["params"]["values"]["Zxunxian_dp"]
            Vxuanxian = self.config["setting"]["params"]["values"]["Vxunxian_dp"]
            Zsishui = self.config["setting"]["params"]["values"]["Zsishui_dp"]
            Vsishui = self.config["setting"]["params"]["values"]["Vsishui_dp"]

            Zxuanxian = [self.limit_level_dp] * 12
            Vxuanxian = [self.limit_capacity_dp] * 12
            
            # 为6-9月的汛线水位，其余月份汛线水位董铺+1，大房郢+0.5
            # 为6-9月的汛线库容，其余月份汛线库容董铺+1873.8，大房郢+700.12

            for i in range(12):
                if i + 1 >= 6 and i + 1 <= 9:
                    Zxuanxian[i] = self.limit_level_dp
                    Vxuanxian[i] = self.limit_capacity_dp
                else:
                    Zxuanxian[i] = self.limit_level_dp + 1
                    Vxuanxian[i] = self.limit_capacity_dp + 1873.8

            Zsishui = self.dead_level_dp
            Vsishui = self.dead_capacity_dp

            Vqitiao = aa * (Zqitiao ** 4) + bb * (Zqitiao ** 3) + \
                cc * (Zqitiao ** 2) + dd * Zqitiao + ee
            Van = aa * (Zan ** 4) + bb * (Zan ** 3) + \
                cc * (Zan ** 2) + dd * Zan + ee

        elif self.args.type == 1:
            Zb = self.config["setting"]["dafangying"]["plan"][pv]["supply_water_level"]
            Db = self.config["setting"]["dafangying"]["plan"][pv]["supply_water_volume"]

            a = self.config["setting"]["params"]["values"]["dfy_a"]
            b = self.config["setting"]["params"]["values"]["dfy_b"]
            c = self.config["setting"]["params"]["values"]["dfy_c"]
            d = self.config["setting"]["params"]["values"]["dfy_d"]
            e = self.config["setting"]["params"]["values"]["dfy_e"]
            f = self.config["setting"]["params"]["values"]["dfy_f"]
            g = self.config["setting"]["params"]["values"]["dfy_g"]

            aa = self.config["setting"]["params"]["values"]["dfy_aa"]
            bb = self.config["setting"]["params"]["values"]["dfy_bb"]
            cc = self.config["setting"]["params"]["values"]["dfy_cc"]
            dd = self.config["setting"]["params"]["values"]["dfy_dd"]
            ee = self.config["setting"]["params"]["values"]["dfy_ee"]
            ff = self.config["setting"]["params"]["values"]["dfy_ff"]
            gg = self.config["setting"]["params"]["values"]["dfy_gg"]

            Zxuanxian = self.config["setting"]["params"]["values"]["Zxunxian_dfy"]
            Vxuanxian = self.config["setting"]["params"]["values"]["Vxunxian_dfy"]
            Zsishui = self.config["setting"]["params"]["values"]["Zsishui_dfy"]
            Vsishui = self.config["setting"]["params"]["values"]["Vsishui_dfy"]

            Zxuanxian = [self.limit_level_dfy] * 12
            Vxuanxian = [self.limit_capacity_dfy] * 12

            # 为6-9月的汛线水位，其余月份汛线水位董铺+1，大房郢+0.5
            # 为6-9月的汛线库容，其余月份汛线库容董铺+1873.8，大房郢+700.12

            for i in range(12):
                if i + 1 >= 6 and i + 1 <= 9:
                    Zxuanxian[i] = self.limit_level_dfy
                    Vxuanxian[i] = self.limit_capacity_dfy
                else:
                    Zxuanxian[i] = self.limit_level_dfy + 0.5
                    Vxuanxian[i] = self.limit_capacity_dfy + 700.12
            
            Zsishui = self.dead_level_dfy
            Vsishui = self.dead_capacity_dfy

            Vqitiao = aa * (Zqitiao ** 6) + bb * (Zqitiao ** 5) + cc * (Zqitiao ** 4) + \
                dd * (Zqitiao ** 3) + ee * (Zqitiao ** 2) + ff * Zqitiao + gg
            Van = aa * (Zan ** 6) + bb * (Zan ** 5) + cc * (Zan ** 4) + \
                dd * (Zan ** 3) + ee * (Zan ** 2) + ff * Zan + gg

        return Zb, Db, a, b, c, d, e, f, g, Zxuanxian, Vxuanxian, Zsishui, Vsishui, Vqitiao, Van

    def run(self):
        # 读取参数
        Zqitiao = self.args.z
        # pv = self.args.plan
        Zan = self.args.safty_z
        inflow_year = self.args.annual_inflow

        Win = self.input["inflow"]
        Da = self.input["need"]
        Dc = self.input["evaporation"]
        Dd = self.input["leakage"]
        Ds = self.input["loss"]

        # 模式0，输入年净流量，先计算月净流量，再计算补水水位
        if self.args.mode == 0 or self.args.mode == 2:

            # 模式2从数据库中读取年净流量
            if self.args.mode == 2:
                self.load_data_from_yearly_db()
                inflow_year = self.input["inflow_yearly"]

            pv = self.get_plan(inflow_year)
            self.plan = pv


            # split annual inflow to monthly inflow

            # 1. get inflow fumula
            if self.args.type == 0:
                if self.args.strategy == "long-series":
                    ratio_by_month = self.config["setting"]["dongpu"]["inflow_formula"]["2"]
                elif self.args.strategy == "typical-year":
                   ratio_by_month = self.config["setting"]["dongpu"]["inflow_formula"][pv]
                else:
                    print("strategy error")
                    logging.error("strategy error")
                    exit(1)
            elif self.args.type == 1:
                if self.args.strategy == "long-series":
                    ratio_by_month = self.config["setting"]["dafangying"]["inflow_formula"]["2"]
                elif self.args.strategy == "typical-year":
                    ratio_by_month = self.config["setting"]["dafangying"]["inflow_formula"][pv]
                else:
                    print("strategy error")
                    logging.error("strategy error")
                    exit(1)


            # 2. get monthly inflow
            Win = [inflow_year * x for x in ratio_by_month]

            self.input["inflow"] = Win

            print(self.input)
            logging.info(self.input)
            
            Zb, Db, a, b, c, d, e, f, g, Zxuanxian, Vxuanxian, Zsishui, Vsishui, Vqitiao, Van = self.get_paln_data(
                pv, Zqitiao, Zan)
            
            print(Zxuanxian, Vxuanxian, Zsishui, Vsishui)

            year = 1
            tmp = self.get_cs_R_Com(Zan, Van, Zb, Da, Db, Dc, Dd, Ds, Win, a, b, c, d,
                                    e, f, g, Zqitiao, Vqitiao, year, Zxuanxian, Vxuanxian, Zsishui, Vsishui)

        elif self.args.mode == 1:
            print(self.input)
            logging.info(self.input)
            inflow_year = sum(Win)

            pv = self.get_plan(inflow_year)
            self.plan = pv

            Zb, Db, a, b, c, d, e, f, g, Zxuanxian, Vxuanxian, Zsishui, Vsishui, Vqitiao, Van = self.get_paln_data(
                pv, Zqitiao, Zan)

            year = 1
            tmp = self.get_cs_R_Com(Zan, Van, Zb, Da, Db, Dc, Dd, Ds, Win, a, b, c, d,
                                    e, f, g, Zqitiao, Vqitiao, year, Zxuanxian, Vxuanxian, Zsishui, Vsishui)

        else:
            print("mode error")
            logging.error("mode error")
            exit(1)

        result = []

        if self.args.month != -1:
            _month = (int(self.args.month) % 100)
        else:
            _month = -1

        print(_month)
        for i in range(12):
            if _month != -1 and i + 1 != _month:
                continue
            # output_month = self.args.month if _month != -1 else (i + 1)

            result.append({
                "month": self.input["month"][i],
                "z": tmp["z"][i],
                "qishui": tmp["qishui"][i],
                "rb": tmp["rb"][i],
                "vg": tmp["vg"][i],
                "rate": tmp["rxa"],
                "rate_b": tmp["bzv"] if _month == -1 else -1,
            })

        self.result = result
        return result


if __name__ == "__main__":
    # Add the arguments from the command line
    pass