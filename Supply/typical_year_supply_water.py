import numpy as np
import argparse
import json
import pymysql as mysql
import logging

log_file_path = 'typical_year_supply_water.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class TypicalSupply:
    def __init__(self, args):
        self.args = args
        self.input = {}
        self.load_config()
        self.result = []

    def load_config(self):
        # 从配置文件中读取配置
        with open(self.args.config, 'r', encoding="utf-8") as f:
            self.config = json.load(f)

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

        table_input = self.config['db']['in']['values']

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
            PLAN = str(self.args.plan.replace("-", "_"))

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
            sql = "INSERT INTO {} (BATCH, MONTH, Z, THROW, RB, VG, TYPE, PLAN) VALUES ('{}', {}, {}, {}, {}, {}, {}, '{}') ON DUPLICATE KEY UPDATE Z = {}, THROW = {}, RB = {}, VG = {}".format(
                table_output, _batch, MONTH, Z, THROW, RB, VG, _type, PLAN, Z, THROW, RB, VG)

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

    def run(self):
        # 读取参数
        Zqitiao = self.args.z
        pv = self.args.plan
        Zan = self.args.safty_z

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

            Vqitiao = aa * (Zqitiao ** 6) + bb * (Zqitiao ** 5) + cc * (Zqitiao ** 4) + \
                dd * (Zqitiao ** 3) + ee * (Zqitiao ** 2) + ff * Zqitiao + gg
            Van = aa * (Zan ** 6) + bb * (Zan ** 5) + cc * (Zan ** 4) + \
                dd * (Zan ** 3) + ee * (Zan ** 2) + ff * Zan + gg

        Win = self.input["inflow"]
        Da = self.input["need"]
        Dc = self.input["evaporation"]
        Dd = self.input["leakage"]
        Ds = self.input["loss"]

        year = 1
        tmp = self.get_cs_R_Com(Zan, Van, Zb, Da, Db, Dc, Dd, Ds, Win, a, b, c, d,
                                e, f, g, Zqitiao, Vqitiao, year, Zxuanxian, Vxuanxian, Zsishui, Vsishui)

        result = []

        if self.args.month != -1:
            _month = (int(self.args.month) % 100)
        else:
            _month = -1

        print(_month)
        for i in range(12):
            if _month != -1 and i + 1 != _month:
                continue
            result.append({
                "month": self.args.month if _month != -1 else self.input["month"][i],
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
    try:
        parser = argparse.ArgumentParser("Supply water")
        parser.add_argument("--z", type=float, default=25.5, help="z value.")
        parser.add_argument("--plan", type=str, default="1-1", help="plan id")
        parser.add_argument("--type", type=int, default=0, help="revisor type", choices=[0, 1])

        # parser.add_argument(
        #     '--config', type=str, default='./typical_year_supply_water_config.json', help='config file')
        
        parser.add_argument(
            '--config', type=str, default='./src/algorithm/py/Supply/typical_year_supply_water_config.json', help='config file')

        parser.add_argument("--batch", type=str,
                            default="test121", help="batch id")
        parser.add_argument("--month", type=int, default=-1, help="month id")
        parser.add_argument("--safty_z", type=float,
                            default=26.5, help="safty water level")

        args = parser.parse_args()


        print(args.__dict__)
        logging.info(args.__dict__)

        ts = TypicalSupply(args)

        ts.load_data_from_db()

        print(ts.input)

        ts.run()

        ts.write_data_to_db()

        print("{\"complete\":true}")
        logging.info("{\"complete\":true}")
    except Exception as e:
        print(e)
        logging.error(e)
