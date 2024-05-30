
import sys
import json
import os
import numpy as np
import argparse
import pymysql as mysql

import logging

log_file_path = 'regular_supply.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG,
                    format='%(asctime)s - %(levelname)s - %(message)s')


class Supply:
    def __init__(self, args) -> None:
        self.args = args
        self.result = {}
        self.limit_level_dfy_datasets = [27.5] * 12

        self.load_config()

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



            self.limit_level_dfy_datasets = [self.limit_level_dfy] * 12

            for i in range(12):
                if i + 1 >= 6 and i + 1 <= 9:
                    self.limit_level_dfy_datasets[i] = self.limit_level_dfy
                else:
                    self.limit_level_dfy_datasets[i] = self.limit_level_dfy + 0.5
                    
            cursor.close()
            conn.close()

        except Exception as e:
            print(e)
            logging.error(e)


    def load_config(self):
        # 从配置文件中读取配置
        with open(self.args.config, 'r', encoding="utf-8") as f:
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
        conn = mysql.connect(host=address, port=port,
                             user=user, password=password, database=database)
        cursor = conn.cursor()

        # write your code here

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

        # write your code here
        table_name = self.config["db"]['out']["values"]

        batch = self.args.batch
        dp_flag = self.result["dp_supply"]["flag"]
        dp_days = self.result["dp_supply"]["days"]
        dp_volume_now = self.result["dp_supply"]["volume"]
        dp_inflow_yes = self.result["dp_supply"]["inflow"]

        dfy_flag = self.result["dfy_supply"]["flag"]
        dfy_days = self.result["dfy_supply"]["days"]
        dfy_volume_now = self.result["dfy_supply"]["volume"]
        dfy_inflow_yes = self.result["dfy_supply"]["inflow"]

        time = self.args.time

        # inflow 和 volume 保留三位小数
        dp_inflow_yes = round(dp_inflow_yes, 3)
        dp_volume_now = round(dp_volume_now, 3)
        dfy_inflow_yes = round(dfy_inflow_yes, 3)
        dfy_volume_now = round(dfy_volume_now, 3)

        mode = self.args.mode

        if mode == 1:
            plan_days = self.result["plan-1"]["plan_days"]
            dp_plan_volume = self.result["plan-1"]["dp_plan_volume"]
            dfy_plan_volume = self.result["plan-1"]["dfy_plan_volume"]
            dfy_z = self.result["plan-1"]["dfy_z"]
            dp_z = self.result["plan-1"]["dp_z"]
            dp_volume = self.result["plan-1"]["dp_volume"]
            dfy_volume = self.result["plan-1"]["dfy_volume"]
            dp_start_z = self.result["plan-1"]["dp_start_z"]
            dfy_start_z = self.result["plan-1"]["dfy_start_z"]
            dp_start_v = self.result["plan-1"]["dp_start_v"]
            dfy_start_v = self.result["plan-1"]["dfy_start_v"]
            message = self.result["plan-1"]["message"]


        elif mode == 2:
            plan_days = self.result["plan-2"]["plan_days"]
            dp_plan_volume = self.result["plan-2"]["dp_plan_volume"]
            dfy_plan_volume = self.result["plan-2"]["dfy_plan_volume"]
            dfy_z = self.result["plan-2"]["dfy_z"]
            dp_z = self.result["plan-2"]["dp_z"]
            dp_volume = self.result["plan-2"]["dp_volume"]
            dfy_volume = self.result["plan-2"]["dfy_volume"]
            dp_start_z = self.result["plan-2"]["dp_start_z"]
            dfy_start_z = self.result["plan-2"]["dfy_start_z"]
            dp_start_v = self.result["plan-2"]["dp_start_v"]
            dfy_start_v = self.result["plan-2"]["dfy_start_v"]
            message = self.result["plan-2"]["message"]

        # 保留三位小数
        dp_z = round(dp_z, 3)
        dfy_z = round(dfy_z, 3)
        dp_volume = round(dp_volume, 3)
        dfy_volume = round(dfy_volume, 3)
        dp_plan_volume = round(dp_plan_volume, 3)
        dfy_plan_volume = round(dfy_plan_volume, 3)
        dp_start_z = round(dp_start_z, 3)
        dfy_start_z = round(dfy_start_z, 3)
        dp_start_v = round(dp_start_v, 3)
        dfy_start_v = round(dfy_start_v, 3)


        sql = f"insert into {table_name} (BATCH, DP_FLAG, DP_DAYS, DP_VOLUME_NOW, DP_INFLOW_YES, DFY_FLAG, DFY_DAYS, DFY_VOLUME_NOW, DFY_INFLOW_YES, TIME, PLAN_DAYS, DP_PLAN_VOLUME, DFY_PLAN_VOLUME, DFY_Z, DP_Z, DP_VOLUME, DFY_VOLUME, MESSAGE, MODE, DP_START_Z, DP_START_V, DFY_START_Z, DFY_START_V) values ('{batch}', '{dp_flag}', '{dp_days}', '{dp_volume_now}', '{dp_inflow_yes}', '{dfy_flag}', '{dfy_days}', '{dfy_volume_now}', '{dfy_inflow_yes}', '{time}', '{plan_days}', '{dp_plan_volume}', '{dfy_plan_volume}', '{dfy_z}', '{dp_z}', '{dp_volume}', '{dfy_volume}', '{message}', '{mode}', '{dp_start_z}', '{dp_start_v}', '{dfy_start_z}', '{dfy_start_v}')"

        cursor.execute(sql)

        conn.commit()

        cursor.close()
        conn.close()

    def supply_water_dongpu(self, aa2, bb2, cc2, dd2, ee2, Zjin, Zzuo, Zan, Dbu, Dxie, Dzheng, Dlian, Dgong, Tyu):
        result = {}
        # 董铺水库
        Vjin = aa2 * Zjin**4 + bb2 * Zjin**3 + cc2 * Zjin**2 + dd2 * Zjin + ee2
        Vzuo = aa2 * Zzuo**4 + bb2 * Zzuo**3 + cc2 * Zzuo**2 + dd2 * Zzuo + ee2
        Win2 = Vjin - Vzuo - Dbu + Dxie + Dzheng - Dlian + Dgong

        result["Vjin"] = float(Vjin)
        result["Vzuo"] = float(Vzuo)

        if Win2 >= 0:
            # print('昨日董铺水库来水量')
            # print(Win2)
            result["inflow"] = float(Win2)
        else:
            # print('昨日董铺水库未产生来水')
            # print(0)
            result["inflow"] = 0

        Van2 = aa2 * Zan**4 + bb2 * Zan**3 + cc2 * Zan**2 + dd2 * Zan + ee2
        Vsheng2 = Vjin - Van2
        # print('董铺水库剩余水量(万方)')
        # print(Vsheng2)
        result["volume"] = float(Vsheng2)

        T = int((Vsheng2 + Dbu + Win2) / (Dgong + Dxie + Dzheng))
        # print('董铺水库剩余水量可用天数')
        # print(T)
        result["days"] = int(T)

        if T <= Tyu:
            # print('水库水量已不足')
            result["flag"] = 0
        else:
            result["flag"] = 1

        return result

    def supply_water_dafangying(self, aa1, bb1, cc1, dd1, ee1, ff1, gg1, Zjin, Zzuo, Zan, Dbu, Dxie, Dzheng, Dlian, Dgong, Tyu):
        result = {}
        # 大房郢水库
        Vjin = aa1 * Zjin**6 + bb1 * Zjin**5 + cc1 * Zjin**4 + \
            dd1 * Zjin**3 + ee1 * Zjin**2 + ff1 * Zjin + gg1
        Vzuo = aa1 * Zzuo**6 + bb1 * Zzuo**5 + cc1 * Zzuo**4 + \
            dd1 * Zzuo**3 + ee1 * Zzuo**2 + ff1 * Zzuo + gg1
        Win = Vjin - Vzuo - Dbu + Dxie + Dzheng - Dlian + Dgong

        result["Vjin"] = float(Vjin)
        result["Vzuo"] = float(Vzuo)

        if Win >= 0:
            # print('昨日大房郢水库来水量')
            # print(Win)
            result["inflow"] = float(Win)
        else:
            # print('昨日大房郢水库未产生来水')
            # print(0)
            result["inflow"] = 0

        Van1 = aa1 * Zan**6 + bb1 * Zan**5 + cc1 * Zan**4 + \
            dd1 * Zan**3 + ee1 * Zan**2 + ff1 * Zan + gg1
        Vsheng1 = Vjin - Van1
        # print('大房郢水库剩余水量(万方)')
        # print(Vsheng1)
        result["volume"] = float(Vsheng1)

        T = int((Vsheng1 + Dbu + Win) / (Dgong + Dxie + Dzheng))
        # print('大房郢水库剩余水量可用天数')
        # print(T)
        result["days"] = int(T)
        if T <= Tyu:
            # print('水库水量已不足')
            result["flag"] = 0
        else:
            result["flag"] = 1

        return result

    def calculate_mode_one(self, a1, b1, c1, d1, e1, f1, g1,
                           aa2, bb2, cc2, dd2, ee2,
                           Lyu1, Lyu2, Zjieshu2,
                           Vqishi1, Vqishi2,
                           Dbchu1, Dbchu2,
                           Djiang1,
                           Zan1, Zxunxian1
                           ):

        Vjieshu2 = aa2 * Zjieshu2**4 + bb2 * Zjieshu2**3 + \
            cc2 * Zjieshu2**2 + dd2 * Zjieshu2 + ee2

        Dbri1 = Lyu1 * 86400 / 10000
        Dbri2 = Lyu2 * 86400 / 10000

        Tyu = max(0, int((Vjieshu2 - Vqishi2) / (Dbri2 - Dbchu2)))

        Db2 = Tyu * Dbri2
        Db1 = Tyu * Dbri1

        Vjieshu1 = Vqishi1 + Db1 - Dbchu1 * Tyu + Djiang1 * Tyu
        Zjieshu1 = a1 * Vjieshu1**6 + b1 * Vjieshu1**5 + c1 * Vjieshu1**4 + \
            d1 * Vjieshu1**3 + e1 * Vjieshu1**2 + f1 * Vjieshu1 + g1

        result = {
            "plan_days": Tyu,
            "dp_plan_volume": Db2,
            "dfy_plan_volume": Db1,
            "dfy_z": Zjieshu1,
            "dp_z": Zjieshu2,
            "dp_volume": Vjieshu2,
            "dfy_volume": Vjieshu1,
        }

        if Tyu >= 0 and Zjieshu1 > Zan1 and Zjieshu1 <= Zxunxian1:
            result["message"] = "正常供水"
        else:
            # print('输入有误，请重新输入')
            result["message"] = "大房郢水库预计到达水位较高，请重新输入"

        return result

    def calculate_mode_two(self, a2, b2, c2, d2, e2, f2, g2,
                           a1, b1, c1, d1, e1, f1, g1,
                           Lyu1, Lyu2, Dzl,
                           Vqishi1, Vqishi2,
                           Dbchu1, Dbchu2,
                           ):
        Dbri1 = Lyu1 * 86400 / 10000
        Dbri2 = Lyu2 * 86400 / 10000
        Tyu = max(1, int(Dzl / (Dbri1 + Dbri2)))

        DZL2 = Tyu * Dbri2
        DZL1 = Tyu * Dbri1

        result = {
            "plan_days": Tyu,
            "dp_plan_volume": DZL2,
            "dfy_plan_volume": DZL1,
        }

        Vjieshu2 = Vqishi2 + (Dbri2 - Dbchu2) * Tyu

        result["dp_volume"] = Vjieshu2

        Zjieshu2 = a2 * Vjieshu2 ** 6 + b2 * Vjieshu2 ** 5 + c2 * Vjieshu2 ** 4 + \
            d2 * Vjieshu2 ** 3 + e2 * Vjieshu2 ** 2 + f2 * Vjieshu2 + g2

        result["dp_z"] = Zjieshu2

        if result["dp_z"] < 0:
            result["dp_z"] = 0

        Vjieshu1 = Vqishi1 + (Dbri1 - Dbchu1) * Tyu

        result["dfy_volume"] = Vjieshu1

        Zjieshu1 = a1 * Vjieshu1 ** 6 + b1 * Vjieshu1 ** 5 + c1 * Vjieshu1 ** 4 + \
            d1 * Vjieshu1 ** 3 + e1 * Vjieshu1 ** 2 + f1 * Vjieshu1 + g1

        result["dfy_z"] = Zjieshu1

        if result["dfy_z"] < 0:
            result["dfy_z"] = 0

        result["message"] = "正常供水"

        return result

    def run(self):
        Zjin_dongpu = self.args.today_z_dongpu
        Zzuo_dongpu = self.args.yesterday_z_dongpu
        Zan_dongpu = self.args.safty_z_dongpu
        Dbu_dongpu = self.args.yesterday_replenishment_dongpu
        Dxie_dongpu = self.args.yesterday_discharge_dongpu
        Dzheng_dongpu = self.args.yesterday_evaporation_dongpu
        Dlian_dongpu = self.args.yesterday_linkpipe_dongpu
        Dgong_dongpu = self.args.yesterday_supply_dongpu

        Zjin_dafangying = self.args.today_z_dafangying
        Zzuo_dafangying = self.args.yesterday_z_dafangying
        Zan_dafangying = self.args.safty_z_dafangying
        Dbu_dafangying = self.args.yesterday_replenishment_dafangying
        Dxie_dafangying = self.args.yesterday_discharge_dafangying
        Dzheng_dafangying = self.args.yesterday_evaporation_dafangying
        Dlian_dafangying = -self.args.yesterday_linkpipe_dongpu
        Dgong_dafangying = self.args.yesterday_supply_dafangying

        Tyu_dp = self.args.warning_days_dp
        Tyu_dfy = self.args.warning_days_dfy
        Tqi = self.args.turnover_days

        Djiang_dongpu = self.args.J_replenishment_dongpu
        Dbgong_dongpu = self.args.replenishment_dongpu
        Dbzheng_dongpu = self.args.evaporation_dongpu

        Dbjiang_dafangying = self.args.J_replenishment_dafangying
        Dbgong_dafangying = self.args.replenishment_dafangying
        Dbzheng_dafangying = self.args.evaporation_dafangying

        Lyu_dongpu = self.args.H_replenishment_dongpu
        Lyu_dafangying = self.args.H_replenishment_dafangying

        Zjieshu_dongpu = self.args.expected_z_dongpu

        # 根据输入的time获取汛限水位
        month = int(self.args.time.split('-')[1])
        Zxunxian_dfy = float(
            self.limit_level_dfy_datasets[month - 1])
        
        print(Zxunxian_dfy)

        Dz = self.args.total_amount

        a1 = self.config["setting"]["values"]["dfy_a"]
        b1 = self.config["setting"]["values"]["dfy_b"]
        c1 = self.config["setting"]["values"]["dfy_c"]
        d1 = self.config["setting"]["values"]["dfy_d"]
        e1 = self.config["setting"]["values"]["dfy_e"]
        f1 = self.config["setting"]["values"]["dfy_f"]
        g1 = self.config["setting"]["values"]["dfy_g"]

        a2 = self.config["setting"]["values"]["dp_a"]
        b2 = self.config["setting"]["values"]["dp_b"]
        c2 = self.config["setting"]["values"]["dp_c"]
        d2 = self.config["setting"]["values"]["dp_d"]
        e2 = self.config["setting"]["values"]["dp_e"]
        f2 = self.config["setting"]["values"]["dp_f"]
        g2 = self.config["setting"]["values"]["dp_g"]

        aa1 = self.config["setting"]["values"]["dfy_aa"]
        bb1 = self.config["setting"]["values"]["dfy_bb"]
        cc1 = self.config["setting"]["values"]["dfy_cc"]
        dd1 = self.config["setting"]["values"]["dfy_dd"]
        ee1 = self.config["setting"]["values"]["dfy_ee"]
        ff1 = self.config["setting"]["values"]["dfy_ff"]
        gg1 = self.config["setting"]["values"]["dfy_gg"]

        aa2 = self.config["setting"]["values"]["dp_aa"]
        bb2 = self.config["setting"]["values"]["dp_bb"]
        cc2 = self.config["setting"]["values"]["dp_cc"]
        dd2 = self.config["setting"]["values"]["dp_dd"]
        ee2 = self.config["setting"]["values"]["dp_ee"]

        # 供水调度
        supply_result_dongpu = self.supply_water_dongpu(
            aa2, bb2, cc2, dd2, ee2, Zjin_dongpu, Zzuo_dongpu, Zan_dongpu, Dbu_dongpu, Dxie_dongpu, Dzheng_dongpu, Dlian_dongpu, Dgong_dongpu, Tyu_dp)
        supply_result_dafangying = self.supply_water_dafangying(aa1, bb1, cc1, dd1, ee1, ff1, gg1, Zjin_dafangying, Zzuo_dafangying,
                                                                Zan_dafangying, Dbu_dafangying, Dxie_dafangying, Dzheng_dafangying, Dlian_dafangying, Dgong_dafangying, Tyu_dfy)

        print(supply_result_dafangying)
        print(supply_result_dongpu)

        self.result["dp_supply"] = supply_result_dongpu
        self.result["dfy_supply"] = supply_result_dafangying

        Vjin_dongpu = supply_result_dongpu["Vjin"]
        Vjin_dafangying = supply_result_dafangying["Vjin"]

        # 补水计划生成
        Vqi_dongpu = Vjin_dongpu - Dbgong_dongpu * Tqi + \
            Dlian_dongpu * Tqi + Djiang_dongpu * Tqi
        Zqishui_dongpu = a2 * Vqi_dongpu**6 + b2 * Vqi_dongpu**5 + c2 * \
            Vqi_dongpu**4 + d2 * Vqi_dongpu**3 + e2 * Vqi_dongpu**2 + f2 * Vqi_dongpu + g2
        Vqishui_dongpu = aa2 * Zqishui_dongpu**4 + bb2 * Zqishui_dongpu**3 + \
            cc2 * Zqishui_dongpu**2 + dd2 * Zqishui_dongpu + ee2

        Vqi_dafangying = Vjin_dafangying - Dbgong_dafangying * \
            Tqi + Dlian_dafangying * Tqi + Dbjiang_dafangying * Tqi
        Zqishui_dafangying = a1 * Vqi_dafangying**6 + b1 * Vqi_dafangying**5 + c1 * \
            Vqi_dafangying**4 + d1 * Vqi_dafangying**3 + e1 * \
            Vqi_dafangying**2 + f1 * Vqi_dafangying + g1
        Vqishui_dafangying = aa1 * Zqishui_dafangying**6 + bb1 * Zqishui_dafangying**5 + cc1 * Zqishui_dafangying**4 + \
            dd1 * Zqishui_dafangying**3 + ee1 * \
            Zqishui_dafangying**2 + ff1 * Zqishui_dafangying + gg1

        DbChu_dafangying = Dbgong_dafangying + Dbzheng_dafangying
        DbChu_dongpu = Dbgong_dongpu + Dbzheng_dongpu

        # 模式一
        if self.args.mode == 1:
            result = self.calculate_mode_one(a1, b1, c1, d1, e1, f1, g1,
                                             aa2, bb2, cc2, dd2, ee2,
                                             Lyu_dafangying, Lyu_dongpu, Zjieshu_dongpu,
                                             Vqishui_dafangying, Vqishui_dongpu,
                                             DbChu_dafangying, DbChu_dongpu,
                                             Djiang_dongpu,
                                             Zan_dafangying, Zxunxian_dfy
                                             )

            result['dp_start_z'] = Zqishui_dongpu
            result['dfy_start_z'] = Zqishui_dafangying
            result['dp_start_v'] = Vqishui_dongpu
            result['dfy_start_v'] = Vqishui_dafangying

            print(result)

            self.result["plan-1"] = result

        # 模式二
        elif self.args.mode == 2:
            result = self.calculate_mode_two(a2, b2, c2, d2, e2, f2, g2,
                                             a1, b1, c1, d1, e1, f1, g1,
                                             Lyu_dafangying, Lyu_dongpu, Dz,
                                             Vqishui_dafangying, Vqishui_dongpu,
                                             DbChu_dafangying, DbChu_dongpu,
                                             )
            
            result['dp_start_z'] = Zqishui_dongpu
            result['dfy_start_z'] = Zqishui_dafangying
            result['dp_start_v'] = Vqishui_dongpu
            result['dfy_start_v'] = Vqishui_dafangying

            print(result)

            self.result["plan-2"] = result

if __name__ == "__main__":
    # Add the arguments from the command line
    pass