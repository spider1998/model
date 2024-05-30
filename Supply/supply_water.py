import sys
import json
import os
import numpy as np
import argparse
import pymysql as mysql

import logging

log_file_path = 'supply_water.log'
# 日志配置
logging.basicConfig(filename=log_file_path, level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')


class Supply:
    def __init__(self, args) -> None:
        self.args = args
        self.result = {}

        self.load_config()

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
        conn = mysql.connect(host=address, port=port, user=user, password=password, database=database)
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
        conn = mysql.connect(host=address, port=port, user=user, password=password, database=database)
        cursor = conn.cursor()

        # write your code here
        table_name = self.config["db"]['out']["values"]

        batch = self.args.batch

        dongpu_z = round(self.result["dongpu"]["Z"], 3)
        dongpu_v = round(self.result["dongpu"]["V"], 3)
        dongpu_flag = self.result["dongpu"]["flag"]
        
        dafangying_z = round(self.result["dafangying"]["Z"], 3)
        dafangying_v = round(self.result["dafangying"]["V"], 3)
        dafangying_flag = self.result["dafangying"]["flag"]
        
        sql = "INSERT INTO {} (BATCH, TYPE, FLAG, Z, V) VALUES ('{}', '{}', '{}', {}, {})".format(table_name, batch, "0", dongpu_flag, dongpu_z, dongpu_v)
        print(sql)
       
        cursor.execute(sql)
        
        sql = "INSERT INTO {} (BATCH, TYPE, FLAG, Z, V) VALUES ('{}', '{}', '{}', {}, {})".format(table_name, batch, "1", dafangying_flag, dafangying_z, dafangying_v)
        print(sql)
       
        cursor.execute(sql)

        conn.commit()

        cursor.close()
        conn.close()

    def run(self):
        Zqitiao1 = self.args.z_dfy
        Zqitiao2 = self.args.z_dp
        win1 = self.args.in_dfy
        win2 = self.args.in_dp
        Da1 = self.args.need_dfy
        Da2 = self.args.need_dp


        Zp2=Zqitiao1-Zqitiao2
        Zp1=Zqitiao2-Zqitiao1

        L1 = 13.64 * (Zp1 ** 0.5)
        L2 = 13.64 * (Zp2 ** 0.5)

        a1 = self.config["setting"]["values"]["dfy_a"]
        a2 = self.config["setting"]["values"]["dp_a"]
        b1 = self.config["setting"]["values"]["dfy_b"]
        b2 = self.config["setting"]["values"]["dp_b"]
        c1 = self.config["setting"]["values"]["dfy_c"]
        c2 = self.config["setting"]["values"]["dp_c"]
        d1 = self.config["setting"]["values"]["dfy_d"]
        d2 = self.config["setting"]["values"]["dp_d"]
        e1 = self.config["setting"]["values"]["dfy_e"]
        e2 = self.config["setting"]["values"]["dp_e"]
        f1 = self.config["setting"]["values"]["dfy_f"]
        f2 = self.config["setting"]["values"]["dp_f"]
        g1 = self.config["setting"]["values"]["dfy_g"]
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

        Zxunxian1 = self.config["setting"]["values"]["Zxunxian_dfy"]
        Zxunxian2 = self.config["setting"]["values"]["Zxunxian_dp"]
        Vxunxian1 = self.config["setting"]["values"]["Vxunxian_dfy"]
        Vxunxian2 = self.config["setting"]["values"]["Vxunxian_dp"]

        Zsishui1 = self.config["setting"]["values"]["Zsishui_dfy"]
        Zsishui2 = self.config["setting"]["values"]["Zsishui_dp"]
        Vsishui1 = self.config["setting"]["values"]["Vsishui_dfy"]
        Vsishui2 = self.config["setting"]["values"]["Vsishui_dp"]


        Vqitaio1 = aa1 * (Zqitiao1 ** 6) + bb1 * (Zqitiao1 ** 5) + cc1 * (Zqitiao1 ** 4) + dd1 * (Zqitiao1 ** 3) + ee1 * (Zqitiao1 ** 2) + ff1 * Zqitiao1 + gg1
        Vqitaio2 = aa2 * (Zqitiao2 ** 4) + bb2 * (Zqitiao2 ** 3) + cc2 * (Zqitiao2 ** 2) + dd2 * Zqitiao2 + ee2

        print(Vqitaio1, Vqitaio2)


        Z1 = Zqitiao1
        Z2 = Zqitiao2
        V1 = Vqitaio1
        V2 = Vqitaio2

        Z_control1 = self.args.control_dfy
        Z_control2 = self.args.control_dp

        Db1 = aa1 * (Z_control1 ** 6) + bb1 * (Z_control1 ** 5) + cc1 * (Z_control1 ** 4) + dd1 * (Z_control1 ** 3) + ee1 * (Z_control1 ** 2) + ff1 * Z_control1 + gg1

        Db2 = aa2 * (Z_control2 ** 4) + bb2 * (Z_control2 ** 3) + cc2 * (Z_control2 ** 2) + dd2 * Z_control2 + ee2

        print(Db1, Db2)

        # 开始计算大方郢调度过程
        V1 = V1 + win1
        Z1 = a1 * (V1**6) + b1 * (V1**5) + c1 * (V1**4) + d1 * (V1**3) + e1 * (V1**2) + f1 * V1 + g1
        Vsheng1 = V1 - Vsishui1  # 水库剩余水量，死库容之上
        Vneed1 = Da1

        if Vsheng1 >= Vneed1:
            V1 = V1 - Da1
            Z1 = a1 * (V1**6) + b1 * (V1**5) + c1 * (V1**4) + d1 * (V1**3) + e1 * (V1**2) + f1 * V1 + g1

            if Z1 >= Z_control1:
                if Z1 >= Zxunxian1[self.args.month - 1]:
                    qishui1 = V1 - Vxunxian1[self.args.month - 1]
                    V1 = Vxunxian1[self.args.month - 1]
                    Z1 = Zxunxian1[self.args.month - 1]
                else:
                    qishui1 = 0
                # print("大房郢供水量充足，无需补水")
                self.result["dafangying"] = {"flag": "0", "Z": Z1, "V": qishui1}
            else:
                # print("大房郢供水量较充足，为维持水位线需补水")
                RbZ1 = aa1 * (Z1 ** 6) + bb1 * (Z1 ** 5) + cc1 * (Z1 ** 4) + dd1 * (Z1 ** 3) + ee1 * (Z1 ** 2) + ff1 * Z1 + gg1
                Rb1 = Db1 - RbZ1
                Z1 = Z_control1
                self.result["dafangying"] = {"flag": "1", "Z": Z1, "V": Rb1}
        else:
            # print("大房郢供水量不足需补水")
            Rb1 = Da1 - Vsheng1 + Db1 - Vsishui1
            Z1 = Z_control1
            self.result["dafangying"] = {"flag": "2", "Z": Z1, "V": Rb1}

        # 开始计算董铺调度过程
        V2 = V2 + win2
        Z2 = a2 * (V2**6) + b2 * (V2**5) + c2 * (V2**4) + d2 * (V2**3) + e2 * (V2**2) + f2 * V2 + g2
        Vsheng2 = V2 - Vsishui2  # 水库剩余水量，死库容之上
        Vneed2 = Da2

        if Vsheng2 >= Vneed2:
            V2 = V2 - Da2
            Z2 = a2 * (V2**6) + b2 * (V2**5) + c2 * (V2**4) + d2 * (V2**3) + e2 * (V2**2) + f2 * V2 + g2

            if Z2 >= Z_control2:
                if Z2 >= Zxunxian2[self.args.month - 1]:
                    qishui2 = V2 - Vxunxian2[self.args.month - 1]
                    V2 = Vxunxian2[self.args.month - 1]
                    Z2 = Zxunxian2[self.args.month - 1]
                else:
                    qishui2 = 0
                # print("董铺水量充足，无需补水")
                self.result["dongpu"] = {"flag": "0", "Z": Z2, "V": qishui2}
            else:
                # print("董铺供水量较充足，为维持水位线需补水")
                RbZ2 = aa2 * (Z2 ** 4) + bb2 * (Z2 ** 3) + cc2 * (Z2 ** 2) + dd2 * Z2 + ee2
                Rb2 = Db2 - RbZ2
                Z2 = Z_control2
                self.result["dongpu"] = {"flag": "1", "Z": Z2, "V": Rb2}
        else:
            # print("董铺供水量不足需补水")
            Rb2 = Da2 - Vsheng2 + Db2 - Vsishui2
            Z2 = Z_control2
            self.result["dongpu"] = {"flag": "2", "Z": Z2, "V": Rb2}



if __name__ == "__main__":
    try:
        parser = argparse.ArgumentParser("Supply water")
        parser.add_argument("--z_dfy", type=float, default=25, help="z value of dafangying.")
        parser.add_argument("--z_dp", type=float, default=25.5, help="z value of dongpu.")
        parser.add_argument("--in_dfy", type=float, default=200, help="inflow of dafangying.")
        parser.add_argument("--in_dp", type=float, default=200, help="inflow of dongpu.")
        parser.add_argument("--need_dfy", type=float, default=300, help="need of dafangying.")
        parser.add_argument("--need_dp", type=float, default=300, help="need of dongpu.")
        parser.add_argument("--control_dfy", type=float, default=27, help="control z of dafangying.")
        parser.add_argument("--control_dp", type=float, default=27.5, help="control z of dongpu.")
        
        # parser.add_argument('--config', type=str, default='./src/algorithm/py/Supply/supply_water_config.json', help='config file')
        parser.add_argument('--config', type=str, default='./supply_water_config.json', help='config file')
        parser.add_argument("--batch", type=str, default="10000", help="batch id")
        parser.add_argument("--month", type=int, default=1, help="month id", choices=[1,2,3,4,5,6,7,8,9,10,11,12])

        args = parser.parse_args()
        
        print(args.__dict__)
        logging.info(args.__dict__)

        jsw = Supply(args=args)
        jsw.run()
        print(jsw.result)
        
        jsw.write_data_to_db()

        print("{\"complete\":true}")
        logging.info("{\"complete\":true}")
    except Exception as e:
        print(e)
        logging.error(e)
