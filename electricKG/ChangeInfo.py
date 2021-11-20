# coding: utf-8

import os
import json
from py2neo import Graph, Node, Relationship


class Changer:
    def __init__(self):
        self.g = Graph(
            host="localhost",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            http_port=7474,  # neo4j 服务器监听的端口号
            user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
            password="123456")

    def change_powerPlant(self, old_jsons, data_jsons):
        for old_json, data_json in zip(old_jsons, data_jsons):
            query = "match(p) where p.name='%s' set p.name='%s'," \
                    "p.region='%s',p.powerPlant_type='%s',p.highest_voltage_level='%s'," \
                    "p.elevation='%s'" % (old_json['电厂名称'], data_json['电厂名称'], data_json['所属地区'],
                                          data_json['电厂类型'], data_json['最高电压等级'], data_json['海拔'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def change_transformerSubstation(self, old_jsons, data_jsons):
        for old_json, data_json in zip(old_jsons, data_jsons):
            query = "match(p) where p.name='%s' set p.name='%s'," \
                    "p.region='%s',p.transformerSubstation_type='%s',p.DC_voltage_level='%s'," \
                    "p.highest_voltage_level='%s',p.elevation='%s'" % (old_json['变电站名称'], data_json['变电站名称'],
                                                                       data_json['所属地区'], data_json['变电站类型'],
                                                                       data_json['直流电压等级'], data_json['最高电压等级'],
                                                                       data_json['海拔'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def change_unit(self, old_jsons, data_jsons):
        for old_json, data_json in zip(old_jsons, data_jsons):
            query = "match(p)-[r:possess]->(q) where p.name='%s'and q.name='%s' set q.name='%s'," \
                    "q.date_of_delivery='%s',q.date_of_back_luck='%s',q.grid_connection_level='%s'," \
                    "q.machine_rated_voltage='%s',q.rated_capacity='%s'" % (old_json['所属发电厂'],
                                                                            old_json['机组名称'], data_json['机组名称'],
                                                                            data_json['投运日期'], data_json['退运日期'],
                                                                            data_json['并入电网等级'], data_json['机端额定电压'],
                                                                            data_json['额定容量'])

            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def change_busbar(self, old_jsons, data_jsons):
        for old_json, data_json in zip(old_jsons, data_jsons):
            query = "match(p)-[r:possess]->(q) where p.name='%s'and q.name='%s' set q.name='%s'," \
                    "q.voltage_level='%s',q.date_of_delivery='%s',q.date_of_back_luck='%s'" % (old_json['所属厂站'],
                                                                                               old_json['母线名称'],
                                                                                               data_json['母线名称'],
                                                                                               data_json['电压等级'],
                                                                                               data_json['投运日期'],
                                                                                               data_json['退运日期'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def change_transformer(self, old_jsons, data_jsons):
        for old_json, data_json in zip(old_jsons, data_jsons):
            query = "match(p)-[r:possess]->(q) where p.name='%s'and q.name='%s' set q.name='%s'," \
                    "q.rated_power='%s',q.rate_voltage='%s',q.running_state='%s',q.date_of_delivery='%s'," \
                    "q.date_of_back_luck='%s',q.fault_information='%s',q.manufacturer='%s'," \
                    "q.rated_capacity='%s'" % (old_json['所属厂站'], old_json['变压器名称'], data_json['变压器名称'],
                                               data_json['额定功率'], data_json['额定电压'], data_json['运行状态'],
                                               data_json['投运日期'], data_json['退运日期'], data_json['缺陷、故障信息'],
                                               data_json['生产厂家'], data_json['额定容量'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def change_line(self, old_jsons, data_jsons):
        for old_json, data_json in zip(old_jsons, data_jsons):
            print(old_json, data_json)
            query = "match(p{name:'%s'})-[rel:line{name:'%s',length:'%s',voltage_level:'%s'}]->(q{name:'%s'})" \
                    "set rel.name='%s',rel.length='%s',rel.voltage_level='%s'" \
                    % (old_json['起点厂站'], old_json['线路名称'], old_json['线路长度'], old_json['电压等级'], old_json['终点厂站'],
                       data_json['线路名称'], data_json['线路长度'], data_json['电压等级'])

            try:
                self.g.run(query)
            except Exception as e:
                print(e)


if __name__ == '__main__':
    json1 = json.loads('[{"电厂名称":"沈海厂","所属地区":"沈阳","电厂类型":"火电","调度机构":"辽宁","最高电压等级":220,"海拔":"1.0000"}]')
    json2 = json.loads('[{"电厂名称":"沈海厂xx","所属地区":"沈阳","电厂类型":"火电","调度机构":"辽宁","最高电压等级":220,"海拔":"1.0000"}]')
    handler = Changer()
    handler.change_powerPlant(json1, json2)
