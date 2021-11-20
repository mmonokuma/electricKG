# coding: utf-8

import os
import json
from py2neo import Graph, Node, Relationship


class Deleter:
    def __init__(self):
        self.g = Graph(
            host="localhost",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            http_port=7474,  # neo4j 服务器监听的端口号
            user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
            password="123456")

    def delete_province(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p) where p.name='%s' DETACH DELETE p" % (data_json['调度机构'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_powerPlant(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p) where p.name='%s' and p.region='%s' DETACH DELETE p" % (data_json['电厂名称'],
                                                                                      data_json['所属地区'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_transformerSubstation(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p) where p.name='%s' and p.region='%s' DETACH DELETE p" % (data_json['变电站名称'],
                                                                                      data_json['所属地区'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_unit(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p)-[r:possess]->(q) where p.name='%s'and q.name='%s' DETACH DELETE q" % (
                data_json['所属发电厂'], data_json['机组名称'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_busbar(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p)-[r:possess]->(q) where p.name='%s'and q.name='%s' DETACH DELETE q" % (
                data_json['所属厂站'], data_json['母线名称'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_transformer(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p)-[r:possess]->(q) where p.name='%s'and q.name='%s' DETACH DELETE q" % (
                data_json['所属厂站'], data_json['变压器名称'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_schedule(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p{name:'%s'})-[rel1:schedule]->(q{name:'%s'})" \
                    "(q{name:'%s'}) DELETE rel" % (data_json['调度机构'], data_json['电厂名称'])
            # query = "match(p{name:'%s'})-[rel1:schedule]->(q{name:'%s'})" \
                    # "(q{name:'%s'}) DELETE rel" % (data_json['调度机构'], data_json['机组名称'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_line(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p{name:'%s'})-[rel:line{name:'%s',length:'%s',voltage_level:'%s'}]->" \
                    "(q{name:'%s'}) DELETE rel" % (data_json['起点厂站'], data_json['线路名称'], data_json['线路长度'],
                                                   data_json['电压等级'], data_json['终点厂站'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)

    def delete_recording(self, data_jsons):
        for data_json in data_jsons:
            query = "match(p{name:'%s'})-[rel1:possess]->(q{name:'%s'})-[rel2:recording{name:'维修记录'}]->(r:检修{" \
                    "end_plant_station:'%s', department:'%s',content:'%s', impact:'%s', start_time:'%s', " \
                    "end_time:'%s'})  DETACH DELETE r" % (data_json['所属厂站首端厂站'], data_json['检修设备'],
                                                          data_json['末端厂站'], data_json['工作单位'], data_json['检修内容'],
                                                          data_json['影响情况'], data_json['实际开始时间'], data_json['实际结束时间'])
            try:
                self.g.run(query)
            except Exception as e:
                print(e)



#删除设备必须保证记录删除，删除上级机构得保证下属机构删除


if __name__ == '__main__':
    jsons = json.loads('[{"机组名称":"1号机","所属发电厂":"沈海厂","调度机构":"辽宁","机端额定电压":220,"额定容量":200,"并入电网等级":"-","投运日期":"1990-11-05","退运日期":""}]')
    handler = Deleter()
    handler.delete_unit(jsons)
