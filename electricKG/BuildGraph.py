# coding: utf-8

import os
import json
from py2neo import Graph, Node, Relationship


class ElectricGraph:
    def __init__(self):
        cur_dir = '/'.join(os.path.abspath(__file__).split('/')[:-1])
        self.PowerPlant_path = os.path.join(cur_dir, 'data/电厂.json')
        self.TransformerSubstation_path = os.path.join(cur_dir, 'data/变电站.json')
        self.Transformer_path = os.path.join(cur_dir, 'data/变压器.json')
        self.BusBar_path = os.path.join(cur_dir, 'data/母线.json')
        self.Unit_path = os.path.join(cur_dir, 'data/机组.json')
        self.Line_path = os.path.join(cur_dir, 'data/线路.json')
        self.Overhaul_path = os.path.join(cur_dir, 'data/检修.json')
        self.PowerSupplyChanges_path = os.path.join(cur_dir, 'data/电源变化.json')
        self.g = Graph(
            host="localhost",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            http_port=7474,  # neo4j 服务器监听的端口号
            user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
            password="123456")
        self.g.delete_all()

    def read_nodes(self):
        province_set = []
        power_plant_set = []
        transformer_substation_set = []
        transformer_set = []
        busbar_set = []
        unit_set = []
        # overhaul_set = [] # 记录在rel_recording_set中因为没有唯一标识
        # power_supply_changes_set = [] # 记录在rel_recording_set中因为没有唯一标识
        rel_schedule_set = []  # 调度机构和电厂的关系
        rel_line_set = []  # 电厂、变电站之间的线路关系
        rel_possess_set = []  # 电厂、变电站持有变电器、母线关系
        rel_recording_set = []  # 检修和电源变化 电源变化的‘机组名称’需要分开成‘电厂’和‘设备’两个字段，且中间有只有‘设备’信息的数据

        # 获取电厂节点，调度机构节点，电厂_调度关系
        try:
            power_plant_str = ''
            for data in open(self.PowerPlant_path, encoding='utf-8'):
                power_plant_str += data
            data_jsons = json.loads(power_plant_str)
            for data_json in data_jsons:
                power_plant_dict = dict()
                province_set.append(data_json['调度机构'])
                power_plant_dict['电厂名称'] = data_json['电厂名称']
                power_plant_dict['所属地区'] = data_json['所属地区']
                power_plant_dict['电厂类型'] = data_json['电厂类型']
                power_plant_dict['最高电压等级'] = data_json['最高电压等级']
                power_plant_dict['海拔'] = data_json['海拔']
                power_plant_set.append(power_plant_dict)
                rel_schedule_set.append([data_json['调度机构'], data_json['电厂名称']])
        except Exception as e:
            print(e)

        # 获取变电站节点
        try:
            transformer_substation_str = ''
            for data in open(self.TransformerSubstation_path, encoding='utf-8'):
                transformer_substation_str += data
            data_jsons = json.loads(transformer_substation_str)
            for data_json in data_jsons:
                transformer_substation_dict = dict()
                transformer_substation_dict['变电站名称'] = data_json['变电站名称']
                transformer_substation_dict['所属地区'] = data_json['所属地区']
                transformer_substation_dict['变电站类型'] = data_json['变电站类型']
                transformer_substation_dict['直流电压等级'] = data_json['直流电压等级']
                transformer_substation_dict['最高电压等级'] = data_json['最高电压等级']
                transformer_substation_dict['海拔'] = data_json['海拔']
                transformer_substation_set.append(transformer_substation_dict)
        except Exception as e:
            print(e)

        # 获取变压器节点，所属厂站关系
        try:
            transformer_str = ''
            machine_type = '变压器'
            for data in open(self.Transformer_path, encoding='utf-8'):
                transformer_str += data
            data_jsons = json.loads(transformer_str)
            for data_json in data_jsons:
                transformer_dict = dict()
                transformer_dict['变压器名称'] = data_json['变压器名称']
                transformer_dict['额定功率'] = data_json['额定功率']
                transformer_dict['额定电压'] = data_json['额定电压']
                transformer_dict['运行状态'] = data_json['运行状态']
                transformer_dict['投运日期'] = data_json['投运日期']
                transformer_dict['退运日期'] = data_json['退运日期']
                transformer_dict['缺陷、故障信息'] = data_json['缺陷、故障信息']
                transformer_dict['生产厂家'] = data_json['生产厂家']
                transformer_dict['额定容量'] = data_json['额定容量']
                transformer_set.append(transformer_dict)
                rel_possess_set.append([data_json['所属厂站'], data_json['变压器名称'], transformer_dict, machine_type])
        except Exception as e:
            print(e)

        # 获取母线节点，所属厂站关系
        try:
            busbar_str = ''
            machine_type = '母线'
            for data in open(self.BusBar_path, encoding='utf-8'):
                busbar_str += data
            data_jsons = json.loads(busbar_str)
            for data_json in data_jsons:
                busbar_dict = dict()
                busbar_dict['母线名称'] = data_json['母线名称']
                busbar_dict['电压等级'] = data_json['电压等级']
                busbar_dict['投运日期'] = data_json['投运日期']
                busbar_dict['退运日期'] = data_json['退运日期']
                busbar_set.append(busbar_dict)
                rel_possess_set.append([data_json['所属厂站'], data_json['母线名称'], busbar_dict, machine_type])
        except Exception as e:
            print(e)

        # 获取机组节点，所属厂站关系
        try:
            unit_str = ''
            machine_type = '机组'
            for data in open(self.Unit_path, encoding='utf-8'):
                unit_str += data
            data_jsons = json.loads(unit_str)
            for data_json in data_jsons:
                unit_dict = dict()
                unit_dict['机组名称'] = data_json['机组名称']
                unit_dict['机端额定电压'] = data_json['机端额定电压']
                unit_dict['额定容量'] = data_json['额定容量']
                unit_dict['并入电网等级'] = data_json['并入电网等级']
                unit_dict['投运日期'] = data_json['投运日期']
                unit_dict['退运日期'] = data_json['退运日期']
                unit_set.append(unit_dict)
                rel_possess_set.append([data_json['所属发电厂'], data_json['机组名称'], unit_dict, machine_type])
                rel_schedule_set.append([data_json['调度机构'], data_json['机组名称']])
        except Exception as e:
            print(e)

        # 获取检修记录节点，检修机器——记录关系
        try:
            overhaul_str = ''
            for data in open(self.Overhaul_path, encoding='utf-8'):
                overhaul_str += data
            data_jsons = json.loads(overhaul_str)
            for data_json in data_jsons:
                overhaul_dict = dict()
                overhaul_dict['检修设备'] = data_json['检修设备']
                overhaul_dict['所属厂站首端厂站'] = data_json['所属厂站首端厂站']
                overhaul_dict['末端厂站'] = data_json['末端厂站']
                overhaul_dict['工作单位'] = data_json['工作单位']
                overhaul_dict['检修内容'] = data_json['检修内容']
                overhaul_dict['影响情况'] = data_json['影响情况']
                overhaul_dict['实际开始时间'] = data_json['实际开始时间']
                overhaul_dict['实际结束时间'] = data_json['实际结束时间']
                rel_recording_set.append([data_json['所属厂站首端厂站'], data_json['检修设备'], overhaul_dict])
        except Exception as e:
            print(e)

        # 获取检修记录节点，检修机器——记录关系
        try:
            line_str = ''
            for data in open(self.Line_path, encoding='utf-8'):
                line_str += data
            data_jsons = json.loads(line_str)
            for data_json in data_jsons:
                line_dict = dict()
                line_dict['线路名称'] = data_json['线路名称']
                line_dict['线路长度'] = data_json['线路长度']
                line_dict['电压等级'] = data_json['电压等级']
                rel_line_set.append([data_json['起点厂站'], data_json['终点厂站'], line_dict])
        except Exception as e:
            print(e)

        return set(province_set), power_plant_set, transformer_substation_set, transformer_set, \
               busbar_set, unit_set, rel_schedule_set, rel_line_set, rel_possess_set, rel_recording_set

    def create_province_nodes(self, provinces):
        count = 0
        for province in provinces:
            node = Node("调度机构", name=province)
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_powerPlant_nodes(self, power_plants):
        count = 0
        for power_plant in power_plants:
            node = Node("电厂", name=power_plant['电厂名称'], region=power_plant['所属地区'], powerPlant_type=power_plant['电厂类型'],
                        highest_voltage_level=power_plant['最高电压等级'], elevation=power_plant['海拔'])
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_transformerSubstation_nodes(self, transformer_substations):
        count = 0
        for transformer_substation in transformer_substations:
            node = Node("变电站", name=transformer_substation['变电站名称'], region=transformer_substation['所属地区'],
                        transformerSubstation_type=transformer_substation['变电站类型'],
                        DC_voltage_level=transformer_substation['直流电压等级'],
                        highest_voltage_level=transformer_substation['最高电压等级'],
                        elevation=transformer_substation['海拔'])
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_transformer_nodes(self, transformers):
        count = 0
        for transformer in transformers:
            node = Node("变压器", name=transformer['变压器名称'], rated_power=transformer['额定功率'],
                        rate_voltage=transformer['额定电压'], running_state=transformer['运行状态'],
                        date_of_delivery=transformer['投运日期'], date_of_back_luck=transformer['退运日期'],
                        fault_information=transformer['缺陷、故障信息'], manufacturer=transformer['生产厂家'],
                        rated_capacity=transformer['额定容量'])
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_busbar_nodes(self, busbars):
        count = 0
        for busbar in busbars:
            node = Node("母线", name=busbar['母线名称'], voltage_level=busbar['电压等级'],
                        date_of_delivery=busbar['投运日期'], date_of_back_luck=busbar['退运日期'])
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_unit_nodes(self, units):
        count = 0
        for unit in units:
            node = Node("机组", name=unit['机组名称'], date_of_delivery=unit['投运日期'],
                        date_of_back_luck=unit['退运日期'], grid_connection_level=unit['并入电网等级'],
                        machine_rated_voltage=unit['机端额定电压'], rated_capacity=unit['额定容量'])
            self.g.create(node)
            count += 1
            # print(count)
        return

    def create_schedule_relationship(self, rel_type, rel_name, edges):
        count = 0
        # 去重处理edge
        set_edges = []
        for edge in edges:
            # set_edges.append('###'.join('%s' %id for id in edge))
            set_edges.append('###'.join(edge))
        # all = len(set(set_edges))
        # print(all)
        for edge in set(set_edges):
            edge = edge.split('###')
            p = edge[0]
            q = edge[1]
            query = "match(p),(q) where p.name='%s'and q.name='%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                p, q, rel_type, rel_name)
            try:
                self.g.run(query)
                count += 1
                print(rel_type, count)
            except Exception as e:
                print(e)
        return

    def create_possess_relationship(self, edges):
        count = 0
        for edge in edges:
            p = edge[0]
            q = edge[1]
            r = edge[2]
            s = edge[3]
            if s == '机组':
                node = Node("机组", name=r['机组名称'], date_of_delivery=r['投运日期'],
                            date_of_back_luck=r['退运日期'], grid_connection_level=r['并入电网等级'],
                            machine_rated_voltage=r['机端额定电压'], rated_capacity=r['额定容量'],
                            machine_type=s)
                holder = self.g.nodes.match(name=p).first()
                rel_possess = Relationship(holder, "possess", node)
            elif s == '母线':
                node = Node("母线", name=r['母线名称'], voltage_level=r['电压等级'],
                            date_of_delivery=r['投运日期'], date_of_back_luck=r['退运日期'],
                            machine_type=s)
                holder = self.g.nodes.match(name=p).first()
                rel_possess = Relationship(holder, "possess", node)
            else:
                node = Node("变压器", name=r['变压器名称'], rated_power=r['额定功率'],
                            rate_voltage=r['额定电压'], running_state=r['运行状态'],
                            date_of_delivery=r['投运日期'], date_of_back_luck=r['退运日期'],
                            fault_information=r['缺陷、故障信息'], manufacturer=r['生产厂家'],
                            rated_capacity=r['额定容量'])
                holder = self.g.nodes.match(name=p).first()
                rel_possess = Relationship(holder, 'possess', node)
            try:
                self.g.create(rel_possess)
                count += 1
                print('possess', count)
            except Exception as e:
                print(e)
        return

    def create_line_relationship(self, edges):
        count = 0
        for edge in edges:
            p = edge[0]
            q = edge[1]
            r = edge[2]
            query = "match(p),(q) where p.name='%s'and q.name='%s' " \
                    "create (p)-[rel:line{name:'%s',length:'%s',voltage_level:'%s'}]->(q)" \
                    % (p, q, r['线路名称'], r['线路长度'], r['电压等级'])
            try:
                self.g.run(query)
                count += 1
                print('线路', count)
            except Exception as e:
                print(e)
        return

    def create_recording_relationship(self, edges):
        count = 0
        for edge in edges:
            p = edge[0]
            q = edge[1]
            r = edge[2]
            query = "match(p)-[rel1:possess]->(q) where p.name='%s'and q.name='%s' " \
                    "create (q)-[rel2:recording{name:'维修记录'}]->(r:检修{end_plant_station:'%s', department:'%s'," \
                    " content:'%s', impact:'%s', start_time:'%s', end_time:'%s'})" \
                    % (p, q, r['末端厂站'], r['工作单位'], r['检修内容'], r['影响情况'], r['实际开始时间'], r['实际结束时间'])
            try:
                self.g.run(query)
                count += 1
                print('维修记录', count)
            except Exception as e:
                print(e)
        return

    def create_graph(self):
        provinces, power_plants, transformer_substations, transformers, busbars, units, \
        rel_schedules, rel_lines, rel_possesss, rel_recordings = self.read_nodes()
        self.create_province_nodes(provinces)
        self.create_powerPlant_nodes(power_plants)
        self.create_transformerSubstation_nodes(transformer_substations)
        # self.create_transformer_nodes(transformers)
        # self.create_busbar_nodes(busbars)
        # self.create_unit_nodes(units)
        self.create_schedule_relationship('schedule', '调度', rel_schedules)
        self.create_possess_relationship(rel_possesss)
        self.create_line_relationship(rel_lines)
        self.create_recording_relationship(rel_recordings)


if __name__ == '__main__':
    handler = ElectricGraph()
    handler.create_graph()
