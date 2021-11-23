# coding: utf-8

import os
import json
from py2neo import Graph, Node, Relationship


class GetData:
    def __init__(self):
        self.g = Graph(
            host="localhost",  # neo4j 搭载服务器的ip地址，ifconfig可获取到
            http_port=7474,  # neo4j 服务器监听的端口号
            user="neo4j",  # 数据库user name，如果没有更改过，应该是neo4j
            password="123456")

    def get_data(self):
        sess = 'MATCH (n)-[r]->(m) RETURN id(n) as source, labels(n) as source_labels, ' \
               'properties(n) as source_attrs, id(m) as target, labels(m) as target_labels, ' \
               'properties(m) as target_attrs, id(r) as link, type(r) as r_type, properties(r) as r_attrs '
        result = self.g.run(sess)
        nodes = dict()
        links = dict()
        for re in result:
            #print(str(re['source']), re['source_labels'], re['source_attrs'])
            nodes[str(re['source'])] = {'labels': re['source_labels'][0], 'attrs': re['source_attrs']}
            nodes[str(re['target'])] = {'labels': re['target_labels'][0], 'attrs': re['target_attrs']}
            links[str(re['link'])] = {'type': re['r_type'][0], 'attrs': re['r_attrs'],
                                      'source': str(re['source']), 'target': str(re['target'])}
        return nodes, links


if __name__ == '__main__':
    gd = GetData()
    ns, ls = gd.get_data()
    # node = dict()
    # node[11] = {'label': 'transformer', 'attrs': {'a': 1, 'b': 2}}
    # #node.update({11: {'label': 'transformer', 'attrs': {'a': 1, 'b': 2}}})
    # print(node)
