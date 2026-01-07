# -*- coding: utf-8 -*-
from kazoo.client import KazooClient, KazooState

class ZookeeperSession(object):

    def __init__(self, locations, name_prefix, root_prefix='/frontera'):
        self._zk = KazooClient(hosts=locations)
        self._zk.add_listener(self.zookeeper_listener)
        self._zk.start()
        self.root_prefix = root_prefix
        self.znode_path = self._zk.create("%s/%s" % (self.root_prefix, name_prefix),
                                          ephemeral=True,
                                          sequence=True,
                                          makepath=True)

    def zookeeper_listener(self, state):
        if state == KazooState.LOST:
            # Register somewhere that the session was lost
            pass
        elif state == KazooState.SUSPENDED:
            # Handle being disconnected from Zookeeper
            pass
        else:
            # Handle being connected/reconnected to Zookeeper
            pass

    def set(self, value):
        self._zk.set(self.znode_path, value)

    def get_workers(self, prefix='', exclude_prefix=''):
        for znode_name in self._zk.get_children(self.root_prefix):
            if prefix and not znode_name.startswith(prefix):
                continue
            if exclude_prefix and znode_name.startswith(exclude_prefix):
                continue
            location, _ = self._zk.get(self.root_prefix+"/"+znode_name)
            yield location
