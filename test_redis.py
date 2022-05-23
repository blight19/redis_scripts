import redis
import random
import string
import logging
from rediscluster import RedisCluster

logger = logging.getLogger("RedisTest")
formatter = logging.Formatter("%(asctime)s -  %(levelname)s: %(message)s")
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)
logger.setLevel(logging.INFO)

REPLICATION_TEST = True  # 是否进行读写测试/主从复制测试
CLUSTER_TESTKEY_NUM = 20  # CLUSTER模式读写测试KEY的数量
EX = 1  # 测试KEY的过期时间

CLUSTER = 1
SENTINEL = 2
REPLICATION = 3
SINGLE = 4
MODE_DICT = {
    CLUSTER: "集群模式",
    SENTINEL: "哨兵模式",
    REPLICATION: "主从复制模式",
    SINGLE: "单机模式",
}


class RedisDetector:
    def __init__(self, host, port=6379, password=None, port_sentinel=26379, sentinel_password=None):

        self.host = host
        self.password = password
        self.port = port
        self.port_sentinel = port_sentinel
        self.sentinel_password = sentinel_password
        self.mode = 0
        self.conn = self.get_conn()

    def get_conn(self):
        return redis.Redis(host=self.host, port=self.port, password=self.password, decode_responses=True)

    def ping(self):
        try:
            self.conn.ping()
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"【{self.host}:{self.port}】连接失败！{e}")
            exit()
        else:
            logger.info(f"【{self.host}:{self.port}】连接成功！")

    def read_type(self):
        # 获取高可用方式
        info = self.conn.info()
        if info.get("role") == "slave":
            logger.warning("此节点为从节点，请输入主节点")
            exit()
        cluster_enabled = info.get("cluster_enabled")
        if cluster_enabled == 1:
            return CLUSTER
        elif info.get("role") == "master":
            if info.get("connected_slaves") == 0:
                return SINGLE
            elif self.__is_sentinel():
                return SENTINEL
            else:
                return REPLICATION

    def __is_sentinel(self):
        # 判断是否为哨兵模式
        conn = redis.Redis(host=self.host, port=self.port_sentinel, password=self.sentinel_password)
        try:
            conn.ping()
        except redis.exceptions.AuthenticationError:
            conn = redis.Redis(host=self.host, port=self.port_sentinel, password=self.password)
            conn.ping()

        except redis.exceptions.ConnectionError:
            return False
        return True

    def detect_read_write(self):
        try:
            letter_expected = self.gen_random_letter()
            self.conn.set("test_key", letter_expected, ex=EX)
            letter_actural = self.conn.get("test_key")
        except Exception as e:
            logger.warning(f"读写实验失败！{e}", )
            return False
        if letter_actural != letter_expected:
            logger.warning("读写不一致，失败！")
            return False
        logger.info("读写实验成功!")
        return letter_actural

    def replica_test(self, client, letter_expected, **kwargs):

        addr = client.get("addr")
        host = addr.split(":")[0]
        conn = redis.Redis(host=host, port=self.port, password=self.password, decode_responses=True)
        letter_actural = conn.get("test_key")
        if letter_actural != letter_expected:
            logger.info(f"【{host}】 主从复制不一致！")
            return
        else:
            logger.info(f"【{host}】 主从复制通过！")

    def detect(self):
        self.ping()
        self.mode = self.read_type()
        self.print_mode(self.mode)
        if self.mode == CLUSTER:
            clu = RedisClusterDetector(host=self.host, port=self.port, password=self.password)
            clu.detect()
            return
        slaves = self.conn.client_list("replica")
        logger.info(f"共有从节点【{len(slaves)}】个")
        if not REPLICATION_TEST:
            return
        read_write_test = self.detect_read_write()

        if read_write_test:
            if self.mode == SINGLE:
                logger.info("测试完毕")
                return
            else:
                for client in slaves:
                    self.replica_test(client, letter_expected=read_write_test)
        else:
            return

    @staticmethod
    def print_mode(mode):
        logger.info(f"检测状态成功,状态为【{MODE_DICT.get(mode)}】")

    @staticmethod
    def gen_random_letter():
        return ''.join(random.sample(string.ascii_letters, 50))

    def print_info(self):
        print(self.conn.info())


class RedisClusterDetector(RedisDetector):
    def __init__(self, host, port=6379, password=None, port_sentinel=26379, sentinel_password=None):
        super(RedisClusterDetector, self).__init__(host, port, password, port_sentinel, sentinel_password)
        self.conn = self.get_conn()

    def get_conn(self):
        return RedisCluster(host=self.host, port=self.port, password=self.password, decode_responses=True,
                            skip_full_coverage_check=True)

    @staticmethod
    def print_master_info(infos: dict):
        for node, info in infos.items():
            if info["cluster_state"] != "ok":
                logger.warning(f"节点【{node}】状态异常！")
            else:
                logger.info(f"节点【{node}】状态正常")
            if info["cluster_slots_assigned"] != 16384:
                logger.warning(f"节点【{node}】检测到槽缺失！")

    def replica_test(self, client: redis.Redis, letter_expected, **kwargs):
        key = kwargs.get("key")
        master_addr = kwargs.get("master_addr")
        slave_addr = kwargs.get("slave_addr")
        client.readonly()
        letter_actural = client.get(key)
        if letter_actural != letter_expected:
            logger.info(f"主【{master_addr}】从【{slave_addr}】复制不一致！")
            return
        else:
            logger.info(f"主【{master_addr}】从【{slave_addr}】复制通过！")

    def run_replica_test(self):

        for node in self.nodes_map.values():
            host, port = node["addr"].split(":")
            if "slave" not in node:
                logger.warning(f"{node['addr']}没有从节点")
                continue
            host_s, port_s = node["slave"].split(":")
            conn = redis.Redis(host=host, port=port, password=self.password, decode_responses=True)
            key = conn.scan(cursor=0, match="test_key*", count=1)[1][0]
            letter_expected = conn.get(key)
            client = redis.Redis(host=host_s, port=port_s, password=self.password, decode_responses=True)
            self.replica_test(client=client, letter_expected=letter_expected,
                              key=key, master_addr=node["addr"], slave_addr=node["slave"])

    @property
    def nodes_map(self):
        nodes = self.conn.cluster_nodes()
        nodes_map = {}
        for node in nodes:
            del node["slots"]
            if "master" in node['flags']:
                nodes_map[node['id']] = {
                    "addr": f"{node['host']}:{node['port']}"
                }
        for node in nodes:
            if "slave" in node["flags"]:
                master_id = node["master"]
                nodes_map[master_id]['slave'] = f"{node['host']}:{node['port']}"
        return nodes_map

    def detect_read_write_cluster(self):
        random_letter_map = {}

        for i in range(CLUSTER_TESTKEY_NUM):
            random_letter = self.gen_random_letter()
            key = f"test_key_{i}"
            random_letter_map[key] = random_letter
            self.conn.set(key, random_letter, ex=EX)  # 由于插入较多 设置一分钟过期时间
            if (i + 1) % 5 == 0:
                logger.info(f"插入数据测试{i + 1}/{CLUSTER_TESTKEY_NUM}成功...")
        return True

    def detect(self):
        self.print_master_info(self.conn.cluster_info())
        self.detect_read_write_cluster()
        self.run_replica_test()


if __name__ == '__main__':

    a = RedisDetector(host="10.10.82.44", port=6379, password="1234.com")
    a.detect()
