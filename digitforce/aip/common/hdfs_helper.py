import glob

import pyhdfs

from digitforce.aip.common.config.bigdata_config import HDFS_HOST, HDFS_PORT


class HdfsClient:
    def __init__(self, hosts=None, user_name="root"):
        if hosts is None:
            hosts = []
        self.hosts = hosts
        self.user_name = user_name
        self.hdfs_client = None

    def get_hdfs_client(self):
        if self.hdfs_client is None:
            self.hdfs_client = pyhdfs.HdfsClient(hosts=self.hosts, user_name=self.user_name)
        return self.hdfs_client

    def list_dir(self, path):
        return self.get_hdfs_client().listdir(path)

    def list_status(self, path):
        return self.get_hdfs_client().list_status(path)

    def delete(self, path, recursive=True):
        return self.get_hdfs_client().delete(path, recursive=recursive)

    def copy_to_local(self, src: str, localdest: str):
        return self.get_hdfs_client().copy_to_local(src, localdest)

    def copy_from_local(self, localsrc: str, dest: str):
        return self.get_hdfs_client().copy_from_local(localsrc, dest)

    def mkdirs(self, path):
        return self.get_hdfs_client().mkdirs(path)

    def exists(self, path):
        return self.get_hdfs_client().exists(path)

    def mkdir_dirs(self, path):
        assert path.startswith("/")
        vals = path.split('/')
        _path = ""
        for _ in vals:
            _path += _ + "/"
            if not self.get_hdfs_client().exists(_path):
                self.mkdirs(_path)

    def copy_from_local_dir(self, local_dir, dest):
        self.mkdir_dirs(dest)
        files = glob.glob(local_dir)


dg_hdfs_client = HdfsClient(hosts=[f"{HDFS_HOST}:{HDFS_PORT}"])


def main():
    dg_hdfs_client.mkdir_dirs("/user/zhangxueren/host_1/hosts_2/host_3")
    print(dg_hdfs_client.exists("/user/zhangxueren/host_1/hosts_2/host_3"))
    print(dg_hdfs_client.list_status("/user/zhangxueren/host_1/hosts_2/"))
    # print(hdfs_client.list_dir("/user/zhangxueren"))
    # print(hdfs_client.list_status("/user/zhangxueren"))


if __name__ == '__main__':
    main()
