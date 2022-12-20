import glob
import pyhdfs
import digitforce.aip.common.utils.config_helper as config_helper

hdfs_config = config_helper.get_module_config("hdfs")


class HdfsClient:
    def __init__(self, hosts=None, user_name="root"):
        if hosts is None:
            hosts = hdfs_config['hosts']
        self.hdfs_client = pyhdfs.HdfsClient(hosts=hosts, user_name=user_name)

    def get_client(self):
        return self.hdfs_client

    def list_dir(self, path):
        return self.hdfs_client.listdir(path)

    def list_status(self, path):
        return self.hdfs_client.list_status(path)

    def delete(self, path, recursive=True):
        return self.hdfs_client.delete(path, recursive=recursive)

    def copy_to_local(self, src: str, localdest: str):
        return self.hdfs_client.copy_to_local(src, localdest)

    def copy_from_local(self, localsrc: str, dest: str):
        return self.hdfs_client.copy_from_local(localsrc, dest)

    def mkdirs(self, path):
        return self.hdfs_client.mkdirs(path)

    def exists(self, path):
        return self.hdfs_client.exists(path)

    def mkdir_dirs(self, path):
        assert path.startswith("/")
        vals = path.split('/')
        _path = ""
        for _ in vals:
            _path += _ + "/"
            if not self.hdfs_client.exists(_path):
                self.mkdirs(_path)

    def copy_from_local_dir(self, local_dir, dest):
        self.mkdir_dirs(dest)
        files = glob.glob(local_dir)


hdfs_client = HdfsClient()
