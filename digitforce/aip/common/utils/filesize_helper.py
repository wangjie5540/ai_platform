import os


def size_format(size):
    if size < 1024:
        return '%i' % size + 'B'
    elif 1024 <= size < (1024 ** 2):
        return '%.1f' % float(size / (1024 ** 1)) + 'KB'
    elif (1024 ** 2) <= size < (1024 ** 3):
        return '%.1f' % float(size / (1024 ** 2)) + 'MB'
    elif (1024 ** 3) <= size < (1024 ** 4):
        return '%.1f' % float(size / (1024 ** 3)) + 'GB'
    elif (1024 ** 4) <= size:
        return '%.1f' % float(size / (1024 ** 4)) + 'TB'

def get_filesize(file: str, format: int):
    """
    获取文件大小
    :param file: 文件名
    :param format: 1：表示原始格式，不需要任何转换，字节为单位； 2：换算为容易理解的单位：B, KB, MB, GB, TB
    :return: 输出标准化后的文件大小格式
    """
    origin_size = os.stat(file)
    if format == 1:
        return str(origin_size)
    else:
        return size_format(origin_size)
