





def main():
    from common.logging_config import setup_console_log
    setup_console_log()
    inputfile = "click_event.csv"
    outputfile = "mf_train_dataset.csv"

    import pyhdfs
    cli = pyhdfs.HdfsClient(hosts="bigdata-server-08:9870", user_name='root')
    cli.mkdirs()
    cli.listdir('/user/ai/')
    cli.delete('/',recursive )
    cli.copy_from_local("hbase-site.xml", '/user/zhangxueren/test/hbase-site.xml')
    cli.copy_to_local("hbase-site.xml", '/user/zhangxueren/test/hbase-site.xml')


if __name__ == '__main__':
    main()
