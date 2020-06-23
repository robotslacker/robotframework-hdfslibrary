# -*- coding: UTF-8 -*-
import os
from pathlib import Path
from hdfs.client import Client
from hdfs.util import HdfsError
import traceback


class HDFSException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class RunHDFSCommand(object):
    # TEST SUITE 在suite中引用，只会实例化一次
    # 也就是说多test case都引用了这个类的方法，但是只有第一个test case调用的时候实例化
    ROBOT_LIBRARY_SCOPE = 'TEST SUITE'

    def __init__(self):
        self.m_HDFS_Handler = None
        self.m_HDFS_Protocal = None
        self.m_HDFS_NodePort = None
        self.m_HDFS_WebFSURL = None
        self.m_HDFS_WebFSDir = None

    # 创建目录
    def HDFS_mkdirs(self, hdfs_path):
        if self.m_HDFS_Handler is None:
            raise HDFSException("Please Connect HDFS first.")
        self.m_HDFS_Handler.makedirs(hdfs_path)

    # 删除hdfs文件
    def HDFS_Delete(self, hdfs_path):
        if self.m_HDFS_Handler is None:
            raise HDFSException("Please Connect HDFS first.")
        self.m_HDFS_Handler.delete(hdfs_path)

    # 上传文件到hdfs
    def HDFS_Upload(self, local_path, hdfs_path=""):
        if self.m_HDFS_Handler is None:
            raise HDFSException("Please Connect HDFS first.")
        try:
            self.m_HDFS_Handler.upload(hdfs_path, local_path, overwrite=True, cleanup=True)
        except HdfsError as he:
            raise HDFSException(repr(he))
        except Exception as oe:
            print('traceback.print_exc():\n%s' % traceback.print_exc())
            print('traceback.format_exc():\n%s' % traceback.format_exc())

    # 从hdfs获取文件到本地
    def HDFS_Download(self, hdfs_path="", local_path=""):
        if self.m_HDFS_Handler is None:
            raise HDFSException("Please Connect HDFS first.")
        self.m_HDFS_Handler.download(hdfs_path, local_path, overwrite=True)

    # 返回目录下的文件
    def HDFS_list(self, hdfs_path=""):
        # status
        #    True    同时返回状态信息
        #    False   不返回状态信息
        if self.m_HDFS_Handler is None:
            raise HDFSException("Please Connect HDFS first.")
        return self.m_HDFS_Handler.list(hdfs_path, status=True)

    # 切换当前目录
    # 其实就是重新连接了
    def HDFS_cd(self, hdfs_path):
        m_NewDirectory = Path(os.path.join(self.m_HDFS_WebFSDir, hdfs_path)).as_posix()
        self.m_HDFS_WebFSDir = m_NewDirectory
        self.m_HDFS_Handler = Client(self.m_HDFS_WebFSURL, self.m_HDFS_WebFSDir, session=None)

    # 连接HDFS
    def HDFS_Connnect(self, p_szURL):
        self.m_HDFS_Protocal = p_szURL.split("://")[0]
        self.m_HDFS_NodePort = p_szURL[len(self.m_HDFS_Protocal) + 3:].split("/")[0]
        self.m_HDFS_WebFSURL = self.m_HDFS_Protocal + "://" + self.m_HDFS_NodePort
        self.m_HDFS_WebFSDir = p_szURL[len(self.m_HDFS_WebFSURL):]
        self.m_HDFS_Handler = Client(self.m_HDFS_WebFSURL,
                                self.m_HDFS_WebFSDir,
                                proxy=None, session=None)

if __name__ == '__main__':
    myCompare = RunHDFSCommand()

    myCompare.HDFS_Connnect("http://node64:50070/node62/jenkins/work")
    myCompare.HDFS_cd("/node62/jenkins/work")
    mylist = myCompare.HDFS_list()
    for row in mylist:
        print("Row = " + str(row))
    # myCompare.HDFS_Download("test1.sql")
    myCompare.HDFS_Upload("tag.txt")
