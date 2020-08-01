# -*- coding: UTF-8 -*-
import os
import fnmatch
from pathlib import Path
from hdfs.client import Client
from hdfs import InsecureClient
from hdfs.util import HdfsError
import traceback
from glob import glob
from robot.api import logger


class HDFSException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class RunHDFSCommand(object):
    # TEST SUITE 在suite中引用，只会实例化一次
    # 也就是说多test case都引用了这个类的方法，但是只有第一个test case调用的时候实例化
    ROBOT_LIBRARY_SCOPE = 'TEST SUITE'

    def __init__(self):
        self.__m_HDFS_Handler__ = None
        self.__m_HDFS_Protocal__ = None
        self.__m_HDFS_NodePort__ = None
        self.__m_HDFS_WebFSURL__ = None
        self.__m_HDFS_WebFSDir__ = None
        self.__m_HDFS_ConnectUser = None

    def HDFS_mkdirs(self, hdfs_path):
        """ 创建目录 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSException("Please Connect HDFS first.")
        self.__m_HDFS_Handler__.makedirs(os.path.join(self.__m_HDFS_WebFSDir__, hdfs_path).replace('\\', '/'))

    def HDFS_setPermission(self, hdfs_path, permission):
        """ 修改指定文件的权限信息 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSException("Please Connect HDFS first.")
        m_hdfs_filepath = os.path.dirname(hdfs_path)
        m_hdfs_filename = os.path.basename(hdfs_path)
        self.__m_HDFS_Handler__.set_permission(
            os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, m_hdfs_filename).replace('\\', '/'),
            permission=permission)

    def HDFS_Delete(self, hdfs_path, recusive=False):
        """ 删除指定的HDFS文件 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSException("Please Connect HDFS first.")

        m_FileList = self.HDFS_list(recusive=recusive)
        for row in m_FileList:
            if fnmatch.fnmatch(row[0], hdfs_path):
                logger.write("Will remove hdfs file [" + row[0] + "] ... ")
                self.__m_HDFS_Handler__.delete(row[0], recursive=True)

    def HDFS_Upload(self, local_path, hdfs_path=None):
        """ 上传文件到hdfs """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSException("Please Connect HDFS first.")

        for file in glob(local_path):
            try:
                logger.info("Will upload file [" + str(file) + "] to [" + str(hdfs_path) + "] .... ")
                if hdfs_path is None:
                    m_hdfs_filepath = ""
                    m_hdfs_filename = os.path.basename(file)
                else:
                    if hdfs_path.endswith("/"):
                        m_hdfs_filepath = hdfs_path
                        m_hdfs_filename = os.path.basename(file)
                    else:
                        m_hdfs_filepath = os.path.dirname(hdfs_path)
                        m_hdfs_filename = os.path.basename(hdfs_path)
                self.__m_HDFS_Handler__.upload(
                    os.path.join(self.__m_HDFS_WebFSDir__, m_hdfs_filepath, m_hdfs_filename).replace('\\', '/'),
                    file,
                    overwrite=True,
                    cleanup=True)
            except HdfsError as he:
                print('traceback.print_exc():\n%s' % traceback.print_exc())
                print('traceback.format_exc():\n%s' % traceback.format_exc())
                raise HDFSException(repr(he))

    def HDFS_Download(self, hdfs_path="", local_path="", recusive=False):
        """ 从hdfs获取文件到本地 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSException("Please Connect HDFS first.")

        logger.write("Try download remote hdfs [" + hdfs_path + "] to [" + local_path + "] ....")
        # 如果本地没有对应目录，且local_path传递的是一个目录，则建立目录
        if "T_WORK" in os.environ:
            m_LocalPath = os.path.join(os.environ["T_WORK"], local_path)
        else:
            m_LocalPath = local_path
        if m_LocalPath.endswith("/") and not os.path.exists(m_LocalPath):
            os.makedirs(m_LocalPath)

        m_FileList = self.HDFS_list(recusive=recusive)
        for row in m_FileList:
            if fnmatch.fnmatch(row[0], hdfs_path):
                logger.write("Downloading remote hdfs file [" + row[0] + "] to local [" + m_LocalPath + "] ... ")
                self.__m_HDFS_Handler__.download(row[0], m_LocalPath, overwrite=True)

    def HDFS_list(self, hdfs_path="", recusive=False):
        """ 返回目录下的文件 """
        if self.__m_HDFS_Handler__ is None:
            raise HDFSException("Please Connect HDFS first.")
        m_ReturnList = []
        if not recusive:
            for row in self.__m_HDFS_Handler__.list(hdfs_path, status=True):
                m_ReturnList.append((os.path.join(hdfs_path, row[0]), row[1]))
            return m_ReturnList
        else:
            for row in self.__m_HDFS_Handler__.list(hdfs_path, status=True):
                if row[1]['type'].upper() == 'DIRECTORY':
                    m_ReturnList.extend(
                        self.HDFS_list(os.path.join(hdfs_path, row[0]).replace("\\", "/"),
                                       recusive=True)
                    )
                else:
                    m_ReturnList.append((os.path.join(hdfs_path, row[0]).replace("\\", "/"), row[1]))
            return m_ReturnList

    def HDFS_cd(self, hdfs_path):
        """ 切换当前目录， 其实就是重新连接了 """
        m_NewDirectory = Path(os.path.join(self.__m_HDFS_WebFSDir__, hdfs_path)).as_posix()
        self.__m_HDFS_WebFSDir__ = m_NewDirectory
        self.__m_HDFS_Handler__ = Client(self.__m_HDFS_WebFSURL__, self.__m_HDFS_WebFSDir__, session=None)

    def HDFS_SetConnectedUser(self, p_ConnectUser):
        """ 设置HDFS连接时使用的用户 """
        self.__m_HDFS_ConnectUser = p_ConnectUser

    def HDFS_Connect(self, p_szURL):
        """ 连接HDFS, URL使用WEBFS协议 """
        self.__m_HDFS_Protocal__ = p_szURL.split("://")[0]
        self.__m_HDFS_NodePort__ = p_szURL[len(self.__m_HDFS_Protocal__) + 3:].split("/")[0]
        self.__m_HDFS_WebFSURL__ = self.__m_HDFS_Protocal__ + "://" + self.__m_HDFS_NodePort__
        self.__m_HDFS_WebFSDir__ = p_szURL[len(self.__m_HDFS_WebFSURL__):]
        logger.info("Will connect to [" + str(self.__m_HDFS_WebFSURL__) + "]," +
                    "Rootdir is [" + str(self.__m_HDFS_WebFSDir__) + "] .... ")
        self.__m_HDFS_Handler__ = InsecureClient(url=self.__m_HDFS_WebFSURL__,
                                                 user=self.__m_HDFS_ConnectUser,
                                                 root=self.__m_HDFS_WebFSDir__)


if __name__ == '__main__':
    myCompare = RunHDFSCommand()
    myCompare.HDFS_SetConnectedUser("ldbtest")
    myCompare.HDFS_Connect("http://node73:50070/user/testdb73/jenkins/work")
    os.environ["T_WORK"] = "C:\\Work\\linkoop\\linkoop-auto-test\\linkoopdb\\regression\\work"
    myCompare.HDFS_Delete("*.txt")
