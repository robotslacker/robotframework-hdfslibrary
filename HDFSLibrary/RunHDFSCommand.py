# -*- coding: UTF-8 -*-


class HDFSException(Exception):
    def __init__(self, message):
        Exception.__init__(self)
        self.message = message


class RunHDFSCommand(object):
    pass


if __name__ == '__main__':
    myCompare = RunHDFSCommand()
    pass
