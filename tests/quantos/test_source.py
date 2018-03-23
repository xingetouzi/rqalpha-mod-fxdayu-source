# encoding: utf-8

import pathlib
import unittest

from rqalpha_mod_fxdayu_source.data_source.quantos import QuantOsSource
from tests.common.source import TestDataSourceMixin


class TestQuantOsDataSource(unittest.TestCase, TestDataSourceMixin):
    # 初始化工作
    def setUp(self):
        path = pathlib.Path("~/.rqalpha/bundle").expanduser()
        self.source = QuantOsSource(str(path))


if __name__ == '__main__':
    unittest.main()
