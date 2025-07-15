from general_process.SourceProcess import SourceProcess


import json
import numpy as np


class Pes2024Process(SourceProcess):
    def __init__(self,data_format,report):
        super().__init__("pes_2024",data_format,report)

    def _url_init(self):
        super()._url_init()

    def get(self):
        super().get()

    def convert(self):
        super().convert()
        
    def fix(self):
        super().fix()
