from PyByteBuffer import ByteBuffer #https://pypi.org/project/PyByteBuffer/
import pandas as pd
import struct
import sys

in_file = open(r"D:\Temp\WeatherStation\2013-08.wlk", "rb")
data = in_file.read()
in_file.close()

content = data.decode('ansi', 'slashescape')
print(content)


