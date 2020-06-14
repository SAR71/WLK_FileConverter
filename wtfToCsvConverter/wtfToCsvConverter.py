#from PyByteBuffer import ByteBuffer #https://pypi.org/project/PyByteBuffer/
import struct

in_file = open(r"D:\Temp\WeatherStation\2013-08.wlk", "rb")
data = in_file.read()
in_file.close()

#content = data.decode('ansi').splitlines()
content = data.decode('ansi', 'slashescape')

#offset = 0
#content = struct.unpack_from("<L", data, offset)

with open(r"D:\Temp\WeatherStation\Output.txt", "w") as text_file:
    for line in content:
        text_file.write(line)

#decoded = data.decode('ansi', 'slashescape')

#offset = 0
#content = struct.unpack_from("<d", data, offset)
#data.decode('ansi', 'slashescape')
