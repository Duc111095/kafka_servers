import base64
import configparser

config = configparser.ConfigParser()

with open('server.txt', 'r') as f:
    for line in f.readlines():
        stroptions = line.split()
        config[stroptions[0]] = {'server': stroptions[1],
                                'database': stroptions[2]
                                }

with open('server.ini', 'w') as configfile:
    config.write(configfile)
