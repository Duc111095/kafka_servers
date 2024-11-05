import base64
import configparser

config = configparser.ConfigParser()

with open('server.txt', 'r') as f:
    for line in f.readlines():
        options = line.split()
        config[options[0]] = {'server': options[1],
                            'database': options[2],
                            'username': options[3],
                            'password': base64.b64encode(options[4].encode("utf-8"))}

with open('server.ini', 'w') as configfile:
    config.write(configfile)
