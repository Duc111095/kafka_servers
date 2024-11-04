import configparser

config = configparser.ConfigParser()

with open('C:/Users/USER/PycharmProjects/kafka_servers/utils/server.txt', 'r') as f:
    for line in f.readlines():
        properties = line.split()
        config[properties[0]] = {'server': properties[1],
                                'database': properties[2],
                                'username': properties[3],
                                'password': properties[4]}

with open('config.ini', 'w') as configfile:
    config.write(configfile)
