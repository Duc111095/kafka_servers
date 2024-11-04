import configparser

config = configparser.ConfigParser()

with open('C:/Users/USER/PycharmProjects/kafka_servers/utils/server.txt', 'r') as f:
    for line in f.readlines():
        option = line.split()
        config[option[0]] = {'server': option[1],
                                'database': option[2],
                                'username': option[3],
                                'password': option[4]}

with open('config.ini', 'w') as configfile:
    config.write(configfile)
