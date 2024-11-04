import configparser

config = configparser.ConfigParser()

with open('C:/Users/USER/PycharmProjects/kafka_servers/utils/server.txt', 'r') as f:
    for line in f.readlines():
        options = line.split()
        config[options[0]] = {'server': options[1],
                                'database': options[2],
                                'username': options[3],
                                'password': options[4]}

with open('config.ini', 'w') as configfile:
    config.write(configfile)
