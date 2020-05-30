#!/usr/bin/python3

from configparser import ConfigParser


# function for create parser config file
def config(section, filename='database.conf'):
    parser = ConfigParser()
    parser.read(filename)

    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        # for python 3.6.8 <
        # raise Exception('Section {0} not found in the {1} file'.format(section, filename))
        # for python 3.6.8 >
        raise Exception(f'Section {section} not found in the {filename} file')
    # print(db)
    return db



