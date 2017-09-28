import configparser
import os

# Keys and Values
CURRENT_PATH = os.path.dirname(os.path.abspath(__file__))


def get_config(source, section="DEFAULT"):
    config = configparser.ConfigParser(interpolation=None)
    config.read(CURRENT_PATH + "/" + source + ".ini")

    sec = config[section]

    ans = {}
    for z in sec:
        ans[z] = sec[z]

    return ans


if __name__ == '__main__':
    print(get_config("db", "MEDICINE"))
