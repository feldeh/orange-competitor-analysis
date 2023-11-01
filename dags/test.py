from utils import *


def load_logs_to_bq():


    logs_data = load_ndjson('mobileviking', 'logs')

    print(logs_data)



load_logs_to_bq()



