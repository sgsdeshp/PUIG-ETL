from api_functions import *
from api_data_read_write import *
import time

if __name__ == '__main__':
    t0 = time.time()
    # connect_to_db()
    connect_to_serv_acc()
    connect_to_api()
    get_bikes()
    t1 = time.time()
    print(f"Execution Time: {(t1-t0)/60} minutes")
