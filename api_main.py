from api_functions import *
import time

if __name__ == '__main__':
    t0 = time.time()
    connect_to_api()
    connect_to_db()
    t1 = time.time()
    print(f"Execution Time: {(t1-t0)/60} minutes")
