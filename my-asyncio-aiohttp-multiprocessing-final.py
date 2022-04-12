import asyncio
import aiohttp
import multiprocessing
from multiprocessing import Pool

import functools
import time
from time import gmtime, strftime

import logging
import os
import sys
#----------------------------------------------------------------------------------------------------------
__author__     = 'Upinder Sujlana'
__copyright__  = 'Copyright 2022'
__version__    = '1.0.0'
__maintainer__ = 'Upinder Sujlana'
__status__     = 'demo'
#----------------------------------------------------------------------------------------------------------
logfilename=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'ayncio-multiprocessing.log')
logging.basicConfig(
    level=logging.ERROR,
    format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%d-%m-%Y:%H:%M:%S',
    handlers=[
        logging.FileHandler(logfilename, mode='a'),
        logging.StreamHandler()
    ]
)
#----------------------------------------------------------------------------------------------------------
#A decorator to get the total runtime for the function
write_timer_to_file = False
timelog=os.path.join(os.path.dirname(os.path.realpath(__file__)), 'timelog.txt')
def timer(func):
    """Print the runtime of the decorated function"""
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        start_time = time.perf_counter()    
        value = func(*args, **kwargs)
        end_time = time.perf_counter()      
        run_time = end_time - start_time    
        print(f"Finished {func.__name__!r} in {run_time:.4f} secs")
        if write_timer_to_file:
            with open (timelog,'a') as outfile:
                outfile.write(f'{strftime("%Y-%m-%d %H:%M:%S", gmtime())}\t{func.__name__}\t{run_time:.4f} secs \n')
        return value
    return wrapper_timer
#----------------------------------------------------------------------------------------------------------
urls = [
    'https://www.yahoo.com/','http://www.cnn.com','http://www.python.org','http://www.jython.org',
    'http://www.pypy.org','http://www.perl.org','http://www.cisco.com','http://www.facebook.com',
    'http://www.twitter.com', 'http://www.macrumors.com/','http://arstechnica.com/',
    'http://www.reuters.com/','http://abcnews.go.com/', 'http://www.cnbc.com/','http://www.twilio.com/',
]
#------------coroutine - aka has a await keyword - section start--------------------
async def url_fetcher(session, url):
    async with session.get(url) as response:
        json_response = await response.read()        
        #Creating a tuple of url and the response & returning
        return (url, json_response) 
async def runner():
    try:
        async with aiohttp.ClientSession() as session:
            #Async suggests to create a list of tasks
            tasks =[url_fetcher(session,url) for url in urls]
            #Async asks to gather all the tasks and than throw a await so GIL
            #does not block and lets other tasks run concurrently
            aiohttp_processed_data = await asyncio.gather(*tasks)
            return aiohttp_processed_data
    except aiohttp.ClientConnectorError as e:
        logging.error("ClientConnectorError runner, stacktrace : ", exc_info=True)
        sys.exit(3)
#----------------------------------------------------------------------------------------------------------
# Multi-processing Section. Print the length of the downloaded webpage.
def calculate_length(item):
    try:
        print (f"For URL, {item[0]}. Length is  {len(item[1])}.") 
    except:
        logging.error("Error calculating length, stacktrace : ", exc_info=True)
        sys.exit(2)    
#----------------------------------------------------------------------------------------------------------
@timer
def main():
    try:
        data_back = asyncio.run(runner())
    except:
        logging.error("Error getting data_back, stacktrace : ", exc_info=True)
        sys.exit(1)
    with Pool(processes=multiprocessing.cpu_count()) as pool:
        pool.map(calculate_length, data_back)        
#----------------------------------------------------------------------------------------------------------
if __name__ ==  '__main__':
    main()