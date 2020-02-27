#!/usr/bin/env python3
# encoding: utf-8
#
# Copyright (c) 2008 Doug Hellmann All rights reserved.
#
"""Using a Condition to control sequencing between threads.
"""
#end_pymotw_header
import logging
import threading
import time


def consumer(cond):
    """wait for the condition and use the resource"""
    logging.debug('Starting consumer thread')
    with cond:
        cond.wait()
        logging.debug('Resource is available to consumer')


def consumer1(cond):
    """wait for the condition and use the resource"""
    logging.debug('Starting consumer thread')
    with cond:
        logging.debug('Resource is available to consumer1')



def producer(cond):
    """set up the resource to be used by the consumer"""
    logging.debug('Starting producer thread')
    with cond:
        logging.debug('Making resource available')
        cond.notifyAll()


logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s (%(threadName)-2s) %(message)s',
)

condition = threading.Condition()
c1 = threading.Thread(name='c1', target=consumer,
                      args=(condition,))
c2 = threading.Thread(name='c2', target=consumer,
                      args=(condition,))
c3 = threading.Thread(name='c3', target=consumer1,
                      args=(condition,))
p = threading.Thread(name='p', target=producer,
                     args=(condition,))

c1.start()
time.sleep(0.2)
c2.start()
time.sleep(0.2)
c3.start()
time.sleep(0.2)
p.start()

"""
2020-02-27 17:16:22,311 (c1) Starting consumer thread
2020-02-27 17:16:22,516 (c2) Starting consumer thread
2020-02-27 17:16:22,718 (c3) Starting consumer thread
2020-02-27 17:16:22,718 (c3) Resource is available to consumer1
2020-02-27 17:16:22,922 (p ) Starting producer thread
2020-02-27 17:16:22,922 (p ) Making resource available
2020-02-27 17:16:22,922 (c1) Resource is available to consumer
2020-02-27 17:16:22,922 (c2) Resource is available to consumer
"""