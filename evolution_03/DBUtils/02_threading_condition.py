#!/usr/bin/env python3
# encoding: utf-8

import logging
import threading
import time
import random

class Foo():
    def __init__(self, count):
        self.count = count
        self.condition = threading.Condition()

    def consumer(self):
        """wait for the condition and use the resource"""
        logging.debug('Starting consumer thread')
        with self.condition:
            if threading.current_thread().getName() == 'c1':
                time.sleep(random.randint(1, 3))
            self.condition.wait()
            self.count += 1
            logging.debug('Resource is available to consumer')

    def producer(self):
        """set up the resource to be used by the consumer"""
        logging.debug('Starting producer thread')
        with self.condition:
            logging.debug('Making resource available')
            self.condition.notify()

foo = Foo(10)

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s (%(threadName)-2s) %(message)s',
)

condition = threading.Condition()
c1 = threading.Thread(name='c1', target=foo.consumer)
c2 = threading.Thread(name='c2', target=foo.consumer)
p = threading.Thread(name='p', target=foo.producer)


c2.start()
c1.start()
p.start()

"""
2020-02-26 10:34:21,742 (c1) Starting consumer thread
2020-02-26 10:34:21,945 (c2) Starting consumer thread
2020-02-26 10:34:22,148 (p ) Starting producer thread
2020-02-26 10:34:22,149 (p ) Making resource available
2020-02-26 10:34:22,149 (c1) Resource is available to consumer
2020-02-26 10:34:22,149 (c2) Resource is available to consumer
"""