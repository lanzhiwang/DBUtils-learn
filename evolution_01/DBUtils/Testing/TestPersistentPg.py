"""Test the PersistentPg module.

Note:
We don't test performance here, so the test does not predicate
whether PersistentPg actually will help in improving performance or not.
We also assume that the underlying SolidPg connections are tested.

Copyright and credit info:

* This test was contributed by Christoph Zwerschke

"""

__version__ = '0.8.1'
__revision__ = "$Rev$"
__date__ = "$Date$"


import sys

# This module also serves as a mock object for the pg API module:

import TestSolidPg

import unittest
sys.path.insert(1, '..')
from PersistentPg import PersistentPg


class TestPersistentPg(unittest.TestCase):

	def test0_CheckVersion(self):
		TestPersistentPgVersion = __version__
		from PersistentPg import __version__ as PersistentPgVersion
		self.assertEqual(PersistentPgVersion, TestPersistentPgVersion)

	def test1_PersistentPg(self):
		numThreads = 3
		persist = PersistentPg()
		from Queue import Queue, Empty
		queryQueue,  resultQueue = [], []
		for i in range(numThreads):
			queryQueue.append(Queue(1))
			resultQueue.append(Queue(1))
		def runQueries(i):
			this_db  = persist.connection().db
			while 1:
				try:
					q = queryQueue[i].get(1, 1)
				except Empty:
					q = None
				if not q: break
				db = persist.connection()
				if db.db != this_db:
					r = 'error - not persistent'
				else:
					if q == 'ping':
						r = 'ok - thread alive'
					elif q == 'close':
						db.db.close()
						r = 'ok - connection closed'
					else:
						r = db.query(q)
				r = '%d(%d): %s' % (i, db._usage, r)
				resultQueue[i].put(r, 1, 0.1)
			db.close()
		from threading import Thread
		threads = []
		for i in range(numThreads):
			thread = Thread(target=runQueries, args=(i,))
			threads.append(thread)
			thread.start()
		for i in range(numThreads):
			queryQueue[i].put('ping', 1, 0.1)
		for i in range(numThreads):
			r = resultQueue[i].get(1, 0.1)
			self.assertEqual(r, '%d(0): ok - thread alive' % i)
			self.assert_(threads[i].isAlive())
		for i in range(numThreads):
			for j in range(i + 1):
				queryQueue[i].put('select test%d' % j, 1, 0.1)
				r = resultQueue[i].get(1, 0.1)
				self.assertEqual(r, '%d(%d): test%d' % (i, j + 1, j))
		queryQueue[1].put('select test4', 1, 0.1)
		r = resultQueue[1].get(1, 0.1)
		self.assertEqual(r, '1(3): test4')
		queryQueue[1].put('close', 1, 0.1)
		r = resultQueue[1].get(1, 0.1)
		self.assertEqual(r, '1(3): ok - connection closed')
		for j in range(2):
			queryQueue[1].put('select test%d' % j, 1, 0.1)
			r = resultQueue[1].get(1, 0.1)
			self.assertEqual(r, '1(%d): test%d' % (j + 1, j))
		for i in range(numThreads):
			self.assert_(threads[i].isAlive())
			queryQueue[i].put('ping', 1, 0.1)
		for i in range(numThreads):
			r = resultQueue[i].get(1, 0.1)
			self.assertEqual(r, '%d(%d): ok - thread alive' % (i, i + 1))
			self.assert_(threads[i].isAlive())
		for i in range(numThreads):
			queryQueue[i].put(None, 1, 1)

	def test2_PersistentPgMaxUsage(self):
		persist = PersistentPg(20)
		db = persist.connection()
		self.assertEqual(db._maxusage, 20)
		for i in range(100):
			r = db.query('select test%d' % i)
			self.assertEqual(r, 'test%d' % i)
			self.assert_(db.db.status)
			j = i % 20 + 1
			self.assertEqual(db._usage, j)
			self.assertEqual(db.num_queries, j)

	def test3_PersistentPgSetSession(self):
		persist = PersistentPg(3, ('set datestyle',))
		db = persist.connection()
		self.assertEqual(db._maxusage, 3)
		self.assertEqual(db._setsession_sql, ('set datestyle',))
		self.assertEqual(db.db.session, ['datestyle'])
		db.query('set test')
		for i in range(3):
			self.assertEqual(db.db.session, ['datestyle', 'test'])
			db.query('select test')
		self.assertEqual(db.db.session, ['datestyle'])


if __name__ == '__main__':
	unittest.main()
