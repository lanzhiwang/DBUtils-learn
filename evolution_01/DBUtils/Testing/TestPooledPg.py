"""Test the PooledPg module.

Note:
We don't test performance here, so the test does not predicate
whether PooledPg actually will help in improving performance or not.
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
from PooledPg import PooledPg


class TestPooledPg(unittest.TestCase):

	def test0_CheckVersion(self):
		TestPooledPgVersion = __version__
		from PooledPg import __version__ as PooledPgVersion
		self.assertEqual(PooledPgVersion, TestPooledPgVersion)

	def test1_CreateConnection(self):
		pool = PooledPg(1, 1, 0, 0, 0, None,
			'PooledPgTestDB', user='PooledPgTestUser')
		self.assert_(hasattr(pool, '_cache'))
		self.assertEqual(pool._cache.qsize(), 1)
		self.assert_(hasattr(pool, '_maxusage'))
		self.assertEqual(pool._maxusage, 0)
		self.assert_(hasattr(pool, '_setsession'))
		self.assert_(pool._setsession is None)
		db_con = pool._cache.get(0)
		pool._cache.put(db_con, 0)
		from SolidPg import SolidPgConnection
		self.assert_(isinstance(db_con, SolidPgConnection))
		db = pool.connection()
		self.assertEqual(pool._cache.qsize(), 0)
		self.assert_(hasattr(db, '_con'))
		self.assertEqual(db._con, db_con)
		self.assert_(hasattr(db, 'query'))
		self.assert_(hasattr(db, 'num_queries'))
		self.assertEqual(db.num_queries, 0)
		self.assert_(hasattr(db, '_maxusage'))
		self.assertEqual(db._maxusage, 0)
		self.assert_(hasattr(db, '_setsession_sql'))
		self.assert_(db._setsession_sql is None)
		self.assert_(hasattr(db, 'dbname'))
		self.assertEqual(db.dbname, 'PooledPgTestDB')
		self.assert_(hasattr(db, 'user'))
		self.assertEqual(db.user, 'PooledPgTestUser')
		db.query('select test')
		self.assertEqual(db.num_queries, 1)
		pool = PooledPg(1)
		db = pool.connection()
		self.assert_(hasattr(db, 'dbname'))
		self.assert_(db.dbname is None)
		self.assert_(hasattr(db, 'user'))
		self.assert_(db.user is None)
		self.assert_(hasattr(db, 'num_queries'))
		self.assertEqual(db.num_queries, 0)
		pool = PooledPg(0, 0, 0, 0, 3, ('set datestyle',),)
		self.assertEqual(pool._maxusage, 3)
		self.assertEqual(pool._setsession, ('set datestyle',))
		db = pool.connection()
		self.assertEqual(db._maxusage, 3)
		self.assertEqual(db._setsession_sql, ('set datestyle',))

	def test2_CloseConnection(self):
		pool = PooledPg(0, 1, 0, 0, 0, None,
			'PooledPgTestDB', user='PooledPgTestUser')
		db = pool.connection()
		self.assert_(hasattr(db, '_con'))
		db_con = db._con
		from SolidPg import SolidPgConnection
		self.assert_(isinstance(db_con, SolidPgConnection))
		self.assert_(hasattr(pool, '_cache'))
		self.assertEqual(pool._cache.qsize(), 0)
		self.assertEqual(db.num_queries, 0)
		db.query('select test')
		self.assertEqual(db.num_queries, 1)
		db.close()
		self.assert_(not hasattr(db, 'num_queries'))
		db = pool.connection()
		self.assert_(hasattr(db, 'dbname'))
		self.assertEqual(db.dbname, 'PooledPgTestDB')
		self.assert_(hasattr(db, 'user'))
		self.assertEqual(db.user, 'PooledPgTestUser')
		self.assertEqual(db.num_queries, 1)
		db.query('select test')
		self.assertEqual(db.num_queries, 2)
		db = pool.connection()
		self.assertEqual(pool._cache.qsize(), 1)
		self.assertEqual(pool._cache.get(0), db_con)

	def test3_MinMaxCached(self):
		pool = PooledPg(3)
		self.assert_(hasattr(pool, '_cache'))
		self.assertEqual(pool._cache.qsize(), 3)
		cache = []
		for i in range(3):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		for i in range(3):
			cache.pop().close()
		self.assertEqual(pool._cache.qsize(), 3)
		for i in range(6):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		for i in range(6):
			cache.pop().close()
		self.assertEqual(pool._cache.qsize(), 6)
		pool = PooledPg(3, 4)
		self.assert_(hasattr(pool, '_cache'))
		self.assertEqual(pool._cache.qsize(), 3)
		cache = []
		for i in range(3):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		for i in range(3):
			cache.pop().close()
		self.assertEqual(pool._cache.qsize(), 3)
		for i in range(6):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		for i in range(6):
			cache.pop().close()
		self.assertEqual(pool._cache.qsize(), 4)
		pool = PooledPg(3, 2)
		self.assert_(hasattr(pool, '_cache'))
		self.assertEqual(pool._cache.qsize(), 3)
		cache = []
		for i in range(4):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		for i in range(4):
			cache.pop().close()
		self.assertEqual(pool._cache.qsize(), 3)
		pool = PooledPg(2, 5)
		self.assert_(hasattr(pool, '_cache'))
		self.assertEqual(pool._cache.qsize(), 2)
		cache = []
		for i in range(10):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		for i in range(10):
			cache.pop().close()
		self.assertEqual(pool._cache.qsize(), 5)

	def test4_MaxConnections(self):
		from PooledPg import TooManyConnections
		pool = PooledPg(1, 2, 3)
		self.assertEqual(pool._cache.qsize(), 1)
		cache = []
		for i in range(3):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		self.assertRaises(TooManyConnections, pool.connection)
		pool = PooledPg(0, 1, 1, 0)
		self.assertEqual(pool._blocking, 0)
		self.assertEqual(pool._cache.qsize(), 0)
		db = pool.connection()
		self.assertEqual(pool._cache.qsize(), 0)
		self.assertRaises(TooManyConnections, pool.connection)
		pool = PooledPg(1, 2, 1)
		self.assertEqual(pool._cache.qsize(), 1)
		cache = []
		cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		self.assertRaises(TooManyConnections, pool.connection)
		pool = PooledPg(3, 2, 1, 0)
		self.assertEqual(pool._cache.qsize(), 3)
		cache = []
		for i in range(3):
			cache.append(pool.connection())
		self.assertEqual(pool._cache.qsize(), 0)
		self.assertRaises(TooManyConnections, pool.connection)
		pool = PooledPg(1, 1, 1, 1)
		self.assertEqual(pool._blocking, 1)
		self.assertEqual(pool._cache.qsize(), 1)
		db = pool.connection()
		self.assertEqual(pool._cache.qsize(), 0)
		def connection():
			pool.connection().query('set thread')
		from threading import Thread
		Thread(target=connection).start()
		from time import sleep
		sleep(0.1)
		self.assertEqual(pool._cache.qsize(), 0)
		session = db._con.session
		self.assertEqual(session, [])
		del db
		sleep(0.1)
		self.assertEqual(pool._cache.qsize(), 1)
		db = pool.connection()
		self.assertEqual(pool._cache.qsize(), 0)
		self.assertEqual(session, ['thread'])

	def test5_OneThreadTwoConnections(self):
		pool = PooledPg(2)
		db1 = pool.connection()
		for i in range(5):
			db1.query('select test')
		db2 = pool.connection()
		self.assertNotEqual(db1, db2)
		self.assertNotEqual(db1._con, db2._con)
		for i in range(7):
			db2.query('select test')
		self.assertEqual(db1.num_queries, 5)
		self.assertEqual(db2.num_queries, 7)
		del db1
		db1 = pool.connection()
		self.assertNotEqual(db1, db2)
		self.assertNotEqual(db1._con, db2._con)
		self.assert_(hasattr(db1, 'query'))
		for i in range(3):
			db1.query('select test')
		self.assertEqual(db1.num_queries, 8)
		db2.query('select test')
		self.assertEqual(db2.num_queries, 8)

	def test6_ThreeThreadsTwoConnections(self):
		pool = PooledPg(2, 2, 2, 1)
		from Queue import Queue, Empty
		queue = Queue(3)
		def connection():
			queue.put(pool.connection(), 1, 0.1)
		from threading import Thread
		for i in range(3):
			Thread(target=connection).start()
		db1 = queue.get(1, 0.1)
		db2 = queue.get(1, 0.1)
		db1_con = db1._con
		db2_con = db2._con
		self.assertNotEqual(db1, db2)
		self.assertNotEqual(db1_con, db2_con)
		self.assertRaises(Empty, queue.get, 1, 0.1)
		del db1
		db1 = queue.get(1, 0.1)
		self.assertNotEqual(db1, db2)
		self.assertNotEqual(db1._con, db2._con)
		self.assertEqual(db1._con, db1_con)


if __name__ == '__main__':
	unittest.main()
