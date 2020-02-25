__version__ = '0.8.1'
__revision__ = "$Rev$"
__date__ = "$Date$"


class PooledDBError(Exception):
	pass


class NotSupportedError(PooledDBError):
	pass


class PooledDBConnection:
	def __init__(self, pool, con):
		self._con = con
		self._pool = pool

	def close(self):
		if self._con is not None:
			self._pool.returnConnection(self._con)
			self._con = None

	def __getattr__(self, name):
		return getattr(self._con, name)

	def __del__(self):
		self.close()


class PooledDB:
	def __init__(self, dbapi, maxconnections, *args, **kwargs):
		try:
			threadsafety = dbapi.threadsafety
		except:
			threadsafety = None

		if threadsafety == 0:
			raise NotSupportedError
		elif threadsafety == 1:
			from Queue import Queue
			self._queue = Queue(maxconnections) # create the queue
			self.connection = self._unthreadsafe_get_connection
			self.addConnection = self._unthreadsafe_add_connection
			self.returnConnection = self._unthreadsafe_return_connection
		elif threadsafety in (2, 3):
			from threading import Lock
			self._lock = Lock() # create a lock object to be used later
			self._nextCon = 0 # the index of the next connection to be used
			self._connections = [] # the list of connections
			self.connection = self._threadsafe_get_connection
			self.addConnection = self._threadsafe_add_connection
			self.returnConnection = self._threadsafe_return_connection
		else:
			raise NotSupportedError
		for i in range(maxconnections):
			self.addConnection(dbapi.connect(*args, **kwargs))

	def _unthreadsafe_get_connection(self):
		""""Get a connection from the pool."""
		return PooledDBConnection(self, self._queue.get())

	def _unthreadsafe_add_connection(self, con):
		""""Add a connection to the pool."""
		self._queue.put(con)

	def _unthreadsafe_return_connection(self, con):
		""""Return a connection to the pool.

		In this case, the connections need to be put
		back into the queue after they have been used.
		This is done automatically when the connection is closed
		and should never be called explicitly outside of this module.
		"""
		self._unthreadsafe_add_connection(con)

	# The following functions are used with DB-API 2 modules
	# that are threadsafe at the connection level, like psycopg.
	# Note: In this case, connections are shared between threads.
	# This may lead to problems if you use transactions.

	def _threadsafe_get_connection(self):
		""""Get a connection from the pool."""
		self._lock.acquire()
		try:
			next = self._nextCon
			con = PooledDBConnection(self, self._connections[next])
			next += 1
			if next >= len(self._connections):
				next = 0
			self._nextCon = next
			return con
		finally:
			self._lock.release()

	def _threadsafe_add_connection(self, con):
		""""Add a connection to the pool."""
		self._connections.append(con)

	def _threadsafe_return_connection(self, con):
		"""Return a connection to the pool.

		In this case, the connections always stay in the pool,
		so there is no need to do anything here.
		"""
		pass


