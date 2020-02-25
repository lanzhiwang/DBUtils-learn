__version__ = '0.8.1'
__revision__ = "$Rev$"
__date__ = "$Date$"


from SolidDB import connect
from threading import Condition

class PooledDBError(Exception): pass
class NotSupportedError(PooledDBError): pass
class TooManyConnections(PooledDBError): pass
class InvalidConnection(PooledDBError): pass


class PooledDB:
	def __init__(
		self,
		dbapi,
		mincached=0,
		maxcached=0,
		maxshared=0,
		maxconnections=0,
		blocking=0,
		maxusage=0,
		setsession=None,
		*args,
		**kwargs):
		self._dbapi = dbapi
		# Connections can be only shareable if the underlying DB-API 2
		# module is thread-safe at the connection level:
		threadsafety = getattr(dbapi, 'threadsafety', None)
		if not threadsafety:
			raise NotSupportedError
		self._args, self._kwargs = args, kwargs
		self._maxusage = maxusage
		self._setsession = setsession
		if maxcached:
			if maxcached < mincached:
				maxcached = mincached
			self._maxcached = maxcached
		else:
			self._maxcached = 0
		if threadsafety > 1 and maxshared:
			self._maxshared = maxshared
			self._shared_cache = [] # the cache for shared connections
		else:
			self._maxshared = 0
		if maxconnections:
			if maxconnections < maxcached:
				maxconnections = maxcached
			if maxconnections < maxshared:
				maxconnections = maxshared
			self._maxconnections = maxconnections
		else:
			self._maxconnections = 0
		self._idle_cache = [] # the actual pool of idle connections
		self._connections = 0
		self._condition = Condition()
		if not blocking:
			def wait():
				raise TooManyConnections
			self._condition.wait = wait
		# Establish an initial number of idle database connections:
		[self.connection(0) for i in range(mincached)]

	def solid_connection(self):
		"""Get a solid, unpooled solid DB-API 2 connection."""
		return connect(self._dbapi,
			self._maxusage, self._setsession, *self._args, **self._kwargs)

	def connection(self, shareable=1):
		""""Get a solid, cached DB-API 2 connection from the pool.

		If shareable is set and the underlying DB-API 2 allows it,
		then the connection may be shared with other threads.
		"""
		if shareable and self._maxshared:
			self._condition.acquire()
			try:
				while not self._shared_cache and self._maxconnections \
					and self._connections >= self._maxconnections:
					self._condition.wait()
				if len(self._shared_cache) < self._maxshared:
					# shared cache is not full, get a dedicated connection
					try: # first try to get it from the idle cache
						con = self._idle_cache.pop(0)
					except IndexError: # else get a fresh connection
						con = self.solid_connection()
					con = SharedDBConnection(con)
					self._connections += 1
				else: # shared cache full or no more connections allowed
					self._shared_cache.sort() # least shared connection first
					con = self._shared_cache.pop(0) # get it
					con.share() # increase share of this connection
				# put the connection (back) into the shared cache
				self._shared_cache.append(con)
				self._condition.notify()
			finally:
				self._condition.release()
			con = PooledSharedDBConnection(self, con)
		else: # try to get a dedicated connection
			self._condition.acquire()
			try:
				while self._maxconnections \
					and self._connections >= self._maxconnections:
					self._condition.wait()
				# connection limit not reached, get a dedicated connection
				try: # first try to get it from the idle cache
					con = self._idle_cache.pop(0)
				except IndexError: # else get a fresh connection
					con = self.solid_connection()
				con = PooledDedicatedDBConnection(self, con)
				self._connections += 1
			finally:
				self._condition.release()
		return con

	def unshare(self, con):
		"""Decrease the share of a connection in the shared cache."""
		self._condition.acquire()
		try:
			con.unshare()
			shared = con.shared
			if not shared: # connection is idle,
				try: # so try to remove it
					self._shared_cache.remove(con) # from shared cache
				except ValueError:
					pass # pool has already been closed
		finally:
			self._condition.release()
		if not shared: # connection has become idle,
			self.cache(con.con) # so add it to the idle cache

	def cache(self, con):
		"""Put a dedicated connection back into the idle cache."""
		self._condition.acquire()
		try:
			if not self._maxcached or len(self._idle_cache) < self._maxcached:
				# the idle cache is not full, so put it there, but
				try: # before returning the connection back to the pool,
					con.rollback() # perform a rollback
					# in order to prevent uncommited actions from being
					# unintentionally commited by some other thread
				except: # if an error occurs (no transaction, not supported)
					pass # then it will be silently ignored
				self._idle_cache.append(con) # append it to the idle cache
			else: # if the idle cache is already full,
				con.close() # then close the connection
			self._connections -= 1
			self._condition.notify()
		finally:
			self._condition.release()

	def close(self):
		"""Close all connections in the pool."""
		self._condition.acquire()
		try:
			while self._idle_cache: # close all idle connections
				self._idle_cache.pop(0).close()
				self._connections -= 1
			if self._maxshared: # close all shared connections
				while self._shared_cache:
					self._shared_cache.pop(0).con.close()
					self._connections -= 1
			self._condition.notifyAll()
		finally:
			self._condition.release()

	def __del__(self):
		"""Delete the pool."""
		if hasattr(self, '_connections'):
			self.close()


# Auxiliary classes for pooled connections

class PooledDedicatedDBConnection:
	"""Auxiliary proxy class for pooled dedicated connections."""

	def __init__(self, pool, con):
		"""Create a pooled dedicated connection.

		pool: the corresponding PooledDB instance
		con: the underlying SolidDB connection
		"""
		self._pool = pool
		self._con = con

	def close(self):
		"""Close the pooled dedicated connection."""
		# Instead of actually closing the connection,
		# return it to the pool for future reuse.
		if self._con:
			self._pool.cache(self._con)
			self._con = None

	def __getattr__(self, name):
		"""Proxy all members of the class."""
		if self._con:
			return getattr(self._con, name)
		else:
			raise InvalidConnection

	def __del__(self):
		"""Delete the pooled connection."""
		self.close()

class SharedDBConnection:
	"""Auxiliary class for shared connections."""

	def __init__(self, con):
		"""Create a shared connection.

		con: the underlying SolidDB connection
		"""
		self.con = con
		self.shared = 1

	def __cmp__(self, other):
		"""Compare how often the connections are shared."""
		return self.shared - other.shared

	def share(self):
		"""Increase the share of this connection."""
		self.shared += 1

	def unshare(self):
		"""Decrease the share of this connection."""
		self.shared -= 1

class PooledSharedDBConnection:
	"""Auxiliary proxy class for pooled shared connections."""

	def __init__(self, pool, shared_con):
		"""Create a pooled shared connection.

		pool: the corresponding PooledDB instance
		con: the underlying SharedDBConnection
		"""
		self._pool = pool
		self._shared_con = shared_con
		self._con = shared_con.con

	def close(self):
		"""Close the pooled shared connection."""
		# Instead of actually closing the connection,
		# unshare it and/or return it to the pool.
		if self._con:
			self._pool.unshare(self._shared_con)
			self._shared_con = self._con = None

	def __getattr__(self, name):
		"""Proxy all members of the class."""
		if self._con:
			return getattr(self._con, name)
		else:
			raise InvalidConnection

	def __del__(self):
		"""Delete the pooled connection."""
		self.close()
