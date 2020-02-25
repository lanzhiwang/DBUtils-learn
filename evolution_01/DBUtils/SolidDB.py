"""
	import pgdb # import used DB-API 2 module
	from SolidDB import connect
	db = connect(pgdb, 10000, ["set datestyle to german"],
		host=..., database=..., user=..., ...)
	...
	cursor = db.cursor()
	...
	cursor.execute('select ...')
	result = cursor.fetchall()
	...
	cursor.close()
	...
	db.close()

"""

__version__ = '0.8.1'
__revision__ = "$Rev$"
__date__ = "$Date$"


def connect(dbapi, maxusage=0, setsession=None, *args, **kwargs):
	return SolidDBConnection(dbapi, maxusage, setsession, *args, **kwargs)


class SolidDBConnection:
	"""
	SolidDBConnection(pgdb, 10000, ["set datestyle to german"],
		host=..., database=..., user=..., ...)
	"""

	def __init__(self, dbapi, maxusage=0, setsession=None, *args, **kwargs):
		""""Create a "tough" DB-API 2 connection."""
		self._dbapi = dbapi
		self._maxusage = maxusage
		self._setsession_sql = setsession
		self._args, self._kwargs = args, kwargs
		self._usage = 0
		self._con = dbapi.connect(*args, **kwargs)
		self._setsession()

	def _setsession(self, con=None):
		"""Execute the SQL commands for session preparation."""
		if con is None:
			con = self._con
		if self._setsession_sql:
			cursor = con.cursor()
			for sql in self._setsession_sql:
				cursor.execute(sql)
			cursor.close()

	def close(self):
		"""Close the tough connection.

		You are allowed to close a tough connection.
		It will not complain if you close it more than once.
		"""
		try:
			self._con.close()
		except:
			pass
		self._usage = 0

	def commit(self):
		"""Commit any pending transaction."""
		self._con.commit()

	def rollback(self):
		"""Rollback pending transaction."""
		self._con.rollback()

	def _cursor(self):
		"""A "tough" version of the method cursor()."""
		try:
			if self._maxusage:
				if self._usage >= self._maxusage:
					# the connection was used too often
					raise self._dbapi.OperationalError
			r = self._con.cursor() # try to get a cursor
		except (self._dbapi.OperationalError, self._dbapi.InternalError): # error in getting cursor
			try: # try to reopen the connection
				con2 = self._dbapi.connect(*self._args, **self._kwargs)
				self._setsession(con2)
			except:
				pass
			else:
				try: # and try one more time to get a cursor
					r = con2.cursor()
				except:
					pass
				else:
					self.close()
					self._con = con2
					return r
				try:
					con2.close()
				except:
					pass
			raise # raise the original error again
		return r

	def cursor(self):
		"""Return a new Cursor Object using the connection."""
		return SolidDBCursor(self)


class SolidDBCursor:
	"""A "tough" version of DB-API 2 cursors."""

	def __init__(self, con):
		""""Create a "tough" DB-API 2 cursor."""
		self._con = con
		self._clearsizes()
		self._cursor = con._cursor()

	def setinputsizes(self, sizes):
		"""Store input sizes in case cursor needs to be reopened."""
		self._inputsize = sizes

	def setoutputsize(self, size, column=None):
		"""Store output sizes in case cursor needs to be reopened."""
		if self._outputsize is None or column is None:
			self._outputsize = [(column, size)]
		else:
			self._outputsize.append(column, size)

	def _clearsizes(self):
		"""Clear stored input sizes."""
		self._inputsize = self._outputsize = None

	def _setsizes(self, cursor=None):
		"""Set stored input and output sizes for cursor execution."""
		if cursor is None:
			cursor = self._cursor
		if self._inputsize is not None:
			cursor.setinputsizes(self._inputsize)
		if self._outputsize is not None:
			for column, size in self._outputsize:
				if column is None:
					cursor.setoutputsize(size)
				else:
					cursor.setoutputsize(size, column)

	def close(self):
		"""Close the tough cursor.

		It will not complain if you close it more than once.
		"""
		try:
			self._cursor.close()
		except:
			pass

	def _get_tough_method(self, name):
		"""Return a "tough" version of the method."""
		def tough_method(*args, **kwargs):
			execute = name.startswith('execute')
			try:
				if self._con._maxusage:
					if self._con._usage >= self._con._maxusage:
						# the connection was used too often
						raise self._con._dbapi.OperationalError
				if execute:
					self._setsizes()
				method = getattr(self._cursor, name)
				r = method(*args, **kwargs) # try to execute
				if execute:
					self._clearsizes()
			except (self._con._dbapi.OperationalError,
				self._con._dbapi.InternalError): # execution error
				try:
					cursor2 = self._con._cursor() # open new cursor
				except:
					pass
				else:
					try: # and try one more time to execute
						if execute:
							self._setsizes(cursor2)
						method = getattr(cursor2, name)
						r = method(*args, **kwargs)
						if execute:
							self._clearsizes()
					except:
						pass
					else:
						self.close()
						self._cursor = cursor2
						self._con._usage += 1
						return r
					try:
						cursor2.close()
					except:
						pass
				try: # try to reopen the connection
					con2 = self._con._dbapi.connect(*self._con._args, **self._con._kwargs)
					self._con._setsession(con2)
				except:
					pass
				else:
					try:
						cursor2 = con2.cursor() # open new cursor
					except:
						pass
					else:
						try: # try one more time to execute
							if execute:
								self._setsizes(cursor2)
							method2 = getattr(cursor2, name)
							r = method2(*args, **kwargs)
							if execute:
								self._clearsizes()
						except:
							pass
						else:
							self.close()
							self._con.close()
							self._con._con, self._cursor = con2, cursor2
							self._con._usage += 1
							return r
						try:
							cursor2.close()
						except:
							pass
					try:
						con2.close()
					except:
						pass
				raise # raise the original error again
			else:
				self._con._usage += 1
				return r
		return tough_method

	def __getattr__(self, name):
		"""Inherit methods and attributes of underlying cursor"""
		if name.startswith('execute') or name.startswith('call'):
			# make execution methods "tough"
			return self._get_tough_method(name)
		else:
			return getattr(self._cursor, name)
