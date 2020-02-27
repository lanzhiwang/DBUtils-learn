"""
Usage:

First you need to set up the database connection pool by creating
an instance of PooledDB, passing the following parameters:

    creator: either an arbitrary function returning new DB-API 2
        connection objects or a DB-API 2 compliant database module

    mincached: the initial number of idle connections in the pool
        (the default of 0 means no connections are made at startup)

    maxcached: the maximum number of idle connections in the pool
        (the default value of 0 or None means unlimited pool size)

    maxshared: maximum number of shared connections allowed
        (the default value of 0 or None means all connections are dedicated 专用)
        When this maximum number is reached, connections are
        shared if they have been requested as shareable.

    maxconnections: maximum number of connections generally allowed
        (the default value of 0 or None means any number of connections)

    blocking: determines behavior when exceeding the maximum
        (if this is set to true, block and wait until the number of
        connections decreases, but by default an error will be reported)
        确定超出最大值时的行为（如果将其设置为true，则阻止并等待直到连接数量减少，但默认情况下会报告错误）

    maxusage: maximum number of reuses of a single connection
        (the default of 0 or None means unlimited reuse)
        When this maximum usage number of the connection is reached,
        the connection is automatically reset (closed and reopened).

    setsession: an optional list of SQL commands that may serve to
        prepare the session, e.g. ["set datestyle to german", ...]

    reset: how connections should be reset when returned to the pool
        (False or None to rollback transcations started with begin(),
        the default value True always issues a rollback for safety's sake)
        返回池后应如何重置连接（对于以begin() 开始的回滚事务，为False或None，为安全起见，默认值True始终发出回滚）

    failures: an optional exception class or a tuple of exception classes
        for which the connection failover mechanism shall be applied,
        if the default (OperationalError, InternalError) is not adequate

    ping: an optional flag controlling when connections are checked
        with the ping() method if such a method is available
        (
            0 = None = never,
            1 = default = whenever fetched from the pool,
            2 = when a cursor is created,
            4 = when a query is executed,
            7 = always, and all other bit combinations of these values
        )

For instance, if you are using pgdb as your DB-API 2 database module and
want a pool of at least five connections to your local database 'mydb':

    import pgdb  # import used DB-API 2 module
    from DBUtils.PooledDB import PooledDB
    pool = PooledDB(pgdb, 5, database='mydb')

Once you have set up the connection pool you can request
database connections from that pool:

    db = pool.connection()

You can use these connections just as if they were ordinary
DB-API 2 connections.  Actually what you get is the hardened
SteadyDB version of the underlying DB-API 2 connection.
您可以像使用普通连接一样使用这些连接DB-API 2连接。 其实你得到的是硬化
基础DB-API 2连接的SteadyDB版本。

Please note that the connection may be shared with other threads
by default if you set a non-zero maxshared parameter and the DB-API 2
module allows this.  If you want to have a dedicated 专用 connection, use:

    db = pool.connection(shareable=False)

You can also use this to get a dedicated 专用 connection:

    db = pool.dedicated_connection()

If you don't need it any more, you should immediately return it to the
pool with db.close().  You can get another connection in the same way.

Warning: In a threaded environment, never do the following:

    pool.connection().cursor().execute(...)

This would release the connection too early for reuse which may be
fatal if the connections are not thread-safe.  Make sure that the
connection object stays alive as long as you are using it, like that:
这将太早释放连接而无法重用，如果连接不是线程安全的，这可能是会致命。 只要使用连接对象，
就要确保连接对象就会保持活动状态，如下所示：

    db = pool.connection()
    cur = db.cursor()
    cur.execute(...)
    res = cur.fetchone()
    cur.close()  # or del cur
    db.close()  # or del db

Note that you need to explicitly start transactions by calling the
begin() method.  This ensures that the connection will not be shared
with other threads, that the transparent reopening will be suspended
until the end of the transaction, and that the connection will be rolled
back before being given back to the connection pool.
请注意，您需要通过调用begin()方法明确开启一个事务。 这样可以确保
连接不会与其他线程共享，当事务结束时将透明重新打开连接，当连接返回给连接池时会被回滚。


mincached | maxcached
0           0
0           3
3           0
3           3
3           5

maxshared
0
2
3
4
5
6

mincached | maxcached | maxshared
0           0           0
0           0           3

0           3           0
0           3           2
0           3           3
0           3           4

3           0           0
3           0           2
3           0           3
3           0           5

3           3           0
3           3           2
3           3           3
3           3           4

3           5           0
3           5           2
3           5           3
3           5           4
3           5           5
3           5           6


mincached | maxcached | maxshared | maxconnections | connections
0           0           0           0
0           0           0           2

0           0           3           0
0           0           3           3
0           0           3           5

0           3           0           0
0           3           0           3
0           3           0           5

0           3           2           0
0           3           2           3
0           3           2           5

0           3           3           0
0           3           3           3
0           3           3           5

0           3           4           0
0           3           4           4
0           3           4           5

3           0           0           0
3           0           0           3
3           0           0           5

3           0           2           0
3           0           2           3
3           0           2           5

3           0           3           0
3           0           3           3
3           0           3           5

3           0           5           0
3           0           5           5
3           0           5           6

3           3           0           0
3           3           0           3
3           3           0           5

3           3           2           0
3           3           2           3
3           3           2           5

3           3           3           0
3           3           3           3
3           3           3           5

3           3           4           0
3           3           4           4
3           3           4           5

3           5           0           0
3           5           0           5
3           5           0           6

3           5           2           0
3           5           2           5
3           5           2           6

3           5           3           0
3           5           3           5
3           5           3           6

3           5           4           0
3           5           4           5
3           5           4           6

3           5           5           0
3           5           5           5
3           5           5           6

3           5           6           0
3           5           6           6
3           5           6           7

"""


from threading import Condition

from SteadyDB import connect

__version__ = '1.3'


class PooledDBError(Exception):
    """General PooledDB error."""


class InvalidConnection(PooledDBError):
    """Database connection is invalid."""


class NotSupportedError(PooledDBError):
    """DB-API module not supported by PooledDB."""


class TooManyConnections(PooledDBError):
    """Too many database connections were opened."""


class PooledDB:
    version = __version__

    def __init__(
            self, creator, mincached=0, maxcached=0,
            maxshared=0, maxconnections=0, blocking=False,
            maxusage=None, setsession=None, reset=True,
            failures=None, ping=1,
            *args, **kwargs):

        try:
            threadsafety = creator.threadsafety
        except AttributeError:
            try:
                if not callable(creator.connect):
                    raise AttributeError
            except AttributeError:
                threadsafety = 2
            else:
                threadsafety = 0
        if not threadsafety:
            raise NotSupportedError("Database module is not thread-safe.")

        self._creator = creator
        self._args, self._kwargs = args, kwargs
        self._blocking = blocking
        self._maxusage = maxusage
        self._setsession = setsession
        self._reset = reset
        self._failures = failures
        self._ping = ping

        if mincached is None:
            mincached = 0
        if maxcached is None:
            maxcached = 0
        if maxconnections is None:
            maxconnections = 0
        if maxcached:
            if maxcached < mincached:
                maxcached = mincached
            self._maxcached = maxcached
        else:
            self._maxcached = 0

        if threadsafety > 1 and maxshared:
            self._maxshared = maxshared
            self._shared_cache = []  # the cache for shared connections
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

        self._idle_cache = []  # the actual pool of idle connections
        self._lock = Condition()
        self._connections = 0
        idle = [self.dedicated_connection() for i in range(mincached)]
        while idle:
            idle.pop().close()

    def steady_connection(self):
        return connect(
            self._creator, self._maxusage, self._setsession,
            self._failures, self._ping, True, *self._args, **self._kwargs)

    def dedicated_connection(self):
        return self.connection(shareable=False)

    def connection(self, shareable=True):
        if shareable and self._maxshared:
            self._lock.acquire()
            try:
                while (not self._shared_cache) and self._maxconnections and self._connections >= self._maxconnections:
                    self._wait_lock()
                if len(self._shared_cache) < self._maxshared:
                    try:
                        con = self._idle_cache.pop(0)
                    except IndexError:
                        con = self.steady_connection()
                    else:
                        con._ping_check()
                    con = SharedDBConnection(con)
                    self._connections += 1
                else:
                    self._shared_cache.sort()
                    con = self._shared_cache.pop(0)
                    while con.con._transaction:
                        self._shared_cache.insert(0, con)
                        self._wait_lock()
                        self._shared_cache.sort()
                        con = self._shared_cache.pop(0)
                    con.con._ping_check()
                    con.share()
                self._shared_cache.append(con)
                self._lock.notify()
            finally:
                self._lock.release()
            con = PooledSharedDBConnection(self, con)
        else:
            self._lock.acquire()
            try:
                while self._maxconnections and self._connections >= self._maxconnections:
                    self._wait_lock()
                try:
                    con = self._idle_cache.pop(0)
                except IndexError:
                    con = self.steady_connection()
                else:
                    con._ping_check()
                con = PooledDedicatedDBConnection(self, con)
                self._connections += 1
            finally:
                self._lock.release()
        return con

    def cache(self, con):
        self._lock.acquire()
        try:
            if not self._maxcached:
                con._reset(force=self._reset)
                self._idle_cache.append(con)
            elif len(self._idle_cache) < self._maxcached:
                con._reset(force=self._reset)
                self._idle_cache.append(con)
            else:
                con.close()
            self._connections -= 1
            self._lock.notify()
        finally:
            self._lock.release()

    def unshare(self, con):
        self._lock.acquire()
        try:
            con.unshare()
            shared = con.shared
            if not shared:
                try:
                    self._shared_cache.remove(con)
                except ValueError:
                    pass
        finally:
            self._lock.release()

        if not shared:
            self.cache(con.con)

    def _wait_lock(self):
        """Wait until notified or report an error."""
        if not self._blocking:
            raise TooManyConnections
        self._lock.wait()

    def close(self):
        """Close all connections in the pool."""
        self._lock.acquire()
        try:
            while self._idle_cache:  # close all idle connections
                con = self._idle_cache.pop(0)
                try:
                    con.close()
                except Exception:
                    pass
            if self._maxshared:  # close all shared connections
                while self._shared_cache:
                    con = self._shared_cache.pop(0).con
                    try:
                        con.close()
                    except Exception:
                        pass
                    self._connections -= 1
            self._lock.notifyAll()
        finally:
            self._lock.release()

    def __del__(self):
        """Delete the pool."""
        try:
            self.close()
        except Exception:
            pass


class PooledDedicatedDBConnection:
    """Auxiliary proxy class for pooled dedicated connections."""

    def __init__(self, pool, con):
        """Create a pooled dedicated connection.

        pool: the corresponding PooledDB instance
        con: the underlying SteadyDB connection

        """
        # basic initialization to make finalizer work
        self._con = None
        # proper initialization of the connection
        if not con.threadsafety():
            raise NotSupportedError("Database module is not thread-safe.")
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
        try:
            self.close()
        except Exception:
            pass


class SharedDBConnection:
    """Auxiliary class for shared connections."""

    def __init__(self, con):
        """Create a shared connection.

        con: the underlying SteadyDB connection

        """
        self.con = con
        self.shared = 1

    def __lt__(self, other):
        if self.con._transaction == other.con._transaction:
            return self.shared < other.shared
        else:
            return not self.con._transaction

    def __le__(self, other):
        if self.con._transaction == other.con._transaction:
            return self.shared <= other.shared
        else:
            return not self.con._transaction

    def __eq__(self, other):
        return (self.con._transaction == other.con._transaction
                and self.shared == other.shared)

    def __ne__(self, other):
        return not self.__eq__(other)

    def __gt__(self, other):
        return other.__lt__(self)

    def __ge__(self, other):
        return other.__le__(self)

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
        # basic initialization to make finalizer work
        self._con = None
        # proper initialization of the connection
        con = shared_con.con
        if not con.threadsafety() > 1:
            raise NotSupportedError("Database connection is not thread-safe.")
        self._pool = pool
        self._shared_con = shared_con
        self._con = con

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
        try:
            self.close()
        except Exception:
            pass
