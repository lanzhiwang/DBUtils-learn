"""
Usage:
    import pgdb  # import used DB-API 2 module
    from DBUtils.SteadyDB import connect
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

    creator: either an arbitrary function returning new DB-API 2 compliant
        connection objects or a DB-API 2 compliant database module

    maxusage: maximum usage limit for the underlying DB-API 2 connection
        (number of database operations, 0 or None means unlimited usage)
        callproc(), execute() and executemany() count as one operation.
        When the limit is reached, the connection is automatically reset.
        基础DB-API 2连接的最大使用限制（数据库操作数，0或无表示无限使用）
        callproc()，execute()和executemany()计为一项操作。
        达到限制后，连接将自动重置。

    setsession: an optional list of SQL commands that may serve to prepare
        the session, e.g. ["set datestyle to german", "set time zone mez"]

    failures: an optional exception class or a tuple of exception classes
        for which the failover mechanism shall be applied, if the default
        (OperationalError, InternalError) is not adequate
        可选的异常类或异常类的元组，发生指定异常将使用故障转移机制

    ping: determines when the connection should be checked with ping()
        确定何时应使用ping()检查连接
        (
            0 = None = never,
            1 = default = when _ping_check() is called,
            2 = whenever a cursor is created,
            4 = when a query is executed,
            7 = always, and all other bit combinations of these values  始终，以及这些值的所有其他位组合
        )

    closeable: if this is set to false, then closing the connection will
        be silently ignored, but by default the connection can be closed
        如果将其设置为false，则关闭连接将被默默忽略，但默认情况下可以关闭连接

    args, kwargs: the parameters that shall be passed to the creator
        function or the connection constructor of the DB-API 2 module
"""

import sys

__version__ = '1.3'

try:
    baseint = (int, long)
except NameError:  # Python 3
    baseint = int


class SteadyDBError(Exception):
    pass


class InvalidCursor(SteadyDBError):
    pass


def connect(
        creator, maxusage=None, setsession=None,
        failures=None, ping=1, closeable=True, *args, **kwargs):
    return SteadyDBConnection(
        creator, maxusage, setsession,
        failures, ping, closeable, *args, **kwargs)


class SteadyDBConnection:
    version = __version__
    def __init__(
            self, creator, maxusage=None, setsession=None,
            failures=None, ping=1, closeable=True, *args, **kwargs):
        self._con = None
        self._closed = True

        try:
            self._creator = creator.connect
            self._dbapi = creator
        except AttributeError:
            self._creator = creator
            try:
                self._dbapi = creator.dbapi
            except AttributeError:
                try:
                    self._dbapi = sys.modules[creator.__module__]
                    if self._dbapi.connect != creator:
                        raise AttributeError
                except (AttributeError, KeyError):
                    self._dbapi = None
        if not callable(self._creator):
            raise TypeError("%r is not a connection provider." % (creator,))

        try:
            self._threadsafety = creator.threadsafety
        except AttributeError:
            try:
                self._threadsafety = self._dbapi.threadsafety
            except AttributeError:
                self._threadsafety = None

        if maxusage is None:
            maxusage = 0
        if not isinstance(maxusage, baseint):
            raise TypeError("'maxusage' must be an integer value.")
        self._maxusage = maxusage

        self._setsession_sql = setsession

        if failures is not None and not isinstance(
                failures, tuple) and not issubclass(failures, Exception):
            raise TypeError("'failures' must be a tuple of exceptions.")
        self._failures = failures

        self._ping = ping if isinstance(ping, int) else 0
        self._closeable = closeable
        self._args, self._kwargs = args, kwargs

        self._store(self._create())

    def _create(self):
        con = self._creator(*self._args, **self._kwargs)
        try:
            try:
                if self._dbapi.connect != self._creator:
                    raise AttributeError
            except AttributeError:
                try:
                    mod = con.__module__
                except AttributeError:
                    mod = None
                while mod:
                    try:
                        self._dbapi = sys.modules[mod]
                        if not callable(self._dbapi.connect):
                            raise AttributeError
                    except (AttributeError, KeyError):
                        pass
                    else:
                        break
                    i = mod.rfind('.')
                    if i < 0:
                        mod = None
                    else:
                        mod = mod[:i]
                else:
                    try:
                        mod = con.OperationalError.__module__
                    except AttributeError:
                        mod = None
                    while mod:
                        try:
                            self._dbapi = sys.modules[mod]
                            if not callable(self._dbapi.connect):
                                raise AttributeError
                        except (AttributeError, KeyError):
                            pass
                        else:
                            break
                        i = mod.rfind('.')
                        if i < 0:
                            mod = None
                        else:
                            mod = mod[:i]
                    else:
                        self._dbapi = None
            if self._threadsafety is None:
                try:
                    self._threadsafety = self._dbapi.threadsafety
                except AttributeError:
                    try:
                        self._threadsafety = con.threadsafety
                    except AttributeError:
                        pass
            if self._failures is None:
                try:
                    self._failures = (self._dbapi.OperationalError, self._dbapi.InternalError)
                except AttributeError:
                    try:
                        self._failures = (self._creator.OperationalError, self._creator.InternalError)
                    except AttributeError:
                        try:
                            self._failures = (con.OperationalError, con.InternalError)
                        except AttributeError:
                            raise AttributeError("Could not determine failure exceptions (please set failures or creator.dbapi).")
            if isinstance(self._failures, tuple):
                self._failure = self._failures[0]
            else:
                self._failure = self._failures
            self._setsession(con)
        except Exception as error:
            try:  # close the connection first
                con.close()
            except Exception:
                pass
            raise error  # re-raise the original error again
        return con

    def _setsession(self, con=None):
        if con is None:
            con = self._con
        if self._setsession_sql:
            cursor = con.cursor()
            for sql in self._setsession_sql:
                cursor.execute(sql)
            cursor.close()

    def _store(self, con):
        """Store a database connection for subsequent use."""
        self._con = con
        self._transaction = False
        self._closed = False
        self._usage = 0

    def dbapi(self):
        if self._dbapi is None:
            raise AttributeError("Could not determine DB-API 2 module (please set creator.dbapi).")
        return self._dbapi

    def threadsafety(self):
        if self._threadsafety is None:
            if self._dbapi is None:
                raise AttributeError("Could not determine threadsafety (please set creator.dbapi or creator.threadsafety).")
            return 0
        return self._threadsafety

    def begin(self, *args, **kwargs):
        self._transaction = True
        try:
            begin = self._con.begin
        except AttributeError:
            pass
        else:
            begin(*args, **kwargs)

    def commit(self):
        self._transaction = False
        try:
            self._con.commit()
        except self._failures as error:  # cannot commit
            try:  # try to reopen the connection
                con = self._create()
            except Exception:
                pass
            else:
                self._close()
                self._store(con)
            raise error  # re-raise the original error

    def rollback(self):
        self._transaction = False
        try:
            self._con.rollback()
        except self._failures as error:  # cannot rollback
            try:  # try to reopen the connection
                con = self._create()
            except Exception:
                pass
            else:
                self._close()
                self._store(con)
            raise error  # re-raise the original error

    def cancel(self):
        self._transaction = False
        try:
            cancel = self._con.cancel
        except AttributeError:
            pass
        else:
            cancel()

    def close(self):
        if self._closeable:
            self._close()
        elif self._transaction:
            self._reset()

    def _close(self):
        if not self._closed:
            try:
                self._con.close()
            except Exception:
                pass
            self._transaction = False
            self._closed = True

    def _reset(self, force=False):
        if not self._closed and (force or self._transaction):
            try:
                self.rollback()
            except Exception:
                pass

    def ping(self, *args, **kwargs):
        return self._con.ping(*args, **kwargs)

    def cursor(self, *args, **kwargs):
        return SteadyDBCursor(self, *args, **kwargs)

    def _cursor(self, *args, **kwargs):
        transaction = self._transaction
        if not transaction:  # transaction = False
            self._ping_check(2)
        try:
            if self._maxusage and self._usage >= self._maxusage:
                raise self._failure
            cursor = self._con.cursor(*args, **kwargs)  # try to get a cursor
        except self._failures as error:  # error in getting cursor
            try:  # try to reopen the connection
                con = self._create()
            except Exception:
                pass
            else:
                try:  # and try one more time to get a cursor
                    cursor = con.cursor(*args, **kwargs)
                except Exception:
                    pass
                else:
                    self._close()
                    self._store(con)
                    if transaction:
                        raise error  # re-raise the original error again
                    return cursor
                try:
                    con.close()
                except Exception:
                    pass
            if transaction:
                self._transaction = False
            raise error  # re-raise the original error again
        return cursor

    def _ping_check(self, ping=1, reconnect=True):
        if ping & self._ping:
            try:  # if possible, ping the connection
                try:  # pass a reconnect=False flag if this is supported
                    alive = self._con.ping(False)
                except TypeError:  # the reconnect flag is not supported
                    alive = self._con.ping()
            except (AttributeError, IndexError, TypeError, ValueError):
                self._ping = 0  # ping() is not available
                alive = None
                reconnect = False
            except Exception:
                alive = False
            else:
                if alive is None:
                    alive = True
                if alive:
                    reconnect = False
            if reconnect and not self._transaction:
                try:  # try to reopen the connection
                    con = self._create()
                except Exception:
                    pass
                else:
                    self._close()
                    self._store(con)
                    alive = True
            return alive

    def __del__(self):
        try:
            self._close()  # make sure the connection is closed
        except Exception:
            pass

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        if exc[0] is None and exc[1] is None and exc[2] is None:
            self.commit()
        else:
            self.rollback()


class SteadyDBCursor:
    def __init__(self, con, *args, **kwargs):
        self._cursor = None
        self._closed = True
        self._con = con
        self._args, self._kwargs = args, kwargs
        self._clearsizes()
        try:
            self._cursor = con._cursor(*args, **kwargs)
        except AttributeError:
            raise TypeError("%r is not a SteadyDBConnection." % (con,))
        self._closed = False

    def _clearsizes(self):
        self._inputsizes = []
        self._outputsizes = {}

    def setinputsizes(self, sizes):
        self._inputsizes = sizes

    def setoutputsize(self, size, column=None):
        self._outputsizes[column] = size

    def __getattr__(self, name):
        if self._cursor:
            if name.startswith(('execute', 'call')):
                return self._get_tough_method(name)
            else:
                return getattr(self._cursor, name)
        else:
            raise InvalidCursor

    def _get_tough_method(self, name):
        # name.startswith(('execute', 'call'))
        def tough_method(*args, **kwargs):
            execute = name.startswith('execute')
            con = self._con
            transaction = con._transaction
            if not transaction:  # transaction = False
                con._ping_check(4)
            try:
                if con._maxusage and con._usage >= con._maxusage:
                    raise con._failure
                if execute:
                    self._setsizes()
                method = getattr(self._cursor, name)
                result = method(*args, **kwargs)  # try to execute
                if execute:
                    self._clearsizes()
            except con._failures as error:  # execution error
                if not transaction:
                    try:
                        cursor2 = con._cursor(*self._args, **self._kwargs)  # open new cursor
                    except Exception:
                        pass
                    else:
                        try:  # and try one more time to execute
                            if execute:
                                self._setsizes(cursor2)
                            method = getattr(cursor2, name)
                            result = method(*args, **kwargs)
                            if execute:
                                self._clearsizes()
                        except Exception:
                            pass
                        else:
                            self.close()
                            self._cursor = cursor2
                            con._usage += 1
                            return result
                        try:
                            cursor2.close()
                        except Exception:
                            pass
                try:  # try to reopen the connection
                    con2 = con._create()
                except Exception:
                    pass
                else:
                    try:
                        cursor2 = con2.cursor(
                            *self._args, **self._kwargs)  # open new cursor
                    except Exception:
                        pass
                    else:
                        if transaction:
                            self.close()
                            con._close()
                            con._store(con2)
                            self._cursor = cursor2
                            raise error  # raise the original error again
                        error2 = None
                        try:  # try one more time to execute
                            if execute:
                                self._setsizes(cursor2)
                            method2 = getattr(cursor2, name)
                            result = method2(*args, **kwargs)
                            if execute:
                                self._clearsizes()
                        except error.__class__:  # same execution error
                            use2 = False
                            error2 = error
                        except Exception as error:  # other execution errors
                            use2 = True
                            error2 = error
                        else:
                            use2 = True
                        if use2:
                            self.close()
                            con._close()
                            con._store(con2)
                            self._cursor = cursor2
                            con._usage += 1
                            if error2:
                                raise error2  # raise the other error
                            return result
                        try:
                            cursor2.close()
                        except Exception:
                            pass
                    try:
                        con2.close()
                    except Exception:
                        pass
                if transaction:
                    self._transaction = False
                raise error  # re-raise the original error again
            else:
                con._usage += 1
                return result
        return tough_method

    def _setsizes(self, cursor=None):
        if cursor is None:
            cursor = self._cursor
        if self._inputsizes:
            cursor.setinputsizes(self._inputsizes)
        for column, size in self._outputsizes.items():
            if column is None:
                cursor.setoutputsize(size)
            else:
                cursor.setoutputsize(size, column)

    def close(self):
        if not self._closed:
            try:
                self._cursor.close()
            except Exception:
                pass
            self._closed = True

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        try:
            self.close()  # make sure the cursor is closed
        except Exception:
            pass
