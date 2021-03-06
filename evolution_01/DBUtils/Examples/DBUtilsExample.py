from WebKit.Examples.ExamplePage import ExamplePage
from MiscUtils.Configurable import Configurable


class DBConfig(Configurable):
	"""Database configuration."""

	def defaultConfig(self):
		"""Get the default database configuration"""
		return {
			'dbapi': 'pg',
			'database': 'demo',
			'mincached': 5,
			'maxcached': 25
		}

	def configFilename(self):
		"""Get the name of the config file"""
		return 'Configs/Database.config'

# the database tables used in this example:
tables = ('''seminars (
    id varchar(4) primary key,
    title varchar(64) unique not null,
    cost money,
    places_left smallint)''',
'''attendees (
    name varchar(64) not null,
    seminar varchar(4),
    paid boolean,
    primary key(name, seminar),
    foreign key (seminar) references seminars(id) on delete cascade)''')

class DBUtilsExample(ExamplePage):
	"""Example page for the DBUtils package."""

	# Initialize the database class once when this class is loaded:
	config = DBConfig().config()
	if config.get('maxcached', None) is None:
		dbmod_name = 'Persistent'
	else:
		dbmod_name = 'Pooled'
	dbapi_name = config.get('dbapi', 'pg')
	if dbapi_name == 'pg': # use the PyGreSQL classic DB API
		dbmod_name += 'Pg'
		if config.has_key('database'):
			config['dbname'] = config['database']
			del config['database']
	else: # use a DB-API 2 compliant module
		dbmod_name += 'DB'
	dbapi = dbmod = dbclass = dbstatus = None
	try:
		dbapi = __import__(dbapi_name)
		try:
			dbmod = getattr(__import__('DBUtils.' + dbmod_name), dbmod_name)
			try:
				if dbapi_name == 'pg':
					del config['dbapi']
				else:
					config['dbapi'] = dbapi
				dbclass = getattr(dbmod, dbmod_name)(**config)
			except dbapi.Error, error:
				dbstatus = str(error)
			except:
				dbstatus = 'Could not connect to the database.'
		except:
			dbstatus = 'Could not import DBUtils.%s.' % dbmod_name
	except:
		dbstatus = 'Could not import %s.' % dbapi_name

	# Initialize the buttons
	_actions = [ 'create_tables',
		'list_seminars', 'list_attendees',
		'new_seminar', 'new_attendee']
	buttons = []
	for action in (_actions):
		value = action.replace('_', ' ').capitalize()
		buttons.append('<input name="_action_%s" '
			'type="submit" value="%s">' % (action, value))
	buttons = tuple(buttons)

	def title(self):
		return "DBUtils Example"

	def actions(self):
		return ExamplePage.actions(self) + self._actions

	def awake(self, transaction):
		ExamplePage.awake(self, transaction)
		self.output = []

	def postAction(self, actionName):
		self.writeBody()
		del self.output
		ExamplePage.postAction(self, actionName)

	def outputMsg(self, msg, error=0):
		self.output.append('<p style="color:%s">%s</p>'
			% (error and 'red' or 'green', msg))

	def connection(self, shareable=1):
		if self.dbstatus:
			error = self.dbstatus
		else:
			try:
				if self.dbmod_name == 'PooledDB':
					return self.dbclass.connection(shareable)
				else:
					return self.dbclass.connection()
			except self.dbapi.Error, error:
				error = str(error)
			except:
				error = 'Cannot connect to the database.'
		self.outputMsg(error, 1)

	def sqlEncode(self, s):
		if s is None: return 'null'
		s = s.replace('\\','\\\\').replace('\'','\\\'')
		return "'%s'" % s

	def create_tables(self):
		db = self.connection(0)
		if not db: return
		for table in tables:
			self.output.append('<p>Creating the following table:</p>'
				'<pre>%s</pre>' % table)
			ddl = 'create table ' + table
			try:
				if self.dbapi_name == 'pg':
					db.query(ddl)
				else:
					db.cursor().execute(ddl)
					db.commit()
			except self.dbapi.Error, error:
				if self.dbapi_name != 'pg':
					db.rollback()
				self.outputMsg(error , 1)
			else:
				self.outputMsg('The table was successfully created.')
		db.close()

	def list_seminars(self):
		id = self.request().field('id', None)
		if id:
			if type(id) != type([]):
				id = [id]
			cmd = ','.join(map(self.sqlEncode, id))
			cmd = 'delete from seminars where id in (%s)' % cmd
			db = self.connection(0)
			if not db: return
			try:
				if self.dbapi_name == 'pg':
					db.query('begin')
					db.query(cmd)
					db.query('end')
				else:
					db.cursor().execute(cmd)
					db.commit()
			except self.dbapi.Error, error:
				try:
					if self.dbapi_name == 'pg':
						db.query('end')
					else:
						db.rollback()
				except:
					pass
				self.outputMsg(error , 1)
				return
			else:
				self.outputMsg('Entries deleted: %d' % len(id))
		db = self.connection()
		if not db: return
		query = ('select id, title, cost, places_left from seminars '
			'order by title')
		try:
			if self.dbapi_name == 'pg':
				result = db.query(query).getresult()
			else:
				cursor = db.cursor()
				cursor.execute(query)
				result = cursor.fetchall()
				cursor.close()
		except self.dbapi.Error, error:
			self.outputMsg(error , 1)
			return
		if not result:
			self.outputMsg('There are no seminars in the database.', 1)
			return
		wr = self.output.append
		button = self.buttons[1].replace('List seminars', 'Delete')
		wr('<h4>List of seminars in the database:</h4>')
		wr('<form><table border="1" cellspacing="0" cellpadding="2">'
			'<tr><th>ID</th><th>Seminar title</th><th>Cost</th><th>Places left</th>'
			'<th>%s</th></tr>' % button)
		for id, title, cost, places in result:
			if places is None:
				places = 'unlimited'
			if not cost:
				cost = 'free'
			wr('<tr><td>%s</td><td>%s</td><td>%s</td><td>%s</td><td>'
				'<input type="checkbox" name="id" value="%s">'
				'</td></tr>' % (id, title, cost, places, id))
		wr('</table></form>')

	def list_attendees(self):
		id = self.request().field('id', None)
		if id:
			if type(id) != type([]):
				id = [id]
			cmd = ','.join(map(self.sqlEncode, id))
			cmd = ['delete from attendees '
				'where rpad(seminar,4)||name in (%s)' % cmd]
			places = {}
			for i in id:
				i = i[:4].rstrip()
				if places.has_key(i):
					places[i] += 1
				else:
					places[i] = 1
			for i, n in places.items():
				cmd.append("update seminars set places_left=places_left+%d "
				"where id=%s" % (n, self.sqlEncode(i)))
			db = self.connection(0)
			if not db: return
			try:
				if self.dbapi_name == 'pg':
					db.query('begin')
					for c in cmd:
						db.query(c)
					db.query('end')
				else:
					for c in cmd:
						db.cursor().execute(c)
					db.commit()
			except self.dbapi.Error, error:
				if self.dbapi_name == 'pg':
					db.query('end')
				else:
					db.rollback()
				self.outputMsg(error , 1)
				return
			else:
				self.outputMsg('Entries deleted: %d' % len(id))
		db = self.connection()
		if not db: return
		query = ('select a.name, s.id, s.title, a.paid '
			' from attendees a,seminars s'
			' where s.id=a.seminar'
			' order by a.name, s.title')
		try:
			if self.dbapi_name == 'pg':
				result = db.query(query).getresult()
			else:
				cursor = db.cursor()
				cursor.execute(query)
				result = cursor.fetchall()
				cursor.close()
		except self.dbapi.Error, error:
			self.outputMsg(error , 1)
			return
		if not result:
			self.outputMsg('There are no attendees in the database.' ,1)
			return
		wr = self.output.append
		button = self.buttons[2].replace('List attendees', 'Delete')
		wr('<h4>List of attendees in the database:</h4>')
		wr('<form><table border="1" cellspacing="0" cellpadding="2">'
			'<tr><th>Name</th><th>Seminar</th><th>Paid</th>'
			'<th>%s</th></tr>' % button)
		for name, id, title, paid in result:
			paid = paid and 'Yes' or 'No'
			id = id.ljust(4) + name
			wr('<tr><td>%s</td><td>%s</td><td>%s</td>'
				'<td><input type="checkbox" name="id" value="%s"></td>'
				'</tr>' % (name, title, paid, id))
		wr('</table></form>')

	def new_seminar(self):
		wr = self.output.append
		wr('<h4>Create a new seminar entry in the database:</h4>')
		wr('<form><table>'
			'<tr><th>ID</th><td><input name="id" type="text" '
			'size="4" maxlength="4"></td></tr>'
			'<tr><th>Title</th><td><input name="title" type="text" '
			'size="40" maxlength="64"></td></tr>'
			'<tr><th>Cost</th><td><input name="cost" type="text" '
			'size="20" maxlength="20"></td></tr>'
			'<tr><th>Places</th><td><input name="places" type="text" '
			'size="20" maxlength="20"></td></tr>'
			'<td colspan="2" align="right">%s</td>'
			'</table></form>' % self.buttons[3])
		request = self.request()
		if not request.hasField('id'): return
		values = []
		for name in ('id', 'title', 'cost', 'places'):
			values.append(request.field(name, '').strip())
		if not values[0] or not values[1]:
			self.outputMsg('You must enter a seminar ID and a title!')
			return
		if not values[2]: values[2] = None
		if not values[3]: values[3] = None
		db = self.connection(0)
		if not db: return
		cmd = ('insert into seminars values (%s,%s,%s,%s)'
			% tuple(map(self.sqlEncode, values)))
		try:
			if self.dbapi_name == 'pg':
				db.query('begin')
				db.query(cmd)
				db.query('end')
			else:
				db.cursor().execute(cmd)
				db.commit()
		except self.dbapi.Error, error:
			if self.dbapi_name == 'pg':
				db.query('end')
			else:
				db.rollback()
			self.outputMsg(error , 1)
		else:
			self.outputMsg('"%s" added to seminars.' % values[1])
		db.close()

	def new_attendee(self):
		db = self.connection()
		if not db: return
		query = ('select id, title from seminars '
			'where places_left is null or places_left>0 order by title')
		try:
			if self.dbapi_name == 'pg':
				result = db.query(query).getresult()
			else:
				cursor = db.cursor()
				cursor.execute(query)
				result = cursor.fetchall()
				cursor.close()
		except self.dbapi.Error, error:
			self.outputMsg(error , 1)
			return
		if not result:
			self.outputMsg('You have to define seminars first.')
			return
		sem = ['<select name="seminar" size="1"']
		for id, title in result:
			sem.append('<option value="%s">%s</option>' % (id, title))
		sem.append('</select>')
		sem = ''.join(sem)
		wr = self.output.append
		wr('<h4>Create a new attendee entry in the database:</h4>')
		wr('<form><table>'
			'<tr><th>Name</th><td><input name="name" type="text" '
			'size="40" maxlength="64"></td></tr>'
			'<tr><th>Seminar</th><td>%s</td></tr>'
			'<tr><th>Paid</th><td>'
			'<input type="radio" name="paid" value="t">Yes '
			'<input type="radio" name="paid" value="f" checked="1">No'
			'</td></tr><td colspan="2" align="right">%s</td>'
			'</table></form>' % (sem, self.buttons[4]))
		request = self.request()
		if not request.hasField('name'): return
		values = []
		for name in ('name', 'seminar', 'paid'):
			values.append(request.field(name, '').strip())
		if not values[0] or not values[1]:
			self.outputMsg('You must enter a name and a seminar!')
			return
		db = self.connection(0)
		if not db: return
		try:
			if self.dbapi_name == 'pg':
				db.query('begin')
				cmd = ("update seminars set places_left=places_left-1 "
					"where id=%s" % self.sqlEncode(values[1]))
				db.query(cmd)
				cmd = ("select places_left from seminars "
					"where id=%s" % self.sqlEncode(values[1]))
				if (db.query(cmd).getresult()[0][0] or 0) < 0:
					raise self.dbapi.Error, 'No more places left.'
				cmd = ("insert into attendees values (%s,%s,%s)"
					% tuple(map(self.sqlEncode, values)))
				db.query(cmd)
				db.query('end')
			else:
				cursor = db.cursor()
				cmd = ("update seminars set places_left=places_left-1 "
					"where id=%s" % self.sqlEncode(values[1]))
				cursor.execute(cmd)
				cmd = ("select places_left from seminars "
					"where id=%s" % self.sqlEncode(values[1]))
				cursor.execute(cmd)
				if (cursor.fetchone()[0] or 0) < 0:
					raise self.dbapi.Error, 'No more places left.'
				cmd = ("insert into attendees values (%s,%s,%s)"
					% tuple(map(self.sqlEncode, values)))
				db.cursor().execute(cmd)
				cursor.close()
				db.commit()
		except self.dbapi.Error, error:
			if self.dbapi_name == 'pg':
				db.query('end')
			else:
				db.rollback()
			self.outputMsg(error , 1)
		else:
			self.outputMsg('%s added to attendees.' % values[0])
		db.close()

	def writeContent(self):
		wr = self.writeln
		if self.output:
			wr('\n'.join(self.output))
			wr('<p><a href="DBUtilsExample">Back</a></p>')
		else:
			wr('<h2>Welcome to the %s!</h2>' % self.title())
			wr('<h4>We are using DBUtils.%s and the %s database module.</h4>'
				% (self.dbmod_name, self.dbapi_name))
			wr('<p>Configuration: %r</p>' % DBConfig().config())
			wr('<p>This example uses a small demo database '
				'designed to track the attendees for a series of seminars '
				'(see <a href="http://www.linuxjournal.com/article/2605">"The '
				'Python DB-API"</a> by Andrew Kuchling).</p>')
			wr('<form>'
				'<p>%s (create the needed database tables first)</p>'
				'<p>%s %s (list all database entries)</p>'
				'<p>%s %s (create new entries)</p>'
				'</form>' % self.buttons)
