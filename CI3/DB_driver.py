
/**
 * CodeIgniter
 *
 * An open source application development framework for PHP
 *
 * This content is released under the MIT License (MIT)
 *
 * Copyright (c) 2014 - 2019, British Columbia Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 *
 * @package	CodeIgniter
 * @author	EllisLab Dev Team
 * @copyright	Copyright (c) 2008 - 2014, EllisLab, Inc. (https://ellislab.com/)
 * @copyright	Copyright (c) 2014 - 2019, British Columbia Institute of Technology (https://bcit.ca/)
 * @license	https://opensource.org/licenses/MIT	MIT License
 * @link	https://codeigniter.com
 * @since	Version 1.0.0
 * @filesource
 */
defined('BASEPATH') OR exit('No direct script access allowed')

/**
 * Database Driver Class
 *
 * This is the platform-independent base DB implementation class.
 * This class will not be called directly. Rather, the adapter
 * class for the specific database will extend and instantiate it.
 *
 * @package		CodeIgniter
 * @subpackage	Drivers
 * @category	Database
 * @author		EllisLab Dev Team
 * @link		https://codeigniter.com/user_guide/database/
 */
abstract class CI_DB_driver {

	/**
	 * Data Source Name / Connect string
	 *
	 * @var	string
	 */

	/**
	 * Username
	 *
	 * @var	string
	 */

	/**
	 * Password
	 *
	 * @var	string
	 */

	/**
	 * Hostname
	 *
	 * @var	string
	 */

	/**
	 * Database name
	 *
	 * @var	string
	 */

	/**
	 * Database driver
	 *
	 * @var	string
	 */

	/**
	 * Sub-driver
	 *
	 * @used-by	CI_DB_pdo_driver
	 * @var	string
	 */

	/**
	 * Table prefix
	 *
	 * @var	string
	 */

	/**
	 * Character set
	 *
	 * @var	string
	 */

	/**
	 * Collation
	 *
	 * @var	string
	 */

	/**
	 * Encryption flag/data
	 *
	 * @var	mixed
	 */

	/**
	 * Swap Prefix
	 *
	 * @var	string
	 */

	/**
	 * Database port
	 *
	 * @var	int
	 */

	/**
	 * Persistent connection flag
	 *
	 * @var	bool
	 */

	/**
	 * Connection ID
	 *
	 * @var	object|resource
	 */

	/**
	 * Result ID
	 *
	 * @var	object|resource
	 */

	/**
	 * Debug flag
	 *
	 * Whether to display error messages.
	 *
	 * @var	bool
	 */

	/**
	 * Benchmark time
	 *
	 * @var	int
	 */

	/**
	 * Executed queries count
	 *
	 * @var	int
	 */

	/**
	 * Bind marker
	 *
	 * Character used to identify values in a prepared statement.
	 *
	 * @var	string
	 */

	/**
	 * Save queries flag
	 *
	 * Whether to keep an in-memory history of queries for debugging purposes.
	 *
	 * @var	bool
	 */

	/**
	 * Queries list
	 *
	 * @see	CI_DB_driver.save_queries
	 * @var	string[]
	 */

	/**
	 * Query times
	 *
	 * A list of times that queries took to execute.
	 *
	 * @var	array
	 */

	/**
	 * Data cache
	 *
	 * An internal generic value cache.
	 *
	 * @var	array
	 */

	/**
	 * Transaction enabled flag
	 *
	 * @var	bool
	 */

	/**
	 * Strict transaction mode flag
	 *
	 * @var	bool
	 */

	/**
	 * Transaction depth level
	 *
	 * @var	int
	 */

	/**
	 * Transaction status flag
	 *
	 * Used with transactions to determine if a rollback should occur.
	 *
	 * @var	bool
	 */

	/**
	 * Transaction failure flag
	 *
	 * Used with transactions to determine if a transaction has failed.
	 *
	 * @var	bool
	 */

	/**
	 * Cache On flag
	 *
	 * @var	bool
	 */

	/**
	 * Cache directory path
	 *
	 * @var	bool
	 */

	/**
	 * Cache auto-delete flag
	 *
	 * @var	bool
	 */

	/**
	 * DB Cache object
	 *
	 * @see	CI_DB_cache
	 * @var	object
	 */

	/**
	 * Protect identifiers flag
	 *
	 * @var	bool
	 */

	/**
	 * List of reserved identifiers
	 *
	 * Identifiers that must NOT be escaped.
	 *
	 * @var	string[]
	 */

	/**
	 * Identifier escape character
	 *
	 * @var	string
	 */

	/**
	 * ESCAPE statement string
	 *
	 * @var	string
	 */

	/**
	 * ESCAPE character
	 *
	 * @var	string
	 */

	/**
	 * ORDER BY random keyword
	 *
	 * @var	array
	 */

	/**
	 * COUNT string
	 *
	 * @used-by	CI_DB_driver.count_all()
	 * @used-by	CI_DB_query_builder.count_all_results()
	 *
	 * @var	string
	 */

	// --------------------------------------------------------------------

	/**
	 * Class constructor
	 *
	 * @param	array	params
	 * @return	void
	 */
	def __init__(self,params):
		self.dsn = None
		self.username = None
		self.password = None
		self.hostname = None
		self.database = None
		self.dbdriver		= 'mysqli' = None
		self.subdriver = None
		self.dbprefix		= '' = None
		self.char_set		= 'utf8' = None
		self.dbcollat		= 'utf8_general_ci' = None
		self.encrypt			= FALSE = None
		self.swap_pre		= '' = None
		self.port			= NULL = None
		self.pconnect		= FALSE = None
		self.conn_id			= FALSE = None
		self.result_id		= FALSE = None
		self.db_debug		= FALSE = None
		self.benchmark		= 0 = None
		self.query_count		= 0 = None
		self.bind_marker		= '?' = None
		self.save_queries		= TRUE = None
		self.queries			= array() = None
		self.query_times		= array() = None
		self.data_cache		= array() = None
		self.trans_enabled		= TRUE = None
		self.trans_strict		= TRUE = None
		self._trans_depth		= 0 = None
		self._trans_status	= TRUE = None
		self._trans_failure	= FALSE = None
		self.cache_on		= FALSE = None
		self.cachedir		= '' = None
		self.cache_autodel		= FALSE = None
		self.CACHE = None
		self._protect_identifiers		= TRUE = None
		self._reserved_identifiers	= array('*') = None
		self._escape_char = '"' = None
		self._like_escape_str = " ESCAPE '%s' " = None
		self._like_escape_chr = '!' = None
		self._random_keyword = array('RAND()', 'RAND(%d)') = None
		self._count_string = 'SELECT COUNT(*) AS ' = None
	{
		if (is_array(params))
		{
			foreach (params as key => val)
			{
				self.key = val
			}
		}

		log_message('info', 'Database Driver Class Initialized')
	}

	// --------------------------------------------------------------------

	/**
	 * Initialize Database Settings
	 *
	 * @return	bool
	 */
	def initialize(self):
	{
		/* If an established connection is available, then there's
		 * no need to connect and select the database.
		 *
		 * Depending on the database driver, conn_id can be either
		 * boolean TRUE, a resource or an object.
		 */
		if (self.conn_id)
		{
			return TRUE
		}

		// ----------------------------------------------------------------

		// Connect to the database and set the connection ID
		self.conn_id = self.db_connect(self.pconnect)

		// No connection resource? Check if there is a failover else throw an error
		if ( ! self.conn_id)
		{
			// Check if there is a failover set
			if ( ! empty(self.failover) && is_array(self.failover))
			{
				// Go over all the failovers
				foreach (self.failover as failover)
				{
					// Replace the current settings with those of the failover
					foreach (failover as key => val)
					{
						self.key = val
					}

					// Try to connect
					self.conn_id = self.db_connect(self.pconnect)

					// If a connection is made break the foreach loop
					if (self.conn_id)
					{
						break
					}
				}
			}

			// We still don't have a connection?
			if ( ! self.conn_id)
			{
				log_message('error', 'Unable to connect to the database')

				if (self.db_debug)
				{
					self.display_error('db_unable_to_connect')
				}

				return FALSE
			}
		}

		// Now we set the character set and that's all
		return self.db_set_charset(self.char_set)
	}

	// --------------------------------------------------------------------

	/**
	 * DB connect
	 *
	 * This is just a dummy method that all drivers will override.
	 *
	 * @return	mixed
	 */
	def db_connect(self):
	{
		return TRUE
	}

	// --------------------------------------------------------------------

	/**
	 * Persistent database connection
	 *
	 * @return	mixed
	 */
	def db_pconnect(self):
	{
		return self.db_connect(TRUE)
	}

	// --------------------------------------------------------------------

	/**
	 * Reconnect
	 *
	 * Keep / reestablish the db connection if no queries have been
	 * sent for a length of time exceeding the server's idle timeout.
	 *
	 * This is just a dummy method to allow drivers without such
	 * functionality to not declare it, while others will override it.
	 *
	 * @return	void
	 */
	def reconnect(self):
	{
	}

	// --------------------------------------------------------------------

	/**
	 * Select database
	 *
	 * This is just a dummy method to allow drivers without such
	 * functionality to not declare it, while others will override it.
	 *
	 * @return	bool
	 */
	def db_select(self):
	{
		return TRUE
	}

	// --------------------------------------------------------------------

	/**
	 * Last error
	 *
	 * @return	array
	 */
	def error(self):
	{
		return array('code' => NULL, 'message' => NULL)
	}

	// --------------------------------------------------------------------

	/**
	 * Set client character set
	 *
	 * @param	string
	 * @return	bool
	 */
	def db_set_charset(self,charset):
	{
		if (method_exists(this, '_db_set_charset') && ! self._db_set_charset(charset))
		{
			log_message('error', 'Unable to set database connection charset: '.charset)

			if (self.db_debug)
			{
				self.display_error('db_unable_to_set_charset', charset)
			}

			return FALSE
		}

		return TRUE
	}

	// --------------------------------------------------------------------

	/**
	 * The name of the platform in use (mysql, mssql, etc...)
	 *
	 * @return	string
	 */
	def platform(self):
	{
		return self.dbdriver
	}

	// --------------------------------------------------------------------

	/**
	 * Database version number
	 *
	 * Returns a string containing the version of the database being used.
	 * Most drivers will override this method.
	 *
	 * @return	string
	 */
	def version(self):
	{
		if (isset(self.data_cache['version']))
		{
			return self.data_cache['version']
		}

		if (FALSE === (sql = self._version()))
		{
			return (self.db_debug) ? self.display_error('db_unsupported_function') : FALSE
		}

		query = self.query(sql)->row()
		return self.data_cache['version'] = query->ver
	}

	// --------------------------------------------------------------------

	/**
	 * Version number query string
	 *
	 * @return	string
	 */
	def _version(self):
	{
		return 'SELECT VERSION() AS ver'
	}

	// --------------------------------------------------------------------

	/**
	 * Execute the query
	 *
	 * Accepts an SQL string as input and returns a result object upon
	 * successful execution of a "read" type query. Returns boolean TRUE
	 * upon successful execution of a "write" type query. Returns boolean
	 * FALSE upon failure, and if the db_debug variable is set to TRUE
	 * will raise an error.
	 *
	 * @param	string	sql
	 * @param	array	binds = FALSE		An array of binding data
	 * @param	bool	return_object = NULL
	 * @return	mixed
	 */
	def query(self,sql, binds = FALSE, return_object = NULL):
	{
		if (sql === '')
		{
			log_message('error', 'Invalid query: '.sql)
			return (self.db_debug) ? self.display_error('db_invalid_query') : FALSE
		}
		elseif ( ! is_bool(return_object))
		{
			return_object = ! self.is_write_type(sql)
		}

		// Verify table prefix and replace if necessary
		if (self.dbprefix !== '' && self.swap_pre !== '' && self.dbprefix !== self.swap_pre)
		{
			sql = preg_replace('/(\W)'.self.swap_pre.'(\S+?)/', '\\1'.self.dbprefix.'\\2', sql)
		}

		// Compile binds if needed
		if (binds !== FALSE)
		{
			sql = self.compile_binds(sql, binds)
		}

		// Is query caching enabled? If the query is a "read type"
		// we will load the caching class and return the previously
		// cached query if it exists
		if (self.cache_on === TRUE && return_object === TRUE && self._cache_init())
		{
			self.load_rdriver()
			if (FALSE !== (cache = self.CACHE->read(sql)))
			{
				return cache
			}
		}

		// Save the query for debugging
		if (self.save_queries === TRUE)
		{
			self.queries[] = sql
		}

		// Start the Query Timer
		time_start = microtime(TRUE)

		// Run the Query
		if (FALSE === (self.result_id = self.simple_query(sql)))
		{
			if (self.save_queries === TRUE)
			{
				self.query_times[] = 0
			}

			// This will trigger a rollback if transactions are being used
			if (self._trans_depth !== 0)
			{
				self._trans_status = FALSE
			}

			// Grab the error now, as we might run some additional queries before displaying the error
			error = self.error()

			// Log errors
			log_message('error', 'Query error: '.error['message'].' - Invalid query: '.sql)

			if (self.db_debug)
			{
				// We call this function in order to roll-back queries
				// if transactions are enabled. If we don't call this here
				// the error message will trigger an exit, causing the
				// transactions to remain in limbo.
				while (self._trans_depth !== 0)
				{
					trans_depth = self._trans_depth
					self.trans_complete()
					if (trans_depth === self._trans_depth)
					{
						log_message('error', 'Database: Failure during an automated transaction commit/rollback!')
						break
					}
				}

				// Display errors
				return self.display_error(array('Error Number: '.error['code'], error['message'], sql))
			}

			return FALSE
		}

		// Stop and aggregate the query time results
		time_end = microtime(TRUE)
		self.benchmark += time_end - time_start

		if (self.save_queries === TRUE)
		{
			self.query_times[] = time_end - time_start
		}

		// Increment the query counter
		self.query_count++

		// Will we have a result object instantiated? If not - we'll simply return TRUE
		if (return_object !== TRUE)
		{
			// If caching is enabled we'll auto-cleanup any existing files related to this particular URI
			if (self.cache_on === TRUE && self.cache_autodel === TRUE && self._cache_init())
			{
				self.CACHE->delete()
			}

			return TRUE
		}

		// Load and instantiate the result driver
		driver		= self.load_rdriver()
		RES		= driver(this)

		// Is query caching enabled? If so, we'll serialize the
		// result object and save it to a cache file.
		if (self.cache_on === TRUE && self._cache_init())
		{
			// We'll create a instance of the result object
			// only without the platform specific driver since
			// we can't use it with cached data (the query result
			// resource ID won't be any good once we've cached the
			// result object, so we'll have to compile the data
			// and save it)
			CR = CI_DB_result(this)
			CR->result_object	= RES->result_object()
			CR->result_array	= RES->result_array()
			CR->num_rows		= RES->num_rows()

			// Reset these since cached objects can not utilize resource IDs.
			CR->conn_id		= NULL
			CR->result_id		= NULL

			self.CACHE->write(sql, CR)
		}

		return RES
	}

	// --------------------------------------------------------------------

	/**
	 * Load the result drivers
	 *
	 * @return	string	the name of the result class
	 */
	def load_rdriver(self):
	{
		driver = 'CI_DB_'.self.dbdriver.'_result'

		if ( ! class_exists(driver, FALSE))
		{
			require_once(BASEPATH.'database/DB_result.php')
			require_once(BASEPATH.'database/drivers/'.self.dbdriver.'/'.self.dbdriver.'_result.php')
		}

		return driver
	}

	// --------------------------------------------------------------------

	/**
	 * Simple Query
	 * This is a simplified version of the query() function. Internally
	 * we only use it when running transaction commands since they do
	 * not require all the features of the main query() function.
	 *
	 * @param	string	the sql query
	 * @return	mixed
	 */
	def simple_query(self,sql):
	{
		if ( ! self.conn_id)
		{
			if ( ! self.initialize())
			{
				return FALSE
			}
		}

		return self._execute(sql)
	}

	// --------------------------------------------------------------------

	/**
	 * Disable Transactions
	 * This permits transactions to be disabled at run-time.
	 *
	 * @return	void
	 */
	def trans_off(self):
	{
		self.trans_enabled = FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Enable/disable Transaction Strict Mode
	 *
	 * When strict mode is enabled, if you are running multiple groups of
	 * transactions, if one group fails all subsequent groups will be
	 * rolled back.
	 *
	 * If strict mode is disabled, each group is treated autonomously,
	 * meaning a failure of one group will not affect any others
	 *
	 * @param	bool	mode = TRUE
	 * @return	void
	 */
	def trans_strict(self,mode = TRUE):
	{
		self.trans_strict = is_bool(mode) ? mode : TRUE
	}

	// --------------------------------------------------------------------

	/**
	 * Start Transaction
	 *
	 * @param	bool	test_mode = FALSE
	 * @return	bool
	 */
	def trans_start(self,test_mode = FALSE):
	{
		if ( ! self.trans_enabled)
		{
			return FALSE
		}

		return self.trans_begin(test_mode)
	}

	// --------------------------------------------------------------------

	/**
	 * Complete Transaction
	 *
	 * @return	bool
	 */
	def trans_complete(self):
	{
		if ( ! self.trans_enabled)
		{
			return FALSE
		}

		// The query() function will set this flag to FALSE in the event that a query failed
		if (self._trans_status === FALSE OR self._trans_failure === TRUE)
		{
			self.trans_rollback()

			// If we are NOT running in strict mode, we will reset
			// the _trans_status flag so that subsequent groups of
			// transactions will be permitted.
			if (self.trans_strict === FALSE)
			{
				self._trans_status = TRUE
			}

			log_message('debug', 'DB Transaction Failure')
			return FALSE
		}

		return self.trans_commit()
	}

	// --------------------------------------------------------------------

	/**
	 * Lets you retrieve the transaction flag to determine if it has failed
	 *
	 * @return	bool
	 */
	def trans_status(self):
	{
		return self._trans_status
	}

	// --------------------------------------------------------------------

	/**
	 * Begin Transaction
	 *
	 * @param	bool	test_mode
	 * @return	bool
	 */
	def trans_begin(self,test_mode = FALSE):
	{
		if ( ! self.trans_enabled)
		{
			return FALSE
		}
		// When transactions are nested we only begin/commit/rollback the outermost ones
		elseif (self._trans_depth > 0)
		{
			self._trans_depth++
			return TRUE
		}

		// Reset the transaction failure flag.
		// If the test_mode flag is set to TRUE transactions will be rolled back
		// even if the queries produce a successful result.
		self._trans_failure = (test_mode === TRUE)

		if (self._trans_begin())
		{
			self._trans_status = TRUE
			self._trans_depth++
			return TRUE
		}

		return FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Commit Transaction
	 *
	 * @return	bool
	 */
	def trans_commit(self):
	{
		if ( ! self.trans_enabled OR self._trans_depth === 0)
		{
			return FALSE
		}
		// When transactions are nested we only begin/commit/rollback the outermost ones
		elseif (self._trans_depth > 1 OR self._trans_commit())
		{
			self._trans_depth--
			return TRUE
		}

		return FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Rollback Transaction
	 *
	 * @return	bool
	 */
	def trans_rollback(self):
	{
		if ( ! self.trans_enabled OR self._trans_depth === 0)
		{
			return FALSE
		}
		// When transactions are nested we only begin/commit/rollback the outermost ones
		elseif (self._trans_depth > 1 OR self._trans_rollback())
		{
			self._trans_depth--
			return TRUE
		}

		return FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Compile Bindings
	 *
	 * @param	string	the sql statement
	 * @param	array	an array of bind data
	 * @return	string
	 */
	def compile_binds(self,sql, binds):
	{
		if (empty(self.bind_marker) OR strpos(sql, self.bind_marker) === FALSE)
		{
			return sql
		}
		elseif ( ! is_array(binds))
		{
			binds = array(binds)
			bind_count = 1
		}
		else
		{
			// Make sure we're using numeric keys
			binds = array_values(binds)
			bind_count = count(binds)
		}

		// We'll need the marker length later
		ml = strlen(self.bind_marker)

		// Make sure not to replace a chunk inside a string that happens to match the bind marker
		if (c = preg_match_all("/'[^']*'|\"[^\"]*\"/i", sql, matches))
		{
			c = preg_match_all('/'.preg_quote(self.bind_marker, '/').'/i',
				str_replace(matches[0],
					str_replace(self.bind_marker, str_repeat(' ', ml), matches[0]),
					sql, c),
				matches, PREG_OFFSET_CAPTURE)

			// Bind values' count must match the count of markers in the query
			if (bind_count !== c)
			{
				return sql
			}
		}
		elseif ((c = preg_match_all('/'.preg_quote(self.bind_marker, '/').'/i', sql, matches, PREG_OFFSET_CAPTURE)) !== bind_count)
		{
			return sql
		}

		do
		{
			c--
			escaped_value = self.escape(binds[c])
			if (is_array(escaped_value))
			{
				escaped_value = '('.implode(',', escaped_value).')'
			}
			sql = substr_replace(sql, escaped_value, matches[0][c][1], ml)
		}
		while (c !== 0)

		return sql
	}

	// --------------------------------------------------------------------

	/**
	 * Determines if a query is a "write" type.
	 *
	 * @param	string	An SQL query string
	 * @return	bool
	 */
	def is_write_type(self,sql):
	{
		return (bool) preg_match('/^\s*"?(SET|INSERT|UPDATE|DELETE|REPLACE|CREATE|DROP|TRUNCATE|LOAD|COPY|ALTER|RENAME|GRANT|REVOKE|LOCK|UNLOCK|REINDEX|MERGE)\s/i', sql)
	}

	// --------------------------------------------------------------------

	/**
	 * Calculate the aggregate query elapsed time
	 *
	 * @param	int	The number of decimal places
	 * @return	string
	 */
	def elapsed_time(self,decimals = 6):
	{
		return number_format(self.benchmark, decimals)
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the total number of queries
	 *
	 * @return	int
	 */
	def total_queries(self):
	{
		return self.query_count
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the last query that was executed
	 *
	 * @return	string
	 */
	def last_query(self):
	{
		return end(self.queries)
	}

	// --------------------------------------------------------------------

	/**
	 * "Smart" Escape String
	 *
	 * Escapes data based on type
	 * Sets boolean and null types
	 *
	 * @param	string
	 * @return	mixed
	 */
	def escape(self,str):
	{
		if (is_array(str))
		{
			str = array_map(array(&this, 'escape'), str)
			return str
		}
		elseif (is_string(str) OR (is_object(str) && method_exists(str, '__toString')))
		{
			return "'".self.escape_str(str)."'"
		}
		elseif (is_bool(str))
		{
			return (str === FALSE) ? 0 : 1
		}
		elseif (str === NULL)
		{
			return 'NULL'
		}

		return str
	}

	// --------------------------------------------------------------------

	/**
	 * Escape String
	 *
	 * @param	string|string[]	str	Input string
	 * @param	bool	like	Whether or not the string will be used in a LIKE condition
	 * @return	string
	 */
	def escape_str(self,str, like = FALSE):
	{
		if (is_array(str))
		{
			foreach (str as key => val)
			{
				str[key] = self.escape_str(val, like)
			}

			return str
		}

		str = self._escape_str(str)

		// escape LIKE condition wildcards
		if (like === TRUE)
		{
			return str_replace(
				array(self._like_escape_chr, '%', '_'),
				array(self._like_escape_chr.self._like_escape_chr, self._like_escape_chr.'%', self._like_escape_chr.'_'),
				str
			)
		}

		return str
	}

	// --------------------------------------------------------------------

	/**
	 * Escape LIKE String
	 *
	 * Calls the individual driver for platform
	 * specific escaping for LIKE conditions
	 *
	 * @param	string|string[]
	 * @return	mixed
	 */
	def escape_like_str(self,str):
	{
		return self.escape_str(str, TRUE)
	}

	// --------------------------------------------------------------------

	/**
	 * Platform-dependent string escape
	 *
	 * @param	string
	 * @return	string
	 */
	def _escape_str(self,str):
	{
		return str_replace("'", "''", remove_invisible_characters(str, FALSE))
	}

	// --------------------------------------------------------------------

	/**
	 * Primary
	 *
	 * Retrieves the primary key. It assumes that the row in the first
	 * position is the primary key
	 *
	 * @param	string	table	Table name
	 * @return	string
	 */
	def primary(self,table):
	{
		fields = self.list_fields(table)
		return is_array(fields) ? current(fields) : FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * "Count All" query
	 *
	 * Generates a platform-specific query string that counts all records in
	 * the specified database
	 *
	 * @param	string
	 * @return	int
	 */
	def count_all(self,table = ''):
	{
		if (table === '')
		{
			return 0
		}

		query = self.query(self._count_string.self.escape_identifiers('numrows').' FROM '.self.protect_identifiers(table, TRUE, NULL, FALSE))
		if (query->num_rows() === 0)
		{
			return 0
		}

		query = query->row()
		self._reset_select()
		return (int) query->numrows
	}

	// --------------------------------------------------------------------

	/**
	 * Returns an array of table names
	 *
	 * @param	string	constrain_by_prefix = FALSE
	 * @return	array
	 */
	def list_tables(self,constrain_by_prefix = FALSE):
	{
		// Is there a cached result?
		if (isset(self.data_cache['table_names']))
		{
			return self.data_cache['table_names']
		}

		if (FALSE === (sql = self._list_tables(constrain_by_prefix)))
		{
			return (self.db_debug) ? self.display_error('db_unsupported_function') : FALSE
		}

		self.data_cache['table_names'] = array()
		query = self.query(sql)

		foreach (query->result_array() as row)
		{
			// Do we know from which column to get the table name?
			if ( ! isset(key))
			{
				if (isset(row['table_name']))
				{
					key = 'table_name'
				}
				elseif (isset(row['TABLE_NAME']))
				{
					key = 'TABLE_NAME'
				}
				else
				{
					/* We have no other choice but to just get the first element's key.
					 * Due to array_shift() accepting its argument by reference, if
					 * E_STRICT is on, this would trigger a warning. So we'll have to
					 * assign it first.
					 */
					key = array_keys(row)
					key = array_shift(key)
				}
			}

			self.data_cache['table_names'][] = row[key]
		}

		return self.data_cache['table_names']
	}

	// --------------------------------------------------------------------

	/**
	 * Determine if a particular table exists
	 *
	 * @param	string	table_name
	 * @return	bool
	 */
	def table_exists(self,table_name):
	{
		return in_array(self.protect_identifiers(table_name, TRUE, FALSE, FALSE), self.list_tables())
	}

	// --------------------------------------------------------------------

	/**
	 * Fetch Field Names
	 *
	 * @param	string	table	Table name
	 * @return	array
	 */
	def list_fields(self,table):
	{
		if (FALSE === (sql = self._list_columns(table)))
		{
			return (self.db_debug) ? self.display_error('db_unsupported_function') : FALSE
		}

		query = self.query(sql)
		fields = array()

		foreach (query->result_array() as row)
		{
			// Do we know from where to get the column's name?
			if ( ! isset(key))
			{
				if (isset(row['column_name']))
				{
					key = 'column_name'
				}
				elseif (isset(row['COLUMN_NAME']))
				{
					key = 'COLUMN_NAME'
				}
				else
				{
					// We have no other choice but to just get the first element's key.
					key = key(row)
				}
			}

			fields[] = row[key]
		}

		return fields
	}

	// --------------------------------------------------------------------

	/**
	 * Determine if a particular field exists
	 *
	 * @param	string
	 * @param	string
	 * @return	bool
	 */
	def field_exists(self,field_name, table_name):
	{
		return in_array(field_name, self.list_fields(table_name))
	}

	// --------------------------------------------------------------------

	/**
	 * Returns an object with field data
	 *
	 * @param	string	table	the table name
	 * @return	array
	 */
	def field_data(self,table):
	{
		query = self.query(self._field_data(self.protect_identifiers(table, TRUE, NULL, FALSE)))
		return (query) ? query->field_data() : FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Escape the SQL Identifiers
	 *
	 * This function escapes column and table names
	 *
	 * @param	mixed
	 * @return	mixed
	 */
	def escape_identifiers(self,item):
	{
		if (self._escape_char === '' OR empty(item) OR in_array(item, self._reserved_identifiers))
		{
			return item
		}
		elseif (is_array(item))
		{
			foreach (item as key => value)
			{
				item[key] = self.escape_identifiers(value)
			}

			return item
		}
		// Avoid breaking functions and literal values inside queries
		elseif (ctype_digit(item) OR item[0] === "'" OR (self._escape_char !== '"' && item[0] === '"') OR strpos(item, '(') !== FALSE)
		{
			return item
		}

		static preg_ec = array()

		if (empty(preg_ec))
		{
			if (is_array(self._escape_char))
			{
				preg_ec = array(
					preg_quote(self._escape_char[0], '/'),
					preg_quote(self._escape_char[1], '/'),
					self._escape_char[0],
					self._escape_char[1]
				)
			}
			else
			{
				preg_ec[0] = preg_ec[1] = preg_quote(self._escape_char, '/')
				preg_ec[2] = preg_ec[3] = self._escape_char
			}
		}

		foreach (self._reserved_identifiers as id)
		{
			if (strpos(item, '.'.id) !== FALSE)
			{
				return preg_replace('/'.preg_ec[0].'?([^'.preg_ec[1].'\.]+)'.preg_ec[1].'?\./i', preg_ec[2].'1'.preg_ec[3].'.', item)
			}
		}

		return preg_replace('/'.preg_ec[0].'?([^'.preg_ec[1].'\.]+)'.preg_ec[1].'?(\.)?/i', preg_ec[2].'1'.preg_ec[3].'2', item)
	}

	// --------------------------------------------------------------------

	/**
	 * Generate an insert string
	 *
	 * @param	string	the table upon which the query will be performed
	 * @param	array	an associative array data of key/values
	 * @return	string
	 */
	def insert_string(self,table, data):
	{
		fields = values = array()

		foreach (data as key => val)
		{
			fields[] = self.escape_identifiers(key)
			values[] = self.escape(val)
		}

		return self._insert(self.protect_identifiers(table, TRUE, NULL, FALSE), fields, values)
	}

	// --------------------------------------------------------------------

	/**
	 * Insert statement
	 *
	 * Generates a platform-specific insert string from the supplied data
	 *
	 * @param	string	the table name
	 * @param	array	the insert keys
	 * @param	array	the insert values
	 * @return	string
	 */
	def _insert(self,table, keys, values):
	{
		return 'INSERT INTO '.table.' ('.implode(', ', keys).') VALUES ('.implode(', ', values).')'
	}

	// --------------------------------------------------------------------

	/**
	 * Generate an update string
	 *
	 * @param	string	the table upon which the query will be performed
	 * @param	array	an associative array data of key/values
	 * @param	mixed	the "where" statement
	 * @return	string
	 */
	def update_string(self,table, data, where):
	{
		if (empty(where))
		{
			return FALSE
		}

		self.where(where)

		fields = array()
		foreach (data as key => val)
		{
			fields[self.protect_identifiers(key)] = self.escape(val)
		}

		sql = self._update(self.protect_identifiers(table, TRUE, NULL, FALSE), fields)
		self._reset_write()
		return sql
	}

	// --------------------------------------------------------------------

	/**
	 * Update statement
	 *
	 * Generates a platform-specific update string from the supplied data
	 *
	 * @param	string	the table name
	 * @param	array	the update data
	 * @return	string
	 */
	def _update(self,table, values):
	{
		foreach (values as key => val)
		{
			valstr[] = key.' = '.val
		}

		return 'UPDATE '.table.' SET '.implode(', ', valstr)
			.self._compile_wh('qb_where')
			.self._compile_order_by()
			.(self.qb_limit !== FALSE ? ' LIMIT '.self.qb_limit : '')
	}

	// --------------------------------------------------------------------

	/**
	 * Tests whether the string has an SQL operator
	 *
	 * @param	string
	 * @return	bool
	 */
	def _has_operator(self,str):
	{
		return (bool) preg_match('/(<|>|!|=|\sIS NULL|\sIS NOT NULL|\sEXISTS|\sBETWEEN|\sLIKE|\sIN\s*\(|\s)/i', trim(str))
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the SQL string operator
	 *
	 * @param	string
	 * @return	string
	 */
	def _get_operator(self,str):
	{
		static _operators

		if (empty(_operators))
		{
			_les = (self._like_escape_str !== '')
				? '\s+'.preg_quote(trim(sprintf(self._like_escape_str, self._like_escape_chr)), '/')
				: ''
			_operators = array(
				'\s*(?:<|>|!)?=\s*',             // =, <=, >=, !=
				'\s*<>?\s*',                     // <, <>
				'\s*>\s*',                       // >
				'\s+IS NULL',                    // IS NULL
				'\s+IS NOT NULL',                // IS NOT NULL
				'\s+EXISTS\s*\(.*\)',        // EXISTS(sql)
				'\s+NOT EXISTS\s*\(.*\)',    // NOT EXISTS(sql)
				'\s+BETWEEN\s+',                 // BETWEEN value AND value
				'\s+IN\s*\(.*\)',            // IN(list)
				'\s+NOT IN\s*\(.*\)',        // NOT IN (list)
				'\s+LIKE\s+\S.*('._les.')?',    // LIKE 'expr'[ ESCAPE '%s']
				'\s+NOT LIKE\s+\S.*('._les.')?' // NOT LIKE 'expr'[ ESCAPE '%s']
			)

		}

		return preg_match('/'.implode('|', _operators).'/i', str, match)
			? match[0] : FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Enables a native PHP function to be run, using a platform agnostic wrapper.
	 *
	 * @param	string	function	Function name
	 * @return	mixed
	 */
	def call_function(self,function):
	{
		driver = (self.dbdriver === 'postgre') ? 'pg_' : self.dbdriver.'_'

		if (FALSE === strpos(driver, function))
		{
			function = driver.function
		}

		if ( ! function_exists(function))
		{
			return (self.db_debug) ? self.display_error('db_unsupported_function') : FALSE
		}

		return (func_num_args() > 1)
			? call_user_func_array(function, array_slice(func_get_args(), 1))
			: call_user_func(function)
	}

	// --------------------------------------------------------------------

	/**
	 * Set Cache Directory Path
	 *
	 * @param	string	the path to the cache directory
	 * @return	void
	 */
	def cache_set_path(self,path = ''):
	{
		self.cachedir = path
	}

	// --------------------------------------------------------------------

	/**
	 * Enable Query Caching
	 *
	 * @return	bool	cache_on value
	 */
	def cache_on(self):
	{
		return self.cache_on = TRUE
	}

	// --------------------------------------------------------------------

	/**
	 * Disable Query Caching
	 *
	 * @return	bool	cache_on value
	 */
	def cache_off(self):
	{
		return self.cache_on = FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Delete the cache files associated with a particular URI
	 *
	 * @param	string	segment_one = ''
	 * @param	string	segment_two = ''
	 * @return	bool
	 */
	def cache_delete(self,segment_one = '', segment_two = ''):
	{
		return self._cache_init()
			? self.CACHE->delete(segment_one, segment_two)
			: FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Delete All cache files
	 *
	 * @return	bool
	 */
	def cache_delete_all(self):
	{
		return self._cache_init()
			? self.CACHE->delete_all()
			: FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Initialize the Cache Class
	 *
	 * @return	bool
	 */
	def _cache_init(self):
	{
		if ( ! class_exists('CI_DB_Cache', FALSE))
		{
			require_once(BASEPATH.'database/DB_cache.php')
		}
		elseif (is_object(self.CACHE))
		{
			return TRUE
		}

		self.CACHE = CI_DB_Cache(this) // pass db object to support multiple db connections and returned db objects
		return TRUE
	}

	// --------------------------------------------------------------------

	/**
	 * Close DB Connection
	 *
	 * @return	void
	 */
	def close(self):
	{
		if (self.conn_id)
		{
			self._close()
			self.conn_id = FALSE
		}
	}

	// --------------------------------------------------------------------

	/**
	 * Close DB Connection
	 *
	 * This method would be overridden by most of the drivers.
	 *
	 * @return	void
	 */
	def _close(self):
	{
		self.conn_id = FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Display an error message
	 *
	 * @param	string	the error message
	 * @param	string	any "swap" values
	 * @param	bool	whether to localize the message
	 * @return	string	sends the application/views/errors/error_db.php template
	 */
	def display_error(self,error = '', swap = '', native = FALSE):
	{
		LANG =& load_class('Lang', 'core')
		LANG->load('db')

		heading = LANG->line('db_error_heading')

		if (native === TRUE)
		{
			message = (array) error
		}
		else
		{
			message = is_array(error) ? error : array(str_replace('%s', swap, LANG->line(error)))
		}

		// Find the most likely culprit of the error by going through
		// the backtrace until the source file is no longer in the
		// database folder.
		trace = debug_backtrace()
		foreach (trace as call)
		{
			if (isset(call['file'], call['class']))
			{
				// We'll need this on Windows, as APPPATH and BASEPATH will always use forward slashes
				if (DIRECTORY_SEPARATOR !== '/')
				{
					call['file'] = str_replace('\\', '/', call['file'])
				}

				if (strpos(call['file'], BASEPATH.'database') === FALSE && strpos(call['class'], 'Loader') === FALSE)
				{
					// Found it - use a relative path for safety
					message[] = 'Filename: '.str_replace(array(APPPATH, BASEPATH), '', call['file'])
					message[] = 'Line Number: '.call['line']
					break
				}
			}
		}

		error =& load_class('Exceptions', 'core')
		echo error->show_error(heading, message, 'error_db')
		exit(8) // EXIT_DATABASE
	}

	// --------------------------------------------------------------------

	/**
	 * Protect Identifiers
	 *
	 * This function is used extensively by the Query Builder class, and by
	 * a couple functions in this class.
	 * It takes a column or table name (optionally with an alias) and inserts
	 * the table prefix onto it. Some logic is necessary in order to deal with
	 * column names that include the path. Consider a query like this:
	 *
	 * SELECT hostname.database.table.column AS c FROM hostname.database.table
	 *
	 * Or a query with aliasing:
	 *
	 * SELECT m.member_id, m.member_name FROM members AS m
	 *
	 * Since the column name can include up to four segments (host, DB, table, column)
	 * or also have an alias prefix, we need to do a bit of work to figure this out and
	 * insert the table prefix (if it exists) in the proper position, and escape only
	 * the correct identifiers.
	 *
	 * @param	string
	 * @param	bool
	 * @param	mixed
	 * @param	bool
	 * @return	string
	 */
	def protect_identifiers(self,item, prefix_single = FALSE, protect_identifiers = NULL, field_exists = TRUE):
	{
		if ( ! is_bool(protect_identifiers))
		{
			protect_identifiers = self._protect_identifiers
		}

		if (is_array(item))
		{
			escaped_array = array()
			foreach (item as k => v)
			{
				escaped_array[self.protect_identifiers(k)] = self.protect_identifiers(v, prefix_single, protect_identifiers, field_exists)
			}

			return escaped_array
		}

		// This is basically a bug fix for queries that use MAX, MIN, etc.
		// If a parenthesis is found we know that we do not need to
		// escape the data or add a prefix. There's probably a more graceful
		// way to deal with this, but I'm not thinking of it -- Rick
		//
		// Added exception for single quotes as well, we don't want to alter
		// literal strings. -- Narf
		if (strcspn(item, "()'") !== strlen(item))
		{
			return item
		}

		// Convert tabs or multiple spaces into single spaces
		item = preg_replace('/\s+/', ' ', trim(item))

		// If the item has an alias declaration we remove it and set it aside.
		// Note: strripos() is used in order to support spaces in table names
		if (offset = strripos(item, ' AS '))
		{
			alias = (protect_identifiers)
				? substr(item, offset, 4).self.escape_identifiers(substr(item, offset + 4))
				: substr(item, offset)
			item = substr(item, 0, offset)
		}
		elseif (offset = strrpos(item, ' '))
		{
			alias = (protect_identifiers)
				? ' '.self.escape_identifiers(substr(item, offset + 1))
				: substr(item, offset)
			item = substr(item, 0, offset)
		}
		else
		{
			alias = ''
		}

		// Break the string apart if it contains periods, then insert the table prefix
		// in the correct location, assuming the period doesn't indicate that we're dealing
		// with an alias. While we're at it, we will escape the components
		if (strpos(item, '.') !== FALSE)
		{
			parts = explode('.', item)

			// Does the first segment of the exploded item match
			// one of the aliases previously identified? If so,
			// we have nothing more to do other than escape the item
			//
			// NOTE: The ! empty() condition prevents this method
			//       from breaking when QB isn't enabled.
			if ( ! empty(self.qb_aliased_tables) && in_array(parts[0], self.qb_aliased_tables))
			{
				if (protect_identifiers === TRUE)
				{
					foreach (parts as key => val)
					{
						if ( ! in_array(val, self._reserved_identifiers))
						{
							parts[key] = self.escape_identifiers(val)
						}
					}

					item = implode('.', parts)
				}

				return item.alias
			}

			// Is there a table prefix defined in the config file? If not, no need to do anything
			if (self.dbprefix !== '')
			{
				// We now add the table prefix based on some logic.
				// Do we have 4 segments (hostname.database.table.column)?
				// If so, we add the table prefix to the column name in the 3rd segment.
				if (isset(parts[3]))
				{
					i = 2
				}
				// Do we have 3 segments (database.table.column)?
				// If so, we add the table prefix to the column name in 2nd position
				elseif (isset(parts[2]))
				{
					i = 1
				}
				// Do we have 2 segments (table.column)?
				// If so, we add the table prefix to the column name in 1st segment
				else
				{
					i = 0
				}

				// This flag is set when the supplied item does not contain a field name.
				// This can happen when this function is being called from a JOIN.
				if (field_exists === FALSE)
				{
					i++
				}

				// dbprefix may've already been applied, with or without the identifier escaped
				ec = '(?<ec>'.preg_quote(is_array(self._escape_char) ? self._escape_char[0] : self._escape_char).')?'
				isset(ec[0]) && ec .= '?' // Just in case someone has disabled escaping by forcing an empty escape character

				// Verify table prefix and replace if necessary
				if (self.swap_pre !== '' && preg_match('#^'.ec.preg_quote(self.swap_pre).'#', parts[i]))
				{
					parts[i] = preg_replace('#^'.ec.preg_quote(self.swap_pre).'(\S+?)#', '\\1'.self.dbprefix.'\\2', parts[i])
				}
				// We only add the table prefix if it does not already exist
				else
				{
					preg_match('#^'.ec.preg_quote(self.dbprefix).'#', parts[i]) OR parts[i] = self.dbprefix.parts[i]
				}

				// Put the parts back together
				item = implode('.', parts)
			}

			if (protect_identifiers === TRUE)
			{
				item = self.escape_identifiers(item)
			}

			return item.alias
		}

		// Is there a table prefix? If not, no need to insert it
		if (self.dbprefix !== '')
		{
			// Verify table prefix and replace if necessary
			if (self.swap_pre !== '' && strpos(item, self.swap_pre) === 0)
			{
				item = preg_replace('/^'.self.swap_pre.'(\S+?)/', self.dbprefix.'\\1', item)
			}
			// Do we prefix an item with no segments?
			elseif (prefix_single === TRUE && strpos(item, self.dbprefix) !== 0)
			{
				item = self.dbprefix.item
			}
		}

		if (protect_identifiers === TRUE && ! in_array(item, self._reserved_identifiers))
		{
			item = self.escape_identifiers(item)
		}

		return item.alias
	}

	// --------------------------------------------------------------------

	/**
	 * Dummy method that allows Query Builder class to be disabled
	 * and keep count_all() working.
	 *
	 * @return	void
	 */
	def _reset_select(self):
	{
	}

