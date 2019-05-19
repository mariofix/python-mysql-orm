
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
 * Database Result Class
 *
 * This is the platform-independent result class.
 * This class will not be called directly. Rather, the adapter
 * class for the specific database will extend and instantiate it.
 *
 * @category	Database
 * @author		EllisLab Dev Team
 * @link		https://codeigniter.com/user_guide/database/
 */
class CI_DB_result(object):

	/**
	 * Connection ID
	 *
	 * @var	resource|object
	 */

	/**
	 * Result ID
	 *
	 * @var	resource|object
	 */

	/**
	 * Result Array
	 *
	 * @var	array[]
	 */

	/**
	 * Result Object
	 *
	 * @var	object[]
	 */

	/**
	 * Custom Result Object
	 *
	 * @var	object[]
	 */

	/**
	 * Current Row index
	 *
	 * @var	int
	 */

	/**
	 * Number of rows
	 *
	 * @var	int
	 */

	/**
	 * Row data
	 *
	 * @var	array
	 */

	// --------------------------------------------------------------------

	/**
	 * Constructor
	 *
	 * @param	object	driver_object
	 * @return	void
	 */
	def __init__(self,&driver_object):
		self.conn_id = None
		self.result_id = None
		self.result_array			= array() = None
		self.result_object			= array() = None
		self.custom_result_object		= array() = None
		self.current_row			= 0 = None
		self.num_rows = None
		self.row_data = None
	{
		self.conn_id = driver_object->conn_id
		self.result_id = driver_object->result_id
	}

	// --------------------------------------------------------------------

	/**
	 * Number of rows in the result set
	 *
	 * @return	int
	 */
	def num_rows(self):
	{
		if (is_int(self.num_rows))
		{
			return self.num_rows
		}
		elseif (count(self.result_array) > 0)
		{
			return self.num_rows = count(self.result_array)
		}
		elseif (count(self.result_object) > 0)
		{
			return self.num_rows = count(self.result_object)
		}

		return self.num_rows = count(self.result_array())
	}

	// --------------------------------------------------------------------

	/**
	 * Query result. Acts as a wrapper function for the following functions.
	 *
	 * @param	string	type	'object', 'array' or a custom class name
	 * @return	array
	 */
	def result(self,type = 'object'):
	{
		if (type === 'array')
		{
			return self.result_array()
		}
		elseif (type === 'object')
		{
			return self.result_object()
		}

		return self.custom_result_object(type)
	}

	// --------------------------------------------------------------------

	/**
	 * Custom query result.
	 *
	 * @param	string	class_name
	 * @return	array
	 */
	def custom_result_object(self,class_name):
	{
		if (isset(self.custom_result_object[class_name]))
		{
			return self.custom_result_object[class_name]
		}
		elseif ( ! self.result_id OR self.num_rows === 0)
		{
			return array()
		}

		// Don't fetch the result set again if we already have it
		_data = NULL
		if ((c = count(self.result_array)) > 0)
		{
			_data = 'result_array'
		}
		elseif ((c = count(self.result_object)) > 0)
		{
			_data = 'result_object'
		}

		if (_data !== NULL)
		{
			for (i = 0 i < c i++)
			{
				self.custom_result_object[class_name][i] = class_name()

				foreach (self.{_data}[i] as key => value)
				{
					self.custom_result_object[class_name][i]->key = value
				}
			}

			return self.custom_result_object[class_name]
		}

		is_null(self.row_data) OR self.data_seek(0)
		self.custom_result_object[class_name] = array()

		while (row = self._fetch_object(class_name))
		{
			self.custom_result_object[class_name][] = row
		}

		return self.custom_result_object[class_name]
	}

	// --------------------------------------------------------------------

	/**
	 * Query result. "object" version.
	 *
	 * @return	array
	 */
	def result_object(self):
	{
		if (count(self.result_object) > 0)
		{
			return self.result_object
		}

		// In the event that query caching is on, the result_id variable
		// will not be a valid resource so we'll simply return an empty
		// array.
		if ( ! self.result_id OR self.num_rows === 0)
		{
			return array()
		}

		if ((c = count(self.result_array)) > 0)
		{
			for (i = 0 i < c i++)
			{
				self.result_object[i] = (object) self.result_array[i]
			}

			return self.result_object
		}

		is_null(self.row_data) OR self.data_seek(0)
		while (row = self._fetch_object())
		{
			self.result_object[] = row
		}

		return self.result_object
	}

	// --------------------------------------------------------------------

	/**
	 * Query result. "array" version.
	 *
	 * @return	array
	 */
	def result_array(self):
	{
		if (count(self.result_array) > 0)
		{
			return self.result_array
		}

		// In the event that query caching is on, the result_id variable
		// will not be a valid resource so we'll simply return an empty
		// array.
		if ( ! self.result_id OR self.num_rows === 0)
		{
			return array()
		}

		if ((c = count(self.result_object)) > 0)
		{
			for (i = 0 i < c i++)
			{
				self.result_array[i] = (array) self.result_object[i]
			}

			return self.result_array
		}

		is_null(self.row_data) OR self.data_seek(0)
		while (row = self._fetch_assoc())
		{
			self.result_array[] = row
		}

		return self.result_array
	}

	// --------------------------------------------------------------------

	/**
	 * Row
	 *
	 * A wrapper method.
	 *
	 * @param	mixed	n
	 * @param	string	type	'object' or 'array'
	 * @return	mixed
	 */
	def row(self,n = 0, type = 'object'):
	{
		if ( ! is_numeric(n))
		{
			// We cache the row data for subsequent uses
			is_array(self.row_data) OR self.row_data = self.row_array(0)

			// array_key_exists() instead of isset() to allow for NULL values
			if (empty(self.row_data) OR ! array_key_exists(n, self.row_data))
			{
				return NULL
			}

			return self.row_data[n]
		}

		if (type === 'object') return self.row_object(n)
		elseif (type === 'array') return self.row_array(n)

		return self.custom_row_object(n, type)
	}

	// --------------------------------------------------------------------

	/**
	 * Assigns an item into a particular column slot
	 *
	 * @param	mixed	key
	 * @param	mixed	value
	 * @return	void
	 */
	def set_row(self,key, value = NULL):
	{
		// We cache the row data for subsequent uses
		if ( ! is_array(self.row_data))
		{
			self.row_data = self.row_array(0)
		}

		if (is_array(key))
		{
			foreach (key as k => v)
			{
				self.row_data[k] = v
			}
			return
		}

		if (key !== '' && value !== NULL)
		{
			self.row_data[key] = value
		}
	}

	// --------------------------------------------------------------------

	/**
	 * Returns a single result row - custom object version
	 *
	 * @param	int	n
	 * @param	string	type
	 * @return	object
	 */
	def custom_row_object(self,n, type):
	{
		isset(self.custom_result_object[type]) OR self.custom_result_object(type)

		if (count(self.custom_result_object[type]) === 0)
		{
			return NULL
		}

		if (n !== self.current_row && isset(self.custom_result_object[type][n]))
		{
			self.current_row = n
		}

		return self.custom_result_object[type][self.current_row]
	}

	// --------------------------------------------------------------------

	/**
	 * Returns a single result row - object version
	 *
	 * @param	int	n
	 * @return	object
	 */
	def row_object(self,n = 0):
	{
		result = self.result_object()
		if (count(result) === 0)
		{
			return NULL
		}

		if (n !== self.current_row && isset(result[n]))
		{
			self.current_row = n
		}

		return result[self.current_row]
	}

	// --------------------------------------------------------------------

	/**
	 * Returns a single result row - array version
	 *
	 * @param	int	n
	 * @return	array
	 */
	def row_array(self,n = 0):
	{
		result = self.result_array()
		if (count(result) === 0)
		{
			return NULL
		}

		if (n !== self.current_row && isset(result[n]))
		{
			self.current_row = n
		}

		return result[self.current_row]
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the "first" row
	 *
	 * @param	string	type
	 * @return	mixed
	 */
	def first_row(self,type = 'object'):
	{
		result = self.result(type)
		return (count(result) === 0) ? NULL : result[0]
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the "last" row
	 *
	 * @param	string	type
	 * @return	mixed
	 */
	def last_row(self,type = 'object'):
	{
		result = self.result(type)
		return (count(result) === 0) ? NULL : result[count(result) - 1]
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the "next" row
	 *
	 * @param	string	type
	 * @return	mixed
	 */
	def next_row(self,type = 'object'):
	{
		result = self.result(type)
		if (count(result) === 0)
		{
			return NULL
		}

		return isset(result[self.current_row + 1])
			? result[++self.current_row]
			: NULL
	}

	// --------------------------------------------------------------------

	/**
	 * Returns the "previous" row
	 *
	 * @param	string	type
	 * @return	mixed
	 */
	def previous_row(self,type = 'object'):
	{
		result = self.result(type)
		if (count(result) === 0)
		{
			return NULL
		}

		if (isset(result[self.current_row - 1]))
		{
			--self.current_row
		}
		return result[self.current_row]
	}

	// --------------------------------------------------------------------

	/**
	 * Returns an unbuffered row and move pointer to next row
	 *
	 * @param	string	type	'array', 'object' or a custom class name
	 * @return	mixed
	 */
	def unbuffered_row(self,type = 'object'):
	{
		if (type === 'array')
		{
			return self._fetch_assoc()
		}
		elseif (type === 'object')
		{
			return self._fetch_object()
		}

		return self._fetch_object(type)
	}

	// --------------------------------------------------------------------

	/**
	 * The following methods are normally overloaded by the identically named
	 * methods in the platform-specific driver -- except when query caching
	 * is used. When caching is enabled we do not load the other driver.
	 * These functions are primarily here to prevent undefined function errors
	 * when a cached result object is in use. They are not otherwise fully
	 * operational due to the unavailability of the database resource IDs with
	 * cached results.
	 */

	// --------------------------------------------------------------------

	/**
	 * Number of fields in the result set
	 *
	 * Overridden by driver result classes.
	 *
	 * @return	int
	 */
	def num_fields(self):
	{
		return 0
	}

	// --------------------------------------------------------------------

	/**
	 * Fetch Field Names
	 *
	 * Generates an array of column names.
	 *
	 * Overridden by driver result classes.
	 *
	 * @return	array
	 */
	def list_fields(self):
	{
		return array()
	}

	// --------------------------------------------------------------------

	/**
	 * Field data
	 *
	 * Generates an array of objects containing field meta-data.
	 *
	 * Overridden by driver result classes.
	 *
	 * @return	array
	 */
	def field_data(self):
	{
		return array()
	}

	// --------------------------------------------------------------------

	/**
	 * Free the result
	 *
	 * Overridden by driver result classes.
	 *
	 * @return	void
	 */
	def free_result(self):
	{
		self.result_id = FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Data Seek
	 *
	 * Moves the internal pointer to the desired offset. We call
	 * this internally before fetching results to make sure the
	 * result set starts at zero.
	 *
	 * Overridden by driver result classes.
	 *
	 * @param	int	n
	 * @return	bool
	 */
	def data_seek(self,n = 0):
	{
		return FALSE
	}

	// --------------------------------------------------------------------

	/**
	 * Result - associative array
	 *
	 * Returns the result set as an array.
	 *
	 * Overridden by driver result classes.
	 *
	 * @return	array
	 */
	def _fetch_assoc(self):
	{
		return array()
	}

	// --------------------------------------------------------------------

	/**
	 * Result - object
	 *
	 * Returns the result set as an object.
	 *
	 * Overridden by driver result classes.
	 *
	 * @param	string	class_name
	 * @return	object
	 */
	def _fetch_object(self,class_name = 'stdClass'):
	{
		return class_name()
	}

