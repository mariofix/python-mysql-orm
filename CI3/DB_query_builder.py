class query_builder 

    def __init__(self, params):
        super(query_builder, self).__init__()

        """ From DB_driver.php """
        self.dsn = ""
        self.username = None
        self.password = None
        self.hostname = None
        self.database = None
        self.dbdriver = "mysql"
        self.subdriver = None
        self.dbprefix = ""
        self.char_set = "utf8"
        self.dbcollat = 'utf8_general_ci'
        self.encrypt = None
        self.swap_pre = ''
        self.port = None
        self.pconnect = False
        self.conn_id = False
        self.result_id = False
        self.db_debug = False
        self.benchmark = 0
        self.query_count = 0
        self.bind_marker = '?'
        self.save_queries = True
        self.queries = []
        self.data_cache = []
        self.trans_enabled = True
        self.trans_strict = True
        self._trans_depth = 0
        self._trans_status = True
        self._trans_failure = False
        self.cache_on = False
        self.cachedir = ''
        self.cache_autodel = False
        self.CACHE = None
        self._protect_identifiers = True
        self._reserved_identifiers = ['*']
        self._escape_char = '"'
        self._like_escape_str = " ESCAPE '%s' "
        self._like_escape_chr = '!'
        self._random_keyword = ['RAND()', 'RAND(%d)']
        self._count_string = 'SELECT COUNT(*) AS '

        """ From DB_query_builder.php """
        self.return_delete_sql = False
        self.reset_delete_data = False
        self.qb_select = []
        self.qb_distinct = False
        self.qb_from = []
        self.qb_join = []
        self.qb_where = []
        self.qb_groupby = []
        self.qb_having = []
        self.qb_keys = []
        self.qb_limit = False
        self.qb_offset = False
        self.qb_orderby = []
        self.qb_set = []
        self.qb_set_ub = []
        self.qb_aliased_tables = []
        self.qb_where_group_started = False
        self.qb_where_group_count = 0
        self.qb_caching = False
        self.qb_cache_exists = []
        self.qb_cache_select = []
        self.qb_cache_from = []
        self.qb_cache_join = []
        self.qb_cache_aliased_tables = []
        self.qb_cache_where = []
        self.qb_cache_groupby = []
        self.qb_cache_having = []
        self.qb_cache_orderby = []
        self.qb_cache_set = []
        self.qb_no_escape = []
        self.qb_cache_no_escape = []

        for key,val in self.params.items():
            self.key = val
        
    
    def select(self, select = '*', escape = NULL):
        if isinstance(select, str):
            select = select.split(',')

        if isinstance(escape, bool):
            escape = self._protect_identifiers

        for val in select:
            val = val.trim()

            if val != '':
                self.qb_select.append(val)
                self.qb_no_escape.append(escape)

                if self.qb_caching == True:
                    self.qb_cache_select.append(val)
                    self.qb_cache_exists.append('select')
                    self.qb_cache_no_escape.append(escape)
        return self
    

    def select_max(self, select = '', alias = ''):
        return self._max_min_avg_sum(select, alias, 'MAX')
    

    def select_min(self, select = '', alias = ''):
        return self._max_min_avg_sum(select, alias, 'MIN')
    

    def select_avg(self, select = '', alias = ''):
        return self._max_min_avg_sum(select, alias, 'AVG')
    

    def select_sum(self,select = '', alias = ''):
        return self._max_min_avg_sum(select, alias, 'SUM')
    

    def _max_min_avg_sum(self,select = '', alias = '', type = 'MAX'):
        if not isinstance(select, string) or select == '':
            self.display_error('db_invalid_query')
        

        type = strtoupper(type)
        if  type not in ['MAX', 'MIN', 'AVG', 'SUM']:
            show_error(f"Invalid function type: {type}")
        

        if alias == '':
            alias = self._create_alias_from_table(select.stip())
        sql = type.'('.self.protect_identifiers(select.strip()).') AS '.self.escape_identifiers(alias.strip())

        self.qb_select.append(sql)
        self.qb_no_escape.append(None)

        if self.qb_caching == True
            self.qb_cache_select.append(sql)
            self.qb_cache_exists.append('select')
        return self
    

    def _create_alias_from_table(self,item):
    
        if '.' in item:
            return item.split('.')[:-1]

        return item
        """
        if (strpos(item, '.') !== FALSE)
            item = explode('.', item)
            return end(item)
        return item
        """
    

    // --------------------------------------------------------------------

    /**
     * DISTINCT
     *
     * Sets a flag which tells the query string compiler to add DISTINCT
     *
     * @param   bool    val
     * @return  CI_DB_query_builder
     */
    def distinct(self,val = TRUE):
    
        self.qb_distinct = is_bool(val) ? val : TRUE
        return this
    

    // --------------------------------------------------------------------

    /**
     * From
     *
     * Generates the FROM portion of the query
     *
     * @param   mixed   from    can be a string or array
     * @return  CI_DB_query_builder
     */
    def from(self,from):
    
        foreach ((array) from as val)
        
            if (strpos(val, ',') !== FALSE)
            
                foreach (explode(',', val) as v)
                
                    v = trim(v)
                    self._track_aliases(v)

                    self.qb_from[] = v = self.protect_identifiers(v, TRUE, NULL, FALSE)

                    if (self.qb_caching === TRUE)
                    
                        self.qb_cache_from[] = v
                        self.qb_cache_exists[] = 'from'
                    
                
            
            else
            
                val = trim(val)

                // Extract any aliases that might exist. We use this information
                // in the protect_identifiers to know whether to add a table prefix
                self._track_aliases(val)

                self.qb_from[] = val = self.protect_identifiers(val, TRUE, NULL, FALSE)

                if (self.qb_caching === TRUE)
                
                    self.qb_cache_from[] = val
                    self.qb_cache_exists[] = 'from'
                
            
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * JOIN
     *
     * Generates the JOIN portion of the query
     *
     * @param   string
     * @param   string  the join condition
     * @param   string  the type of join
     * @param   string  whether not to try to escape identifiers
     * @return  CI_DB_query_builder
     */
    def join(self,table, cond, type = '', escape = NULL):
    
        if (type !== '')
        
            type = strtoupper(trim(type))

            if ( ! in_array(type, array('LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER'), TRUE))
            
                type = ''
            
            else
            
                type .= ' '
            
        

        // Extract any aliases that might exist. We use this information
        // in the protect_identifiers to know whether to add a table prefix
        self._track_aliases(table)

        is_bool(escape) OR escape = self._protect_identifiers

        if ( ! self._has_operator(cond))
        
            cond = ' USING ('.(escape ? self.escape_identifiers(cond) : cond).')'
        
        elseif (escape === FALSE)
        
            cond = ' ON '.cond
        
        else
        
            // Split multiple conditions
            if (preg_match_all('/\sAND\s|\sOR\s/i', cond, joints, PREG_OFFSET_CAPTURE))
            
                conditions = array()
                joints = joints[0]
                array_unshift(joints, array('', 0))

                for (i = count(joints) - 1, pos = strlen(cond) i >= 0 i--)
                
                    joints[i][1] += strlen(joints[i][0]) // offset
                    conditions[i] = substr(cond, joints[i][1], pos - joints[i][1])
                    pos = joints[i][1] - strlen(joints[i][0])
                    joints[i] = joints[i][0]
                
            
            else
            
                conditions = array(cond)
                joints = array('')
            

            cond = ' ON '
            for (i = 0, c = count(conditions) i < c i++)
            
                operator = self._get_operator(conditions[i])
                cond .= joints[i]
                cond .= preg_match("/(\(*)?([\[\]\w\.'-]+)".preg_quote(operator)."(.*)/i", conditions[i], match)
                    ? match[1].self.protect_identifiers(match[2]).operator.self.protect_identifiers(match[3])
                    : conditions[i]
            
        

        // Do we want to escape the table name?
        if (escape === TRUE)
        
            table = self.protect_identifiers(table, TRUE, NULL, FALSE)
        

        // Assemble the JOIN statement
        self.qb_join[] = join = type.'JOIN '.table.cond

        if (self.qb_caching === TRUE)
        
            self.qb_cache_join[] = join
            self.qb_cache_exists[] = 'join'
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * WHERE
     *
     * Generates the WHERE portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param   mixed
     * @param   mixed
     * @param   bool
     * @return  CI_DB_query_builder
     */
    def where(self,key, value = NULL, escape = NULL):
    
        return self._wh('qb_where', key, value, 'AND ', escape)
    

    // --------------------------------------------------------------------

    /**
     * OR WHERE
     *
     * Generates the WHERE portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param   mixed
     * @param   mixed
     * @param   bool
     * @return  CI_DB_query_builder
     */
    def or_where(self,key, value = NULL, escape = NULL):
    
        return self._wh('qb_where', key, value, 'OR ', escape)
    

    // --------------------------------------------------------------------

    /**
     * WHERE, HAVING
     *
     * @used-by where()
     * @used-by or_where()
     * @used-by having()
     * @used-by or_having()
     *
     * @param   string  qb_key  'qb_where' or 'qb_having'
     * @param   mixed   key
     * @param   mixed   value
     * @param   string  type
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def _wh(self,qb_key, key, value = NULL, type = 'AND ', escape = NULL):
    
        qb_cache_key = (qb_key === 'qb_having') ? 'qb_cache_having' : 'qb_cache_where'

        if ( ! is_array(key))
        
            key = array(key => value)
        

        // If the escape value was not set will base it on the global setting
        is_bool(escape) OR escape = self._protect_identifiers

        foreach (key as k => v)
        
            prefix = (count(self.qb_key) === 0 && count(self.qb_cache_key) === 0)
                ? self._group_get_type('')
                : self._group_get_type(type)

            if (v !== NULL)
            
                if (escape === TRUE)
                
                    v = self.escape(v)
                

                if ( ! self._has_operator(k))
                
                    k .= ' = '
                
            
            elseif ( ! self._has_operator(k))
            
                // value appears not to have been set, assign the test to IS NULL
                k .= ' IS NULL'
            
            elseif (preg_match('/\s*(!?=|<>|\sIS(?:\s+NOT)?\s)\s*/i', k, match, PREG_OFFSET_CAPTURE))
            
                k = substr(k, 0, match[0][1]).(match[1][0] === '=' ? ' IS NULL' : ' IS NOT NULL')
            

            qb_key = array('condition' => prefix.k, 'value' => v, 'escape' => escape)
            self.qb_key[] = qb_key
            if (self.qb_caching === TRUE)
            
                self.qb_cache_key[] = qb_key
                self.qb_cache_exists[] = substr(qb_key, 3)
            

        

        return this
    

    // --------------------------------------------------------------------

    /**
     * WHERE IN
     *
     * Generates a WHERE field IN('item', 'item') SQL query,
     * joined with 'AND' if appropriate.
     *
     * @param   string  key The field to search
     * @param   array   values  The values searched on
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def where_in(self,key = NULL, values = NULL, escape = NULL):
    
        return self._where_in(key, values, FALSE, 'AND ', escape)
    

    // --------------------------------------------------------------------

    /**
     * OR WHERE IN
     *
     * Generates a WHERE field IN('item', 'item') SQL query,
     * joined with 'OR' if appropriate.
     *
     * @param   string  key The field to search
     * @param   array   values  The values searched on
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def or_where_in(self,key = NULL, values = NULL, escape = NULL):
    
        return self._where_in(key, values, FALSE, 'OR ', escape)
    

    // --------------------------------------------------------------------

    /**
     * WHERE NOT IN
     *
     * Generates a WHERE field NOT IN('item', 'item') SQL query,
     * joined with 'AND' if appropriate.
     *
     * @param   string  key The field to search
     * @param   array   values  The values searched on
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def where_not_in(self,key = NULL, values = NULL, escape = NULL):
    
        return self._where_in(key, values, TRUE, 'AND ', escape)
    

    // --------------------------------------------------------------------

    /**
     * OR WHERE NOT IN
     *
     * Generates a WHERE field NOT IN('item', 'item') SQL query,
     * joined with 'OR' if appropriate.
     *
     * @param   string  key The field to search
     * @param   array   values  The values searched on
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def or_where_not_in(self,key = NULL, values = NULL, escape = NULL):
    
        return self._where_in(key, values, TRUE, 'OR ', escape)
    

    // --------------------------------------------------------------------

    /**
     * Internal WHERE IN
     *
     * @used-by where_in()
     * @used-by or_where_in()
     * @used-by where_not_in()
     * @used-by or_where_not_in()
     *
     * @param   string  key The field to search
     * @param   array   values  The values searched on
     * @param   bool    not If the statement would be IN or NOT IN
     * @param   string  type
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def _where_in(self,key = NULL, values = NULL, not = FALSE, type = 'AND ', escape = NULL):
    
        if (key === NULL OR values === NULL)
        
            return this
        

        if ( ! is_array(values))
        
            values = array(values)
        

        is_bool(escape) OR escape = self._protect_identifiers

        not = (not) ? ' NOT' : ''

        if (escape === TRUE)
        
            where_in = array()
            foreach (values as value)
            
                where_in[] = self.escape(value)
            
        
        else
        
            where_in = array_values(values)
        

        prefix = (count(self.qb_where) === 0 && count(self.qb_cache_where) === 0)
            ? self._group_get_type('')
            : self._group_get_type(type)

        where_in = array(
            'condition' => prefix.key.not.' IN('.implode(', ', where_in).')',
            'value' => NULL,
            'escape' => escape
        )

        self.qb_where[] = where_in
        if (self.qb_caching === TRUE)
        
            self.qb_cache_where[] = where_in
            self.qb_cache_exists[] = 'where'
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * LIKE
     *
     * Generates a %LIKE% portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param   mixed   field
     * @param   string  match
     * @param   string  side
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def like(self,field, match = '', side = 'both', escape = NULL):
    
        return self._like(field, match, 'AND ', side, '', escape)
    

    // --------------------------------------------------------------------

    /**
     * NOT LIKE
     *
     * Generates a NOT LIKE portion of the query.
     * Separates multiple calls with 'AND'.
     *
     * @param   mixed   field
     * @param   string  match
     * @param   string  side
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def not_like(self,field, match = '', side = 'both', escape = NULL):
    
        return self._like(field, match, 'AND ', side, 'NOT', escape)
    

    // --------------------------------------------------------------------

    /**
     * OR LIKE
     *
     * Generates a %LIKE% portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param   mixed   field
     * @param   string  match
     * @param   string  side
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def or_like(self,field, match = '', side = 'both', escape = NULL):
    
        return self._like(field, match, 'OR ', side, '', escape)
    

    // --------------------------------------------------------------------

    /**
     * OR NOT LIKE
     *
     * Generates a NOT LIKE portion of the query.
     * Separates multiple calls with 'OR'.
     *
     * @param   mixed   field
     * @param   string  match
     * @param   string  side
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def or_not_like(self,field, match = '', side = 'both', escape = NULL):
    
        return self._like(field, match, 'OR ', side, 'NOT', escape)
    

    // --------------------------------------------------------------------

    /**
     * Internal LIKE
     *
     * @used-by like()
     * @used-by or_like()
     * @used-by not_like()
     * @used-by or_not_like()
     *
     * @param   mixed   field
     * @param   string  match
     * @param   string  type
     * @param   string  side
     * @param   string  not
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def _like(self,field, match = '', type = 'AND ', side = 'both', not = '', escape = NULL):
    
        if ( ! is_array(field))
        
            field = array(field => match)
        

        is_bool(escape) OR escape = self._protect_identifiers
        // lowercase side in case somebody writes e.g. 'BEFORE' instead of 'before' (doh)
        side = strtolower(side)

        foreach (field as k => v)
        
            prefix = (count(self.qb_where) === 0 && count(self.qb_cache_where) === 0)
                ? self._group_get_type('') : self._group_get_type(type)

            if (escape === TRUE)
            
                v = self.escape_like_str(v)
            

            switch (side)
            
                case 'none':
                    v = "'v'"
                    break
                case 'before':
                    v = "'%v'"
                    break
                case 'after':
                    v = "'v%'"
                    break
                case 'both':
                default:
                    v = "'%v%'"
                    break
            

            // some platforms require an escape sequence definition for LIKE wildcards
            if (escape === TRUE && self._like_escape_str !== '')
            
                v .= sprintf(self._like_escape_str, self._like_escape_chr)
            

            qb_where = array('condition' => "prefix k not LIKE v", 'value' => NULL, 'escape' => escape)
            self.qb_where[] = qb_where
            if (self.qb_caching === TRUE)
            
                self.qb_cache_where[] = qb_where
                self.qb_cache_exists[] = 'where'
            
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * Starts a query group.
     *
     * @param   string  not (Internal use only)
     * @param   string  type    (Internal use only)
     * @return  CI_DB_query_builder
     */
    def group_start(self,not = '', type = 'AND '):
    
        type = self._group_get_type(type)

        self.qb_where_group_started = TRUE
        prefix = (count(self.qb_where) === 0 && count(self.qb_cache_where) === 0) ? '' : type
        where = array(
            'condition' => prefix.not.str_repeat(' ', ++self.qb_where_group_count).' (',
            'value' => NULL,
            'escape' => FALSE
        )

        self.qb_where[] = where
        if (self.qb_caching)
        
            self.qb_cache_where[] = where
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but ORs the group
     *
     * @return  CI_DB_query_builder
     */
    def or_group_start(self):
    
        return self.group_start('', 'OR ')
    

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but NOTs the group
     *
     * @return  CI_DB_query_builder
     */
    def not_group_start(self):
    
        return self.group_start('NOT ', 'AND ')
    

    // --------------------------------------------------------------------

    /**
     * Starts a query group, but OR NOTs the group
     *
     * @return  CI_DB_query_builder
     */
    def or_not_group_start(self):
    
        return self.group_start('NOT ', 'OR ')
    

    // --------------------------------------------------------------------

    /**
     * Ends a query group
     *
     * @return  CI_DB_query_builder
     */
    def group_end(self):
    
        self.qb_where_group_started = FALSE
        where = array(
            'condition' => str_repeat(' ', self.qb_where_group_count--).')',
            'value' => NULL,
            'escape' => FALSE
        )

        self.qb_where[] = where
        if (self.qb_caching)
        
            self.qb_cache_where[] = where
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * Group_get_type
     *
     * @used-by group_start()
     * @used-by _like()
     * @used-by _wh()
     * @used-by _where_in()
     *
     * @param   string  type
     * @return  string
     */
    def _group_get_type(self,type):
    
        if (self.qb_where_group_started)
        
            type = ''
            self.qb_where_group_started = FALSE
        

        return type
    

    // --------------------------------------------------------------------

    /**
     * GROUP BY
     *
     * @param   string  by
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def group_by(self,by, escape = NULL):
    
        is_bool(escape) OR escape = self._protect_identifiers

        if (is_string(by))
        
            by = (escape === TRUE)
                ? explode(',', by)
                : array(by)
        

        foreach (by as val)
        
            val = trim(val)

            if (val !== '')
            
                val = array('field' => val, 'escape' => escape)

                self.qb_groupby[] = val
                if (self.qb_caching === TRUE)
                
                    self.qb_cache_groupby[] = val
                    self.qb_cache_exists[] = 'groupby'
                
            
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * HAVING
     *
     * Separates multiple calls with 'AND'.
     *
     * @param   string  key
     * @param   string  value
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def having(self,key, value = NULL, escape = NULL):
    
        return self._wh('qb_having', key, value, 'AND ', escape)
    

    // --------------------------------------------------------------------

    /**
     * OR HAVING
     *
     * Separates multiple calls with 'OR'.
     *
     * @param   string  key
     * @param   string  value
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def or_having(self,key, value = NULL, escape = NULL):
    
        return self._wh('qb_having', key, value, 'OR ', escape)
    

    // --------------------------------------------------------------------

    /**
     * ORDER BY
     *
     * @param   string  orderby
     * @param   string  direction   ASC, DESC or RANDOM
     * @param   bool    escape
     * @return  CI_DB_query_builder
     */
    def order_by(self,orderby, direction = '', escape = NULL):
    
        direction = strtoupper(trim(direction))

        if (direction === 'RANDOM')
        
            direction = ''

            // Do we have a seed value?
            orderby = ctype_digit((string) orderby)
                ? sprintf(self._random_keyword[1], orderby)
                : self._random_keyword[0]
        
        elseif (empty(orderby))
        
            return this
        
        elseif (direction !== '')
        
            direction = in_array(direction, array('ASC', 'DESC'), TRUE) ? ' '.direction : ''
        

        is_bool(escape) OR escape = self._protect_identifiers

        if (escape === FALSE)
        
            qb_orderby[] = array('field' => orderby, 'direction' => direction, 'escape' => FALSE)
        
        else
        
            qb_orderby = array()
            foreach (explode(',', orderby) as field)
            
                qb_orderby[] = (direction === '' && preg_match('/\s+(ASC|DESC)/i', rtrim(field), match, PREG_OFFSET_CAPTURE))
                    ? array('field' => ltrim(substr(field, 0, match[0][1])), 'direction' => ' '.match[1][0], 'escape' => TRUE)
                    : array('field' => trim(field), 'direction' => direction, 'escape' => TRUE)
            
        

        self.qb_orderby = array_merge(self.qb_orderby, qb_orderby)
        if (self.qb_caching === TRUE)
        
            self.qb_cache_orderby = array_merge(self.qb_cache_orderby, qb_orderby)
            self.qb_cache_exists[] = 'orderby'
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * LIMIT
     *
     * @param   int value   LIMIT value
     * @param   int offset  OFFSET value
     * @return  CI_DB_query_builder
     */
    def limit(self,value, offset = 0):
    
        is_null(value) OR self.qb_limit = (int) value
        empty(offset) OR self.qb_offset = (int) offset

        return this
    

    // --------------------------------------------------------------------

    /**
     * Sets the OFFSET value
     *
     * @param   int offset  OFFSET value
     * @return  CI_DB_query_builder
     */
    def offset(self,offset):
    
        empty(offset) OR self.qb_offset = (int) offset
        return this
    

    // --------------------------------------------------------------------

    /**
     * LIMIT string
     *
     * Generates a platform-specific LIMIT clause.
     *
     * @param   string  sql SQL Query
     * @return  string
     */
    def _limit(self,sql):
    
        return sql.' LIMIT '.(self.qb_offset ? self.qb_offset.', ' : '').(int) self.qb_limit
    

    // --------------------------------------------------------------------

    /**
     * The "set" function.
     *
     * Allows key/value pairs to be set for inserting or updating
     *
     * @param   mixed
     * @param   string
     * @param   bool
     * @return  CI_DB_query_builder
     */
    def set(self,key, value = '', escape = NULL):
    
        key = self._object_to_array(key)

        if ( ! is_array(key))
        
            key = array(key => value)
        

        is_bool(escape) OR escape = self._protect_identifiers

        foreach (key as k => v)
        
            self.qb_set[self.protect_identifiers(k, FALSE, escape)] = (escape)
                ? self.escape(v) : v
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * Get SELECT query string
     *
     * Compiles a SELECT query string and returns the sql.
     *
     * @param   string  the table name to select from (optional)
     * @param   bool    TRUE: resets QB values FALSE: leave QB values alone
     * @return  string
     */
    def get_compiled_select(self,table = '', reset = TRUE):
    
        if (table !== '')
        
            self._track_aliases(table)
            self.from(table)
        

        select = self._compile_select()

        if (reset === TRUE)
        
            self._reset_select()
        

        return select
    

    // --------------------------------------------------------------------

    /**
     * Get
     *
     * Compiles the select statement based on the other functions called
     * and runs the query
     *
     * @param   string  the table
     * @param   string  the limit clause
     * @param   string  the offset clause
     * @return  CI_DB_result
     */
    def get(self,table = '', limit = NULL, offset = NULL):
    
        if (table !== '')
        
            self._track_aliases(table)
            self.from(table)
        

        if ( ! empty(limit))
        
            self.limit(limit, offset)
        

        result = self.query(self._compile_select())
        self._reset_select()
        return result
    

    // --------------------------------------------------------------------

    /**
     * "Count All Results" query
     *
     * Generates a platform-specific query string that counts all records
     * returned by an Query Builder query.
     *
     * @param   string
     * @param   bool    the reset clause
     * @return  int
     */
    def count_all_results(self,table = '', reset = TRUE):
    
        if (table !== '')
        
            self._track_aliases(table)
            self.from(table)
        

        // ORDER BY usage is often problematic here (most notably
        // on Microsoft SQL Server) and ultimately unnecessary
        // for selecting COUNT(*) ...
        qb_orderby       = self.qb_orderby
        qb_cache_orderby = self.qb_cache_orderby
        self.qb_orderby = self.qb_cache_orderby = array()

        result = (self.qb_distinct === TRUE OR ! empty(self.qb_groupby) OR ! empty(self.qb_cache_groupby) OR self.qb_limit OR self.qb_offset)
            ? self.query(self._count_string.self.protect_identifiers('numrows')."\nFROM (\n".self._compile_select()."\n) CI_count_all_results")
            : self.query(self._compile_select(self._count_string.self.protect_identifiers('numrows')))

        if (reset === TRUE)
        
            self._reset_select()
        
        else
        
            self.qb_orderby       = qb_orderby
            self.qb_cache_orderby = qb_cache_orderby
        

        if (result->num_rows() === 0)
        
            return 0
        

        row = result->row()
        return (int) row->numrows
    

    // --------------------------------------------------------------------

    /**
     * get_where()
     *
     * Allows the where clause, limit and offset to be added directly
     *
     * @param   string  table
     * @param   string  where
     * @param   int limit
     * @param   int offset
     * @return  CI_DB_result
     */
    def get_where(self,table = '', where = NULL, limit = NULL, offset = NULL):
    
        if (table !== '')
        
            self.from(table)
        

        if (where !== NULL)
        
            self.where(where)
        

        if ( ! empty(limit))
        
            self.limit(limit, offset)
        

        result = self.query(self._compile_select())
        self._reset_select()
        return result
    

    // --------------------------------------------------------------------

    /**
     * Insert_Batch
     *
     * Compiles batch insert strings and runs the queries
     *
     * @param   string  table   Table to insert into
     * @param   array   set     An associative array of insert values
     * @param   bool    escape  Whether to escape values and identifiers
     * @return  int Number of rows inserted or FALSE on failure
     */
    def insert_batch(self,table, set = NULL, escape = NULL, batch_size = 100):
    
        if (set === NULL)
        
            if (empty(self.qb_set))
            
                return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
            
        
        else
        
            if (empty(set))
            
                return (self.db_debug) ? self.display_error('insert_batch() called with no data') : FALSE
            

            self.set_insert_batch(set, '', escape)
        

        if (strlen(table) === 0)
        
            if ( ! isset(self.qb_from[0]))
            
                return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
            

            table = self.qb_from[0]
        

        // Batch this baby
        affected_rows = 0
        for (i = 0, total = count(self.qb_set) i < total i += batch_size)
        
            if (self.query(self._insert_batch(self.protect_identifiers(table, TRUE, escape, FALSE), self.qb_keys, array_slice(self.qb_set, i, batch_size))))
            
                affected_rows += self.affected_rows()
            
        

        self._reset_write()
        return affected_rows
    

    // --------------------------------------------------------------------

    /**
     * Insert batch statement
     *
     * Generates a platform-specific insert string from the supplied data.
     *
     * @param   string  table   Table name
     * @param   array   keys    INSERT keys
     * @param   array   values  INSERT values
     * @return  string
     */
    def _insert_batch(self,table, keys, values):
    
        return 'INSERT INTO '.table.' ('.implode(', ', keys).') VALUES '.implode(', ', values)
    

    // --------------------------------------------------------------------

    /**
     * The "set_insert_batch" function.  Allows key/value pairs to be set for batch inserts
     *
     * @param   mixed
     * @param   string
     * @param   bool
     * @return  CI_DB_query_builder
     */
    def set_insert_batch(self,key, value = '', escape = NULL):
    
        key = self._object_to_array_batch(key)

        if ( ! is_array(key))
        
            key = array(key => value)
        

        is_bool(escape) OR escape = self._protect_identifiers

        keys = array_keys(self._object_to_array(reset(key)))
        sort(keys)

        foreach (key as row)
        
            row = self._object_to_array(row)
            if (count(array_diff(keys, array_keys(row))) > 0 OR count(array_diff(array_keys(row), keys)) > 0)
            
                // batch function above returns an error on an empty array
                self.qb_set[] = array()
                return
            

            ksort(row) // puts row in the same order as our keys

            if (escape !== FALSE)
            
                clean = array()
                foreach (row as value)
                
                    clean[] = self.escape(value)
                

                row = clean
            

            self.qb_set[] = '('.implode(',', row).')'
        

        foreach (keys as k)
        
            self.qb_keys[] = self.protect_identifiers(k, FALSE, escape)
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * Get INSERT query string
     *
     * Compiles an insert query and returns the sql
     *
     * @param   string  the table to insert into
     * @param   bool    TRUE: reset QB values FALSE: leave QB values alone
     * @return  string
     */
    def get_compiled_insert(self,table = '', reset = TRUE):
    
        if (self._validate_insert(table) === FALSE)
        
            return FALSE
        

        sql = self._insert(
            self.protect_identifiers(
                self.qb_from[0], TRUE, NULL, FALSE
            ),
            array_keys(self.qb_set),
            array_values(self.qb_set)
        )

        if (reset === TRUE)
        
            self._reset_write()
        

        return sql
    

    // --------------------------------------------------------------------

    /**
     * Insert
     *
     * Compiles an insert string and runs the query
     *
     * @param   string  the table to insert data into
     * @param   array   an associative array of insert values
     * @param   bool    escape  Whether to escape values and identifiers
     * @return  bool    TRUE on success, FALSE on failure
     */
    def insert(self,table = '', set = NULL, escape = NULL):
    
        if (set !== NULL)
        
            self.set(set, '', escape)
        

        if (self._validate_insert(table) === FALSE)
        
            return FALSE
        

        sql = self._insert(
            self.protect_identifiers(
                self.qb_from[0], TRUE, escape, FALSE
            ),
            array_keys(self.qb_set),
            array_values(self.qb_set)
        )

        self._reset_write()
        return self.query(sql)
    

    // --------------------------------------------------------------------

    /**
     * Validate Insert
     *
     * This method is used by both insert() and get_compiled_insert() to
     * validate that the there data is actually being set and that table
     * has been chosen to be inserted into.
     *
     * @param   string  the table to insert data into
     * @return  string
     */
    def _validate_insert(self,table = ''):
    
        if (count(self.qb_set) === 0)
        
            return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
        

        if (table !== '')
        
            self.qb_from[0] = table
        
        elseif ( ! isset(self.qb_from[0]))
        
            return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
        

        return TRUE
    

    // --------------------------------------------------------------------

    /**
     * Replace
     *
     * Compiles an replace into string and runs the query
     *
     * @param   string  the table to replace data into
     * @param   array   an associative array of insert values
     * @return  bool    TRUE on success, FALSE on failure
     */
    def replace(self,table = '', set = NULL):
    
        if (set !== NULL)
        
            self.set(set)
        

        if (count(self.qb_set) === 0)
        
            return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
        

        if (table === '')
        
            if ( ! isset(self.qb_from[0]))
            
                return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
            

            table = self.qb_from[0]
        

        sql = self._replace(self.protect_identifiers(table, TRUE, NULL, FALSE), array_keys(self.qb_set), array_values(self.qb_set))

        self._reset_write()
        return self.query(sql)
    

    // --------------------------------------------------------------------

    /**
     * Replace statement
     *
     * Generates a platform-specific replace string from the supplied data
     *
     * @param   string  the table name
     * @param   array   the insert keys
     * @param   array   the insert values
     * @return  string
     */
    def _replace(self,table, keys, values):
    
        return 'REPLACE INTO '.table.' ('.implode(', ', keys).') VALUES ('.implode(', ', values).')'
    

    // --------------------------------------------------------------------

    /**
     * FROM tables
     *
     * Groups tables in FROM clauses if needed, so there is no confusion
     * about operator precedence.
     *
     * Note: This is only used (and overridden) by MySQL and CUBRID.
     *
     * @return  string
     */
    def _from_tables(self):
    
        return implode(', ', self.qb_from)
    

    // --------------------------------------------------------------------

    /**
     * Get UPDATE query string
     *
     * Compiles an update query and returns the sql
     *
     * @param   string  the table to update
     * @param   bool    TRUE: reset QB values FALSE: leave QB values alone
     * @return  string
     */
    def get_compiled_update(self,table = '', reset = TRUE):
    
        // Combine any cached components with the current statements
        self._merge_cache()

        if (self._validate_update(table) === FALSE)
        
            return FALSE
        

        sql = self._update(self.qb_from[0], self.qb_set)

        if (reset === TRUE)
        
            self._reset_write()
        

        return sql
    

    // --------------------------------------------------------------------

    /**
     * UPDATE
     *
     * Compiles an update string and runs the query.
     *
     * @param   string  table
     * @param   array   set An associative array of update values
     * @param   mixed   where
     * @param   int limit
     * @return  bool    TRUE on success, FALSE on failure
     */
    def update(self,table = '', set = NULL, where = NULL, limit = NULL):
    
        // Combine any cached components with the current statements
        self._merge_cache()

        if (set !== NULL)
        
            self.set(set)
        

        if (self._validate_update(table) === FALSE)
        
            return FALSE
        

        if (where !== NULL)
        
            self.where(where)
        

        if ( ! empty(limit))
        
            self.limit(limit)
        

        sql = self._update(self.qb_from[0], self.qb_set)
        self._reset_write()
        return self.query(sql)
    

    // --------------------------------------------------------------------

    /**
     * Validate Update
     *
     * This method is used by both update() and get_compiled_update() to
     * validate that data is actually being set and that a table has been
     * chosen to be update.
     *
     * @param   string  the table to update data on
     * @return  bool
     */
    def _validate_update(self,table):
    
        if (count(self.qb_set) === 0)
        
            return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
        

        if (table !== '')
        
            self.qb_from = array(self.protect_identifiers(table, TRUE, NULL, FALSE))
        
        elseif ( ! isset(self.qb_from[0]))
        
            return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
        

        return TRUE
    

    // --------------------------------------------------------------------

    /**
     * Update_Batch
     *
     * Compiles an update string and runs the query
     *
     * @param   string  the table to retrieve the results from
     * @param   array   an associative array of update values
     * @param   string  the where key
     * @return  int number of rows affected or FALSE on failure
     */
    def update_batch(self,table, set = NULL, index = NULL, batch_size = 100):
    
        // Combine any cached components with the current statements
        self._merge_cache()

        if (index === NULL)
        
            return (self.db_debug) ? self.display_error('db_must_use_index') : FALSE
        

        if (set === NULL)
        
            if (empty(self.qb_set_ub))
            
                return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
            
        
        else
        
            if (empty(set))
            
                return (self.db_debug) ? self.display_error('update_batch() called with no data') : FALSE
            

            self.set_update_batch(set, index)
        

        if (strlen(table) === 0)
        
            if ( ! isset(self.qb_from[0]))
            
                return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
            

            table = self.qb_from[0]
        

        // Batch this baby
        affected_rows = 0
        for (i = 0, total = count(self.qb_set_ub) i < total i += batch_size)
        
            if (self.query(self._update_batch(self.protect_identifiers(table, TRUE, NULL, FALSE), array_slice(self.qb_set_ub, i, batch_size), index)))
            
                affected_rows += self.affected_rows()
            

            self.qb_where = array()
        

        self._reset_write()
        return affected_rows
    

    // --------------------------------------------------------------------

    /**
     * Update_Batch statement
     *
     * Generates a platform-specific batch update string from the supplied data
     *
     * @param   string  table   Table name
     * @param   array   values  Update data
     * @param   string  index   WHERE key
     * @return  string
     */
    def _update_batch(self,table, values, index):
    
        ids = array()
        foreach (values as key => val)
        
            ids[] = val[index]['value']

            foreach (array_keys(val) as field)
            
                if (field !== index)
                
                    final[val[field]['field']][] = 'WHEN '.val[index]['field'].' = '.val[index]['value'].' THEN '.val[field]['value']
                
            
        

        cases = ''
        foreach (final as k => v)
        
            cases .= k." = CASE \n"
                .implode("\n", v)."\n"
                .'ELSE '.k.' END, '
        

        self.where(val[index]['field'].' IN('.implode(',', ids).')', NULL, FALSE)

        return 'UPDATE '.table.' SET '.substr(cases, 0, -2).self._compile_wh('qb_where')
    

    // --------------------------------------------------------------------

    /**
     * The "set_update_batch" function.  Allows key/value pairs to be set for batch updating
     *
     * @param   array
     * @param   string
     * @param   bool
     * @return  CI_DB_query_builder
     */
    def set_update_batch(self,key, index = '', escape = NULL):
    
        key = self._object_to_array_batch(key)

        if ( ! is_array(key))
        
            // @todo error
        

        is_bool(escape) OR escape = self._protect_identifiers

        foreach (key as k => v)
        
            index_set = FALSE
            clean = array()
            foreach (v as k2 => v2)
            
                if (k2 === index)
                
                    index_set = TRUE
                

                clean[k2] = array(
                    'field'  => self.protect_identifiers(k2, FALSE, escape),
                    'value'  => (escape === FALSE ? v2 : self.escape(v2))
                )
            

            if (index_set === FALSE)
            
                return self.display_error('db_batch_missing_index')
            

            self.qb_set_ub[] = clean
        

        return this
    

    // --------------------------------------------------------------------

    /**
     * Empty Table
     *
     * Compiles a delete string and runs "DELETE FROM table"
     *
     * @param   string  the table to empty
     * @return  bool    TRUE on success, FALSE on failure
     */
    def empty_table(self,table = ''):
    
        if (table === '')
        
            if ( ! isset(self.qb_from[0]))
            
                return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
            

            table = self.qb_from[0]
        
        else
        
            table = self.protect_identifiers(table, TRUE, NULL, FALSE)
        

        sql = self._delete(table)
        self._reset_write()
        return self.query(sql)
    

    // --------------------------------------------------------------------

    /**
     * Truncate
     *
     * Compiles a truncate string and runs the query
     * If the database does not support the truncate() command
     * This function maps to "DELETE FROM table"
     *
     * @param   string  the table to truncate
     * @return  bool    TRUE on success, FALSE on failure
     */
    def truncate(self,table = ''):
    
        if (table === '')
        
            if ( ! isset(self.qb_from[0]))
            
                return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
            

            table = self.qb_from[0]
        
        else
        
            table = self.protect_identifiers(table, TRUE, NULL, FALSE)
        

        sql = self._truncate(table)
        self._reset_write()
        return self.query(sql)
    

    // --------------------------------------------------------------------

    /**
     * Truncate statement
     *
     * Generates a platform-specific truncate string from the supplied data
     *
     * If the database does not support the truncate() command,
     * then this method maps to 'DELETE FROM table'
     *
     * @param   string  the table name
     * @return  string
     */
    def _truncate(self,table):
    
        return 'TRUNCATE '.table
    

    // --------------------------------------------------------------------

    /**
     * Get DELETE query string
     *
     * Compiles a delete query string and returns the sql
     *
     * @param   string  the table to delete from
     * @param   bool    TRUE: reset QB values FALSE: leave QB values alone
     * @return  string
     */
    def get_compiled_delete(self,table = '', reset = TRUE):
    
        self.return_delete_sql = TRUE
        sql = self.delete(table, '', NULL, reset)
        self.return_delete_sql = FALSE
        return sql
    

    // --------------------------------------------------------------------

    /**
     * Delete
     *
     * Compiles a delete string and runs the query
     *
     * @param   mixed   the table(s) to delete from. String or array
     * @param   mixed   the where clause
     * @param   mixed   the limit clause
     * @param   bool
     * @return  mixed
     */
    def delete(self,table = '', where = '', limit = NULL, reset_data = TRUE):
    
        // Combine any cached components with the current statements
        self._merge_cache()

        if (table === '')
        
            if ( ! isset(self.qb_from[0]))
            
                return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
            

            table = self.qb_from[0]
        
        elseif (is_array(table))
        
            empty(where) && reset_data = FALSE

            foreach (table as single_table)
            
                self.delete(single_table, where, limit, reset_data)
            

            return
        
        else
        
            table = self.protect_identifiers(table, TRUE, NULL, FALSE)
        

        if (where !== '')
        
            self.where(where)
        

        if ( ! empty(limit))
        
            self.limit(limit)
        

        if (count(self.qb_where) === 0)
        
            return (self.db_debug) ? self.display_error('db_del_must_use_where') : FALSE
        

        sql = self._delete(table)
        if (reset_data)
        
            self._reset_write()
        

        return (self.return_delete_sql === TRUE) ? sql : self.query(sql)
    

    // --------------------------------------------------------------------

    /**
     * Delete statement
     *
     * Generates a platform-specific delete string from the supplied data
     *
     * @param   string  the table name
     * @return  string
     */
    def _delete(self,table):
    
        return 'DELETE FROM '.table.self._compile_wh('qb_where')
            .(self.qb_limit !== FALSE ? ' LIMIT '.self.qb_limit : '')
    

    // --------------------------------------------------------------------

    /**
     * DB Prefix
     *
     * Prepends a database prefix if one exists in configuration
     *
     * @param   string  the table
     * @return  string
     */
    def dbprefix(self,table = ''):
    
        if (table === '')
        
            self.display_error('db_table_name_required')
        

        return self.dbprefix.table
    

    // --------------------------------------------------------------------

    /**
     * Set DB Prefix
     *
     * Set's the DB Prefix to something without needing to reconnect
     *
     * @param   string  the prefix
     * @return  string
     */
    def set_dbprefix(self,prefix = ''):
    
        return self.dbprefix = prefix
    

    // --------------------------------------------------------------------

    /**
     * Track Aliases
     *
     * Used to track SQL statements written with aliased tables.
     *
     * @param   string  The table to inspect
     * @return  string
     */
    def _track_aliases(self,table):
    
        if (is_array(table))
        
            foreach (table as t)
            
                self._track_aliases(t)
            
            return
        

        // Does the string contain a comma?  If so, we need to separate
        // the string into discreet statements
        if (strpos(table, ',') !== FALSE)
        
            return self._track_aliases(explode(',', table))
        

        // if a table alias is used we can recognize it by a space
        if (strpos(table, ' ') !== FALSE)
        
            // if the alias is written with the AS keyword, remove it
            table = preg_replace('/\s+AS\s+/i', ' ', table)

            // Grab the alias
            table = trim(strrchr(table, ' '))

            // Store the alias, if it doesn't already exist
            if ( ! in_array(table, self.qb_aliased_tables, TRUE))
            
                self.qb_aliased_tables[] = table
                if (self.qb_caching === TRUE && ! in_array(table, self.qb_cache_aliased_tables, TRUE))
                
                    self.qb_cache_aliased_tables[] = table
                    self.qb_cache_exists[] = 'aliased_tables'
                
            
        
    

    // --------------------------------------------------------------------

    /**
     * Compile the SELECT statement
     *
     * Generates a query string based on which functions were used.
     * Should not be called directly.
     *
     * @param   bool    select_override
     * @return  string
     */
    def _compile_select(self,select_override = FALSE):
    
        // Combine any cached components with the current statements
        self._merge_cache()

        // Write the "select" portion of the query
        if (select_override !== FALSE)
        
            sql = select_override
        
        else
        
            sql = ( ! self.qb_distinct) ? 'SELECT ' : 'SELECT DISTINCT '

            if (count(self.qb_select) === 0)
            
                sql .= '*'
            
            else
            
                // Cycle through the "select" portion of the query and prep each column name.
                // The reason we protect identifiers here rather than in the select() function
                // is because until the user calls the from() function we don't know if there are aliases
                foreach (self.qb_select as key => val)
                
                    no_escape = isset(self.qb_no_escape[key]) ? self.qb_no_escape[key] : NULL
                    self.qb_select[key] = self.protect_identifiers(val, FALSE, no_escape)
                

                sql .= implode(', ', self.qb_select)
            
        

        // Write the "FROM" portion of the query
        if (count(self.qb_from) > 0)
        
            sql .= "\nFROM ".self._from_tables()
        

        // Write the "JOIN" portion of the query
        if (count(self.qb_join) > 0)
        
            sql .= "\n".implode("\n", self.qb_join)
        

        sql .= self._compile_wh('qb_where')
            .self._compile_group_by()
            .self._compile_wh('qb_having')
            .self._compile_order_by() // ORDER BY

        // LIMIT
        if (self.qb_limit !== FALSE OR self.qb_offset)
        
            return self._limit(sql."\n")
        

        return sql
    

    // --------------------------------------------------------------------

    /**
     * Compile WHERE, HAVING statements
     *
     * Escapes identifiers in WHERE and HAVING statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of whether
     * where(), or_where(), having(), or_having are called prior to from(),
     * join() and dbprefix is added only if needed.
     *
     * @param   string  qb_key  'qb_where' or 'qb_having'
     * @return  string  SQL statement
     */
    def _compile_wh(self,qb_key):
    
        if (count(self.qb_key) > 0)
        
            for (i = 0, c = count(self.qb_key) i < c i++)
            
                // Is this condition already compiled?
                if (is_string(self.qb_key[i]))
                
                    continue
                
                elseif (self.qb_key[i]['escape'] === FALSE)
                
                    self.qb_key[i] = self.qb_key[i]['condition'].(isset(self.qb_key[i]['value']) ? ' '.self.qb_key[i]['value'] : '')
                    continue
                

                // Split multiple conditions
                conditions = preg_split(
                    '/((?:^|\s+)AND\s+|(?:^|\s+)OR\s+)/i',
                    self.qb_key[i]['condition'],
                    -1,
                    PREG_SPLIT_DELIM_CAPTURE | PREG_SPLIT_NO_EMPTY
                )

                for (ci = 0, cc = count(conditions) ci < cc ci++)
                
                    if ((op = self._get_operator(conditions[ci])) === FALSE
                        OR ! preg_match('/^(\(?)(.*)('.preg_quote(op, '/').')\s*(.*(?<!\)))?(\)?)/i', conditions[ci], matches))
                    
                        continue
                    

                    // matches = array(
                    //  0 => '(test <= foo)',   /* the whole thing */
                    //  1 => '(',       /* optional */
                    //  2 => 'test',        /* the field name */
                    //  3 => ' <= ',        /* op */
                    //  4 => 'foo',     /* optional, if op is e.g. 'IS NULL' */
                    //  5 => ')'        /* optional */
                    // )

                    if ( ! empty(matches[4]))
                    
                        self._is_literal(matches[4]) OR matches[4] = self.protect_identifiers(trim(matches[4]))
                        matches[4] = ' '.matches[4]
                    

                    conditions[ci] = matches[1].self.protect_identifiers(trim(matches[2]))
                        .' '.trim(matches[3]).matches[4].matches[5]
                

                self.qb_key[i] = implode('', conditions).(isset(self.qb_key[i]['value']) ? ' '.self.qb_key[i]['value'] : '')
            

            return (qb_key === 'qb_having' ? "\nHAVING " : "\nWHERE ")
                .implode("\n", self.qb_key)
        

        return ''
    

    // --------------------------------------------------------------------

    /**
     * Compile GROUP BY
     *
     * Escapes identifiers in GROUP BY statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of whether
     * group_by() is called prior to from(), join() and dbprefix is added
     * only if needed.
     *
     * @return  string  SQL statement
     */
    def _compile_group_by(self):
    
        if (count(self.qb_groupby) > 0)
        
            for (i = 0, c = count(self.qb_groupby) i < c i++)
            
                // Is it already compiled?
                if (is_string(self.qb_groupby[i]))
                
                    continue
                

                self.qb_groupby[i] = (self.qb_groupby[i]['escape'] === FALSE OR self._is_literal(self.qb_groupby[i]['field']))
                    ? self.qb_groupby[i]['field']
                    : self.protect_identifiers(self.qb_groupby[i]['field'])
            

            return "\nGROUP BY ".implode(', ', self.qb_groupby)
        

        return ''
    

    // --------------------------------------------------------------------

    /**
     * Compile ORDER BY
     *
     * Escapes identifiers in ORDER BY statements at execution time.
     *
     * Required so that aliases are tracked properly, regardless of whether
     * order_by() is called prior to from(), join() and dbprefix is added
     * only if needed.
     *
     * @return  string  SQL statement
     */
    def _compile_order_by(self):
    
        if (empty(self.qb_orderby))
        
            return ''
        

        for (i = 0, c = count(self.qb_orderby) i < c i++)
        
            if (is_string(self.qb_orderby[i]))
            
                continue
            

            if (self.qb_orderby[i]['escape'] !== FALSE && ! self._is_literal(self.qb_orderby[i]['field']))
            
                self.qb_orderby[i]['field'] = self.protect_identifiers(self.qb_orderby[i]['field'])
            

            self.qb_orderby[i] = self.qb_orderby[i]['field'].self.qb_orderby[i]['direction']
        

        return "\nORDER BY ".implode(', ', self.qb_orderby)
    

    // --------------------------------------------------------------------

    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param   object
     * @return  array
     */
    def _object_to_array(self,object):
    
        if ( ! is_object(object))
        
            return object
        

        array = array()
        foreach (get_object_vars(object) as key => val)
        
            // There are some built in keys we need to ignore for this conversion
            if ( ! is_object(val) && ! is_array(val) && key !== '_parent_name')
            
                array[key] = val
            
        

        return array
    

    // --------------------------------------------------------------------

    /**
     * Object to Array
     *
     * Takes an object as input and converts the class variables to array key/vals
     *
     * @param   object
     * @return  array
     */
    def _object_to_array_batch(self,object):
    
        if ( ! is_object(object))
        
            return object
        

        array = array()
        out = get_object_vars(object)
        fields = array_keys(out)

        foreach (fields as val)
        
            // There are some built in keys we need to ignore for this conversion
            if (val !== '_parent_name')
            
                i = 0
                foreach (out[val] as data)
                
                    array[i++][val] = data
                
            
        

        return array
    

    // --------------------------------------------------------------------

    /**
     * Start Cache
     *
     * Starts QB caching
     *
     * @return  CI_DB_query_builder
     */
    def start_cache(self):
    
        self.qb_caching = TRUE
        return this
    

    // --------------------------------------------------------------------

    /**
     * Stop Cache
     *
     * Stops QB caching
     *
     * @return  CI_DB_query_builder
     */
    def stop_cache(self):
    
        self.qb_caching = FALSE
        return this
    

    // --------------------------------------------------------------------

    /**
     * Flush Cache
     *
     * Empties the QB cache
     *
     * @return  CI_DB_query_builder
     */
    def flush_cache(self):
    
        self._reset_run(array(
            'qb_cache_select'       => array(),
            'qb_cache_from'         => array(),
            'qb_cache_join'         => array(),
            'qb_cache_where'        => array(),
            'qb_cache_groupby'      => array(),
            'qb_cache_having'       => array(),
            'qb_cache_orderby'      => array(),
            'qb_cache_set'          => array(),
            'qb_cache_exists'       => array(),
            'qb_cache_no_escape'    => array(),
            'qb_cache_aliased_tables'   => array()
        ))

        return this
    

    // --------------------------------------------------------------------

    /**
     * Merge Cache
     *
     * When called, this function merges any cached QB arrays with
     * locally called ones.
     *
     * @return  void
     */
    def _merge_cache(self):
    
        if (count(self.qb_cache_exists) === 0)
        
            return
        
        elseif (in_array('select', self.qb_cache_exists, TRUE))
        
            qb_no_escape = self.qb_cache_no_escape
        

        foreach (array_unique(self.qb_cache_exists) as val) // select, from, etc.
        
            qb_variable = 'qb_'.val
            qb_cache_var    = 'qb_cache_'.val
            qb_ = self.qb_cache_var

            for (i = 0, c = count(self.qb_variable) i < c i++)
            
                if ( ! in_array(self.qb_variable[i], qb_new, TRUE))
                
                    qb_new[] = self.qb_variable[i]
                    if (val === 'select')
                    
                        qb_no_escape[] = self.qb_no_escape[i]
                    
                
            

            self.qb_variable = qb_new
            if (val === 'select')
            
                self.qb_no_escape = qb_no_escape
            
        
    

    // --------------------------------------------------------------------

    /**
     * Is literal
     *
     * Determines if a string represents a literal value or a field name
     *
     * @param   string  str
     * @return  bool
     */
    def _is_literal(self,str):
    
        str = trim(str)

        if (empty(str) OR ctype_digit(str) OR (string) (float) str === str OR in_array(strtoupper(str), array('TRUE', 'FALSE'), TRUE))
        
            return TRUE
        

        static _str

        if (empty(_str))
        
            _str = (self._escape_char !== '"')
                ? array('"', "'") : array("'")
        

        return in_array(str[0], _str, TRUE)
    

    // --------------------------------------------------------------------

    /**
     * Reset Query Builder values.
     *
     * Publicly-visible method to reset the QB values.
     *
     * @return  CI_DB_query_builder
     */
    def reset_query(self):
    
        self._reset_select()
        self._reset_write()
        return this
    

    // --------------------------------------------------------------------

    /**
     * Resets the query builder values.  Called by the get() function
     *
     * @param   array   An array of fields to reset
     * @return  void
     */
    def _reset_run(self,qb_reset_items):
    
        foreach (qb_reset_items as item => default_value)
        
            self.item = default_value
        
    

    // --------------------------------------------------------------------

    /**
     * Resets the query builder values.  Called by the get() function
     *
     * @return  void
     */
    def _reset_select(self):
    
        self._reset_run(array(
            'qb_select'     => array(),
            'qb_from'       => array(),
            'qb_join'       => array(),
            'qb_where'      => array(),
            'qb_groupby'        => array(),
            'qb_having'     => array(),
            'qb_orderby'        => array(),
            'qb_aliased_tables' => array(),
            'qb_no_escape'      => array(),
            'qb_distinct'       => FALSE,
            'qb_limit'      => FALSE,
            'qb_offset'     => FALSE
        ))
    

    // --------------------------------------------------------------------

    /**
     * Resets the query builder "write" values.
     *
     * Called by the insert() update() insert_batch() update_batch() and delete() functions
     *
     * @return  void
     */
    def _reset_write(self):
    
        self._reset_run(array(
            'qb_set'    => array(),
            'qb_set_ub' => array(),
            'qb_from'   => array(),
            'qb_join'   => array(),
            'qb_where'  => array(),
            'qb_orderby'    => array(),
            'qb_keys'   => array(),
            'qb_limit'  => FALSE
        ))
    

