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
    

    def select_sum(self, select = '', alias = ''):
        return self._max_min_avg_sum(select, alias, 'SUM')
    

    def _max_min_avg_sum(self, select = '', alias = '', type = 'MAX'):
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
    

    def _create_alias_from_table(self, item):
    
        if '.' in item:
            return item.split('.')[:-1]

        return item
        """
        if (strpos(item, '.') !== FALSE)
            item = explode('.', item)
            return end(item)
        return item
        """
    

    def distinct(self, val = True):
        self.ar_distinct = val if isinstance(val, bool) else True
        return self
    

    def table(self, table):
        for val in table:
            if ',' in val:
                for v in val.split(','):
                    v = v.strip()
                    self._track_aliases(v)

                    self.ar_from.append(self._protect_identifiers(v, True, None, False))

                    if self.ar_caching == True:
                        self.ar_cache_from.append(self._protect_identifiers(v, True, None, False))
                        self.ar_cache_exists.append('from')
            else:
                val = val.strip()

                # any aliases that might exist.  We use this information
                # the _protect_identifiers to know whether to add a table prefix
                self._track_aliases(val)

                self.ar_from.append(self._protect_identifiers(val, True, None, False))

                if self.ar_caching == True:
                    self.ar_cache_from.append(self._protect_identifiers(val, True, None, False))
                    self.ar_cache_exists.append('from')
        return self

    def join(self, table, cond, type = '', escape = None):
    
        if type != '':
            type = type.strip().upper()

            if type not in ['LEFT', 'RIGHT', 'OUTER', 'INNER', 'LEFT OUTER', 'RIGHT OUTER']:
                type = ''
            else:
                type += ' '    
        

        # Extract any aliases that might exist. We use this information
        # in the protect_identifiers to know whether to add a table prefix
        self._track_aliases(table)

        if not isinstance(escape, bool):
            escape = self._protect_identifiers

        if not self._has_operator(cond):
            cond = f' USING ('{escape = if self.escape_identifiers(cond) else: cond}')'
        else if escape == False:
            cond = ' ON '.cond
        
        else:
        
            # Split multiple conditions
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
            
        
        if escape == True:
            table = self.protect_identifiers(table, True, None, False)
        

        
        join = type.'JOIN '.table.cond
        self.qb_join.append(join)
        if self.qb_caching == True:
            self.qb_cache_join.append(join)
            self.qb_cache_exists.append('join')
        return self
    

    def where(self, key, value = NULL, escape = NULL):
        return self._wh('qb_where', key, value, 'AND ', escape)
    
    def or_where(self, key, value = NULL, escape = NULL):    
        return self._wh('qb_where', key, value, 'OR ', escape)
    

    def _wh(self, qb_key, key, value = NULL, type = 'AND ', escape = NULL):
        qb_cache_key = 'qb_cache_having' if qb_key == 'qb_having' else 'qb_cache_where'

        if instance(key, dict):
            key = {key: value}
        
        # If the escape value was not set will base it on the global setting
        if not isinstance(escape, bool):
            escape = self.__protect_identifiers

        for k,v in key.items():
        
            if isinstance(v, int):
                v = str(v)
            prefix = '' if len(self.ar_where) == 0 and len(self.ar_cache_where) == 0 else type

            if v is None and not self._has_operator(k):
                # value appears not to have been set, assign the test to IS NULL
                k += ' IS NULL'

            if not v is None:
                if escape == True:
                    k = self._protect_identifiers(k, False, escape)

                    v = ' ' + self.escape(v)
                
                if not self._has_operator(k):
                    k += ' = '

            match = re.search('/\s*(!?=|<>|\sIS(?:\s+NOT)?\s)\s*/i', k, $match)
            if match:
                k = k.[0, match[0][1]].append(" IS NULL" if match[1][0] == '==' else " IS NOT NULL")
                k = match[1] + match[2] + match[3]
            else:
                k = self._protect_identifiers(k, False, escape)
            
            qb_key = {
                'condition': f"{prefix}{k}", 
                'value': v, 
                'escape': escape
            }
            self.qb_key.append(qb_key)
            if self.qb_caching == True:
                self.qb_cache_key.append(qb_key)
                self.qb_cache_exists.append(qb_key[qb_key:3])
        return self
    

    def where_in(self, key = None, values = None, escape = None):
        return self._where_in(key, values, False, 'AND ', escape)
    

    def or_where_in(self, key = None, values = None, escape = None):
        return self._where_in(key, values, False, 'OR ', escape)
    
    def where_not_in(self, key = None, values = None, escape = None):    
        return self._where_in(key, values, True, 'AND ', escape)
    
    def or_where_not_in(self, key = None, values = None, escape = None):
        return self._where_in(key, values, True, 'OR ', escape)
    
    def _where_in(self, key = None, values = None, not_in = False, type = 'AND ', escape = None):
    
        if key == None or values == None:
            return

        if not isinstance(values, list):
            values = list(values)

        if not isinstance(escape, bool):
            escape = self.__protect_identifiers

        not_in = ' NOT' if not_in else ''

        if escape:
            where_in = []
            for value in values:
                where_in.append(self.escape(value))
        else:
            where_in = values.items()
        

        prefix = '' if len(self.qb_where) == 0 and len(self.qb_cache_where) == 0 else type

        where_in = {
            'condition': f"{prefix} {key} {not_in} IN({", ".join(self.ar_wherein)}) "
            'value': None,
            'escape': escape
        }

        self.qb_where.append(where_in)
        if self.qb_caching == True:
            self.qb_cache_where.append(where_in)
            self.qb_cache_exists.append('where')
        return self
    

    def like(self, field, match = '', side = 'both', escape = None):
        return self._like(field, match, 'AND ', side, '', escape)
    
    def not_like(self, field, match = '', side = 'both', escape = None):
        return self._like(field, match, 'AND ', side, 'NOT', escape)
    
    def or_like(self, field, match = '', side = 'both', escape = None):
        return self._like(field, match, 'OR ', side, '', escape)
    
    def or_not_like(self, field, match = '', side = 'both', escape = None):
        return self._like(field, match, 'OR ', side, 'NOT', escape)
    
    def _like(self, field, match = '', type = 'AND ', side = 'both', not_in = '', escape = None):
    
        if not isinstance(field, dict):
            field = {field: match}


        if not isinstance(escape, bool):
            escape = self.__protect_identifiers

        # lowercase side in case somebody writes e.g. 'BEFORE' instead of 'before' (doh)
        side = side.lower()

        for k,v in field.item():
        
            prefix = '' if len(self.qb_where) == 0 and len(self.qb_cache_where) else type

            if escape:
                v = self.escape_like_str(v)
            
            if side == 'none':
                v = f"'{v}'"
            elif side == 'before':
                v = f"'%{v}'"
            elif side == 'after':
                v = f"'{v}%'"
            else:
                v = f"'%{v}%'"

            # some platforms require an escape sequence definition for LIKE wildcards
            if escape and self._like_escape_str !== '':
                v = v % (self._like_escape_str, self._like_escape_chr)
            

            qb_where = {
                'condition': f"{prefix} {k} {not_in} LIKE {v}", 
                'value': None, 
                'escape': escape
            }
            self.qb_where.append(qb_where)
            if self.qb_caching:
                self.qb_cache_where.append(qb_where)
                self.qb_cache_exists.append('where')
        return self
    

    def group_start(self, not_in = '', type = 'AND '):
        type = self._group_get_type(type)
        self.qb_where_group_started = True
        prefix = "" if len(self.qb_where) == 0 and len(self.qb_cache_where) == 0 else type
        v = ''
        for c in range(1, self.qb_where_group_count):
            v = v % v
        where = {
            'condition': f"{prefix} {not_in} {v}",
            'value': None
            'escape': False
        }
        self.qb_where.append(where)
        if self.qb_caching
            self.qb_cache_where.append(where)
        return self
    

    def or_group_start(self):
        return self.group_start('', 'OR ')
    

    def not_group_start(self):
        return self.group_start('NOT ', 'AND ')
    

    def or_not_group_start(self):    
        return self.group_start('NOT ', 'OR ')
    
    def group_end(self):
        self.qb_where_group_started = False
        v = ''
        for c in range(1, self.qb_where_group_count):
            v = v % v
        where = array(
            'condition': f'{v})'
            'value': None,
            'escape': False
        )

        self.qb_where.append(where)
        if self.qb_caching:
            self.qb_cache_where.append(where)
        return self
    

    def _group_get_type(self, type):
        if self.qb_where_group_started:
            type = ''
            self.qb_where_group_started = False
        return type
    

    def group_by(self, by, escape = None):

        if not isinstance(escape, bool):
            escape = self.__protect_identifiers

        if isinstance(by, str):
            by = by.split(",") if escape else [by]

        for val in by:
            val = val.strip()
            
            if val != '':
                val = {
                    'field': val, 
                    'escape': escape
                }
                self.qb_groupby.append(val)
                if self.qb_caching:
                    self.qb_cache_groupby.append(val)
                    self.qb_cache_exists.append('groupby')
        return self
    

    def having(self, key, value = None, escape = None):
        return self._wh('qb_having', key, value, 'AND ', escape)
    
    def or_having(self, key, value = None, escape = None):
        return self._wh('qb_having', key, value, 'OR ', escape)
    
    def order_by(self, orderby, direction = '', escape = None):
        direction = direction.strip().upper()

        if direction == 'RANDOM':
            direction = ''
            # Do we have a seed value?
            orderby = self._random_keyword[1] if isinstance(order_by, int) else self._random_keyword[0]
        elif len(orderby) == 0:
            return self
        elif direction != '':
            direction = ' ' + direction if direction in ['ASC', 'DESC'] else ' ASC'
            
        if not isinstance(escape, bool):
            escape = self.__protect_identifiers

        if not escape:
            qb_orderby.append({'field': orderby, 'direction': direction, 'escape': False})
        else:
            qb_orderby = []
            for field in orderby.split(","):
                match = re.search('/\s+(ASC|DESC)/i', field, match)
                if (direction == '' and match):
                    qb_orderby.append({'field': match[0][1][field, 0], 'direction': f"{match[1][0]}", escape: True })
                else:
                    qb_orderby.append({'field': field, 'direction': direction, 'escape': True})

        self.qb_orderby.append(qb_orderby)
        if self.qb_caching:
            self.qb_cache_orderby.append(qb_orderby)
            self.qb_cache_exists.append('orderby')
        return self
    

    def limit(self, value, offset = 0):
    
        is_null(value) OR self.qb_limit = (int) value
        empty(offset) OR self.qb_offset = (int) offset

        return this
    
    def offset(self,offset):
    
        empty(offset) OR self.qb_offset = (int) offset
        return this
    
    def _limit(self,sql):
    
        return sql.' LIMIT '.(self.qb_offset ? self.qb_offset.', ' : '').(int) self.qb_limit
    

    def set(self,key, value = '', escape = NULL):
    
        key = self._object_to_array(key)

        if ( ! is_array(key))
        
            key = array(key => value)
        

        is_bool(escape) OR escape = self._protect_identifiers

        foreach (key as k => v)
        
            self.qb_set[self.protect_identifiers(k, FALSE, escape)] = (escape)
                ? self.escape(v) : v
        

        return this
    
    def get_compiled_select(self,table = '', reset = TRUE):
    
        if (table !== '')
        
            self._track_aliases(table)
            self.from(table)
        

        select = self._compile_select()

        if (reset === TRUE)
        
            self._reset_select()
        

        return select
    

    def get(self,table = '', limit = NULL, offset = NULL):
    
        if (table !== '')
        
            self._track_aliases(table)
            self.from(table)
        

        if ( ! empty(limit))
        
            self.limit(limit, offset)
        

        result = self.query(self._compile_select())
        self._reset_select()
        return result
    

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
    

    def _insert_batch(self,table, keys, values):
    
        return 'INSERT INTO '.table.' ('.implode(', ', keys).') VALUES '.implode(', ', values)
    
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
    

    def _validate_insert(self,table = ''):
    
        if (count(self.qb_set) === 0)
        
            return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
        

        if (table !== '')
        
            self.qb_from[0] = table
        
        elseif ( ! isset(self.qb_from[0]))
        
            return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
        

        return TRUE
    

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
    

    def _replace(self,table, keys, values):
    
        return 'REPLACE INTO '.table.' ('.implode(', ', keys).') VALUES ('.implode(', ', values).')'
    

    def _from_tables(self):
    
        return implode(', ', self.qb_from)
    

    def get_compiled_update(self,table = '', reset = TRUE):
    
        // Combine any cached components with the current statements
        self._merge_cache()

        if (self._validate_update(table) === FALSE)
        
            return FALSE
        

        sql = self._update(self.qb_from[0], self.qb_set)

        if (reset === TRUE)
        
            self._reset_write()
        

        return sql
    

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
    

    def _validate_update(self,table):
    
        if (count(self.qb_set) === 0)
        
            return (self.db_debug) ? self.display_error('db_must_use_set') : FALSE
        

        if (table !== '')
        
            self.qb_from = array(self.protect_identifiers(table, TRUE, NULL, FALSE))
        
        elseif ( ! isset(self.qb_from[0]))
        
            return (self.db_debug) ? self.display_error('db_must_set_table') : FALSE
        

        return TRUE
    

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
    

    def _truncate(self,table):
    
        return 'TRUNCATE '.table
    

    def get_compiled_delete(self,table = '', reset = TRUE):
    
        self.return_delete_sql = TRUE
        sql = self.delete(table, '', NULL, reset)
        self.return_delete_sql = FALSE
        return sql
    

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
    

    def _delete(self,table):
    
        return 'DELETE FROM '.table.self._compile_wh('qb_where')
            .(self.qb_limit !== FALSE ? ' LIMIT '.self.qb_limit : '')
    

    def dbprefix(self,table = ''):
    
        if (table === '')
        
            self.display_error('db_table_name_required')
        

        return self.dbprefix.table
    

    def set_dbprefix(self,prefix = ''):
    
        return self.dbprefix = prefix
    

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
    

    def _object_to_array(self,object):
    
        if ( ! is_object(object))
        
            return object
        

        array = array()
        foreach (get_object_vars(object) as key => val)
        
            // There are some built in keys we need to ignore for this conversion
            if ( ! is_object(val) && ! is_array(val) && key !== '_parent_name')
            
                array[key] = val
            
        

        return array
    

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
    

    def start_cache(self):
    
        self.qb_caching = TRUE
        return this
    

    def stop_cache(self):
    
        self.qb_caching = FALSE
        return this
    

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
            
        
    

    def _is_literal(self,str):
    
        str = trim(str)

        if (empty(str) OR ctype_digit(str) OR (string) (float) str === str OR in_array(strtoupper(str), array('TRUE', 'FALSE'), TRUE))
        
            return TRUE
        

        static _str

        if (empty(_str))
        
            _str = (self._escape_char !== '"')
                ? array('"', "'") : array("'")
        

        return in_array(str[0], _str, TRUE)
    

    def reset_query(self):
    
        self._reset_select()
        self._reset_write()
        return this
    

    def _reset_run(self,qb_reset_items):
    
        foreach (qb_reset_items as item => default_value)
        
            self.item = default_value
        
    

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