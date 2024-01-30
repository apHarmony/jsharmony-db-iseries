/*
Copyright 2022 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var DB = require('jsharmony-db');
var types = DB.types;
var typeHandler = require('./DB.iseries.types.js');
var odbc = require('odbc');
var async = require('async');
var _ = require('lodash');
var moment = require('moment');
var {performance, PerformanceObserver} = require('perf_hooks');

odbc.SQL_CHAR = 1;
// varchar 12 already defined
odbc.SQL_CLOB = -1;
odbc.SQL_NCHAR = -8;
odbc.SQL_NVARCHAR = -9;
odbc.SQL_NCLOB = -10;
// nchar and graphic have the same type code

function DBdriver() {
  this.name = 'iseries';
  this.sql = require('./DB.iseries.sql.js');
  this.meta = require('./DB.iseries.meta.js');
  this.pools = []; /* { dbconfig: xxx, con: yyy } */
  this.initStatements = [
    "DECLARE GLOBAL TEMPORARY TABLE SESSION.JSHARMONY_META AS (SELECT 'USystem' CONTEXT FROM SYSIBM.SYSDUMMY1) WITH DATA WITH REPLACE",
  ];

  var _this = this;
  this.obs = new PerformanceObserver(function(list, observer) {
    list.getEntries().forEach(function(entry) {
      _this.platform.Log.info(entry.name + ': ' + entry.duration, { source: 'database' });
    });
  });
  
  //Initialize platform
  this.platform = {
    Log: function(msg){ console.log(msg); }, // eslint-disable-line no-console
    Config: {
      debug_params: {
        db_log_level: 6,           //Bitmask: 2 = WARNING, 4 = NOTICES :: Database messages logged to the console / log
        db_error_sql_state: false,  //Log SQL state during DB error
        db_perf_reporting: false // log performance metrics
      }
    }
  };
  this.platform.Log.info = function(msg){ console.log(msg); }; // eslint-disable-line no-console
  this.platform.Log.warning = function(msg){ console.log(msg); }; // eslint-disable-line no-console
  this.platform.Log.error = function(msg){ console.log(msg); }; // eslint-disable-line no-console
}

DBdriver.prototype.getDefaultSchema = function(){
  return 'QGPL';
};

DBdriver.prototype.logRawSQL = function(sql){
  if (this.platform.Config.debug_params && this.platform.Config.debug_params.db_raw_sql && this.platform.Log) {
    this.platform.Log.info(sql, { source: 'database_raw_sql' });
  }
};

function initDBConfig(dbconfig){
  if(!dbconfig) return;
  if(!dbconfig.options) dbconfig.options = {};
  if(!dbconfig.options.pooled) dbconfig.options.pooled = false;
  if(dbconfig.options.automatic_compound_commands !== false) dbconfig.options.automatic_compound_commands = true;
  if(dbconfig.options.autotrim !== false) dbconfig.options.autotrim = true;
  if(dbconfig.options.idle_disconnect !== false && typeof(dbconfig.options.idle_disconnect) != 'number') dbconfig.options.idle_disconnect = 30 * 60 * 1000;
  console.log(dbconfig);
}

DBdriver.prototype.togglePerformanceLogging = function() {
  if (this.platform.Config.debug_params && this.platform.Config.debug_params.db_perf_reporting === true && this.platform.Log) {
    this.obs.observe({entryTypes: ['measure']});
  } else {
    this.obs.disconnect();
  }
};

DBdriver.prototype.getConnectionPool = function (dbconfig, onPool) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;

  var odbcpool = null;
  //Check if pool was already added
  for(var i=0;i<this.pools.length;i++){
    if(this.pools[i].dbconfig==dbconfig) odbcpool = this.pools[i];
  }
  //Add pool if it does not exist
  if(!odbcpool){
    _this.pools.push({
      dbconfig: dbconfig,
      pool: null,
      pending: [],
      idleDisconnectHandle: null,
    });
    odbcpool = _this.pools[_this.pools.length - 1];
  }

  if(odbcpool.pool) {
    resetIdle(odbcpool);
    return onPool(null, odbcpool.pool);
  } else if (odbcpool.pending.length > 0) odbcpool.pending.push(onPool);
  //Initialize pool connection if it was not initialized
  else {
    performance.mark('odbc.pool start');
    odbcpool.pending.push(onPool);
    odbc.pool(dbconfig, function(err, pool) {
      performance.mark('odbc.pool end');
      performance.measure('odbc.pool', 'odbc.pool start', 'odbc.pool end');
      if (err) {
        pool.close();
        pool = null;
      }
      odbcpool.pool = pool;
      resetIdle(odbcpool);
      odbcpool.pending.forEach(function(f) {f(err, pool);});
      odbcpool.pending = [];
    });
  }
};

function closePool(odbcpool, pool_cb) {
  pool_cb = pool_cb || function() {};
  if(!odbcpool.pool) return pool_cb();
  clearTimeout(odbcpool.idleDisconnectHandle);
  odbcpool.idleDisconnectHandle = null;
  performance.mark('pool.close start');
  odbcpool.pool.close(function(){
    performance.mark('pool.close end');
    performance.measure('pool.close', 'pool.close start', 'pool.close end');
    odbcpool.pool = null;
    odbcpool.pending = [];
    pool_cb();
  });
}

function resetIdle(odbcpool) {
  clearTimeout(odbcpool.idleDisconnectHandle);
  odbcpool.idleDisconnectHandle = null;
  var idle_disconnect = odbcpool.dbconfig.options.idle_disconnect;
  if (odbcpool.pool && typeof(idle_disconnect) == 'number' && idle_disconnect > 0) {
    odbcpool.idleDisconnectHandle = setTimeout(closePool, idle_disconnect, odbcpool);
  }
}

DBdriver.prototype.Close = function(onClosed){
  var _this = this;
  async.each(_this.pools, closePool, onClosed);
};

DBdriver.prototype.getDBParam = function (dbtype, val) {
  var _this = this;
  if (!dbtype) throw new Error('Cannot get dbtype of null object');
  if (val === null) return 'NULL';
  if (typeof val === 'undefined') return 'NULL';
  
  if ((dbtype.name == 'VarChar') || (dbtype.name == 'Char')) {
    var valstr = val.toString();
    if ((dbtype.length == types.MAX) || (dbtype.length == -1)) return "'" + _this.escape(valstr) + "'";
    return "'" + _this.escape(valstr.substring(0, dbtype.length)) + "'";
  }
  else if (dbtype.name == 'VarBinary') {
    var valbin = null;
    if (val instanceof Buffer) valbin = val;
    else valbin = Buffer.from(val.toString());
    if (valbin.legth == 0) return "NULL";
    return "BX'" + valbin.toString('hex').toUpperCase() + "'";
  }
  else if ((dbtype.name == 'BigInt') || (dbtype.name == 'Int') || (dbtype.name == 'SmallInt') || (dbtype.name == 'TinyInt')) {
    var valint = parseInt(val);
    if (isNaN(valint)) { return "NULL"; }
    return valint.toString();
  }
  else if (dbtype.name == 'Boolean') {
    if((val==='')||(typeof val == 'undefined')) return "NULL";
    if(typeHandler.boolParser(val)) return '1';
    return '0';
  }
  else if (dbtype.name == 'Decimal') {
    let valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    return _this.escape(val.toString());
  }
  else if (dbtype.name == 'Float') {
    let valfloat = parseFloat(val);
    if (isNaN(valfloat)) { return "NULL"; }
    return _this.escape(val.toString());
  }
  else if ((dbtype.name == 'Date') || (dbtype.name == 'Time') || (dbtype.name == 'DateTime')) {
    var suffix = '';

    var valdt = null;
    if (val instanceof Date) { valdt = val; }
    else if(_.isNumber(val) && !isNaN(val)){
      valdt = moment(moment.utc(val).format('YYYY-MM-DDTHH:mm:ss.SSS'), "YYYY-MM-DDTHH:mm:ss.SSS").toDate();
    }
    else {
      if (isNaN(Date.parse(val))) return "NULL";
      valdt = new Date(val);
    }

    var mdate = moment(valdt);
    if (!mdate.isValid()) return "NULL";

    if(!_.isNumber(val) && !_.isString(val)){
      if('jsh_utcOffset' in val){
        //Time is in UTC, Offset specifies amount and timezone
        var neg = false;
        if(val.jsh_utcOffset < 0){ neg = true; }
        suffix = moment.utc(new Date(val.jsh_utcOffset*(neg?-1:1)*60*1000)).format('HH:mm');
        //Reverse offset
        suffix = ' '+(neg?'+':'-')+suffix;

        mdate = moment.utc(valdt);
        mdate = mdate.add(val.jsh_utcOffset*-1, 'minutes');
      }

      if('jsh_microseconds' in val){
        var ms_str = "000"+(Math.round(val.jsh_microseconds)).toString();
        ms_str = ms_str.slice(-3);
        suffix = ms_str.replace(/0+$/,'') + suffix;
      }
    }

    var rslt = '';
    if (dbtype.name == 'Date') rslt = "DATE('" + mdate.format('YYYY-MM-DD') + "')";
    else if (dbtype.name == 'Time') rslt = "TIME('" + mdate.format('HH.mm.ss') + suffix + "')";
    else rslt = "TIMESTAMP('" + mdate.format('YYYY-MM-DD HH:mm:ss.SSS') + suffix + "')";
    return rslt;
  }
  throw new Error('Invalid datatype: ' + JSON.stringify(dbtype));
};

var connectionId = 1;

DBdriver.prototype.ExecSession = function (dbtrans, dbconfig, session, callback, networkRetryCount) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  
  if (dbtrans) {
    session(null, dbtrans.con, [], callback);
  }
  else {
    var preStatements = [];
    var onConnect = function (err, con) {
      if (err) { return _this.ExecError(err, callback, "DB Connect Error: "); }
      if (!con.jshId) {
        con.jshId = connectionId++;
        preStatements = [];
        if(dbconfig && dbconfig._presql) preStatements = preStatements.concat(splitSQL(dbconfig._presql));
      }
      //console.log('odbc connect', con.jshId);
      session(null, con, preStatements, function (err) {
        var args = Array.prototype.slice.call(arguments);

        if (isNetworkError(err)) {
          if (_this.platform.Config.debug_params.db_error_sql_state && !_this.silent) _this.platform.Log('Network connection error, resetting database connections', { source: 'database' });

          return _this.Close(function() {
            networkRetryCount = networkRetryCount || 0;
            if (networkRetryCount < 3 && (err.statementIndex||0) == 0) {
              setTimeout(function() {_this.ExecSession(dbtrans, dbconfig, session, callback, networkRetryCount+1);});
            } else {
              callback.apply(null, args);
            }
          });
        } else {
          //console.log('odbc close', con.jshId);
          performance.mark('con.close start');
          con.close();
          performance.mark('con.close end');
          performance.measure('con.close', 'con.close start', 'con.close end');
          callback.apply(null, args);
        }
      });
    };
    initDBConfig(dbconfig);
    _this.togglePerformanceLogging();
    if(dbconfig.options.pooled){
      _this.getConnectionPool(dbconfig, function(err, pool) {
        if (err) return onConnect(err);
        performance.mark('pool.connect start');
        pool.connect(function(err, con) {
          performance.mark('pool.connect end');
          performance.measure('pool.connect', 'pool.connect start', 'pool.connect end');
          onConnect(err, con);
        });
      });
    }
    else {
      performance.mark('odbc.connect start');
      odbc.connect(dbconfig, function(err, con) {
        performance.mark('odbc.connect end');
        performance.measure('odbc.connect', 'odbc.connect start', 'odbc.connect end');
        onConnect(err, con);
      });
    }
  }
};

/*
https://www.ibm.com/docs/en/i/7.4?topic=reference-sql-messages-codes
https://www.ibm.com/docs/en/i/7.4?topic=application-db2-i-cli-sqlstate-values

    01, is a warning.
    HY, is generated by the CLI driver (either Db2 for i CLI or ODBC).

- If SQLCODE = 0 and SQLWARN0 is blank, execution was successful.
- If SQLCODE = 100, no data was found. For example, a FETCH statement
returned no data, because the cursor was positioned after the last row of the
result table.
- If SQLCODE > 0 and not = 100, execution was successful with a warning.
- If SQLCODE = 0 and SQLWARN0 = 'W', execution was successful with a
warning.
- If SQLCODE < 0, execution was not successful

[Error: [odbc] Error executing the sql statement] {
  odbcErrors: [
    {
      state: 'HY000',
      code: -104,
      message: '[IBM][System i Access ODBC Driver][DB2 for i5/OS]SQL0104 - Token <END-OF-STATEMENT> was not valid. Valid tokens: + - AS <IDENTIFIER>.'
    },
    {
      state: 'HY000',
      code: 69898,
      message: '[IBM][System i Access ODBC Driver][DB2 for i5/OS]PWS0005 - Error occurred in the database host server code.'
    }
  ]
}

Gracefull connection closed:
[Error: Communication link failure. comm rc=10054 - CWBCO1047 - The IBM i server application  disconnected the connection] {
    odbcErrors: [
      {
        state: '08S01',
        code: 10054,
        message: '[IBM][System i Access ODBC Driver]Communication link failure. comm rc=10054 - CWBCO1047 - The IBM i server application  disconnected the connection'
      }
    ]
  }

Server unreachable:
[Error: Communication link failure. comm rc=10060 - CWBCO1048 - A firewall blockage or time-out occurred trying to connect to the IBM i] {
  odbcErrors: [
    {
      state: '08S01',
      code: 10060,
      message: '[IBM][System i Access ODBC Driver]Communication link failure. comm rc=10060 - CWBCO1048 - A firewall blockage or time-out occurred trying to connect to the IBM i'
    }
  ]
}

Network cable pulled, two consecutive queries:
[Error: Communication link failure. comm rc=8413 - CWBCO1054 - A user-specified time-out occurred while sending or receiving data] {
  odbcErrors: [
    {
      state: '08S01',
      code: 8413,
      message: '[IBM][System i Access ODBC Driver]Communication link failure. comm rc=8413 - CWBCO1054 - A user-specified time-out occurred while sending or receiving data'
    }
  ]
}
[Error: Communication link failure. comm rc=11001 - CWBCO1004 - Remote address could not be resolved] {
  odbcErrors: [
    {
      state: '08S01',
      code: 11001,
      message: '[IBM][System i Access ODBC Driver]Communication link failure. comm rc=11001 - CWBCO1004 - Remote address could not be resolved'
    }
  ]

*/
var ibmErrorPrefix = /^(\[[^\]]+\])*/;
var networkErrorCodes = [
  8413,
  // https://www.ibm.com/docs/en/i/7.3?topic=codes-winsock-return
  10024, //   CWB_TOO_MANY_OPEN_SOCKETS
  10035, //   CWB_RESOURCE_TEMPORARILY_UNAVAILABLE
  10038, //   CWB_SOCKET_OPERATION_ON_NON_SOCKET
  10047, //   CWB_PROTOCOL_NOT_INSTALLED
  10050, //   CWB_NETWORK_IS_DOWN
  10051, //   CWB_NETWORK_IS_UNREACHABLE
  10052, //   CWB_NETWORK_DROPPED_CONNECTION_ON_RESET
  10053, //   CWB_SOFTWARE_CAUSED_CONNECTION_ABORT
  10054, //   CWB_CONNECTION_RESET_BY_PEER
  10055, //   CWB_NO_BUFFER_SPACE_AVAILABLE
  10057, //   CWB_SOCKET_IS_NOT_CONNECTED
  10058, //   CWB_CANNOT_SEND_AFTER_SOCKET_SHUTDOWN
  10060, //   CWB_CONNECTION_TIMED_OUT
  10061, //   CWB_CONNECTION_REFUSED
  10064, //   CWB_HOST_IS_DOWN
  10065, //   CWB_NO_ROUTE_TO_HOST
  10091, //   CWB_NETWORK_SUBSYSTEM_IS_UNAVAILABLE
  10092, //   CWB_WINSOCK_VERSION_NOT_SUPPORTED
  11001, //   CWB_HOST_DEFINITELY_NOT_FOUND
  11002, //   CWB_HOST_NOT_FOUND_BUT_WE_ARE_NOT_SURE
  11004, //   CWB_VALID_NAME_BUT_NO_DATA_RECORD
];

function isNetworkError(err) {
  if (!err) return false;
  var networkError = false;
  (err.odbcErrors || []).forEach(function(message) {
    if (networkErrorCodes.indexOf(message.code) != -1) networkError = true;
  });
  return networkError;
}

DBdriver.prototype.ExecError = function(err, callback, errprefix, sql) {
  if(err){
    var errMsg = err.message || '';
    var odbcErrors = [];
    (err.odbcErrors || []).forEach(function(message) {
      if (message.code == 69898) return;
      if (message.code == -438) {
        odbcErrors.push(message.message.replace(ibmErrorPrefix, '').replace('SQL0438 - ', ''));
        return;
      }
      odbcErrors.push('('+message.code+') '+message.message.replace(ibmErrorPrefix, ''));
    });
    if (odbcErrors.length > 0) errMsg += (errMsg ? ' :: ' : '') + odbcErrors.join(' :: ');
    err.message = errMsg;

  }
  if (this.platform.Config.debug_params.db_error_sql_state && !this.silent){
    this.platform.Log((errprefix || '') + err.message + (sql ? ':: ' + sql + '\n' : ''), { source: 'database' });
  }

  if (callback) return callback(err, null);
  else throw err;
};

DBdriver.prototype.ExecStatements = function(con, dbconfig, statements, callback) {
  var _this = this;
  var statementIndex = -1;
  async.mapSeries(statements, function(sql, cb) {
    statementIndex++;
    _this.ExecStatement(con, dbconfig, sql, cb);
  }, function(err, rslt) {
    if (err) err.statementIndex = statementIndex;
    var result = [];
    var previous;
    rslt.forEach(function(r) {
      if (r && r.xrowcount && previous) {
        return result.push([{xrowcount: previous.properties.count}]);
      }
      previous = r;
      if (r && r.rows) {
        return result.push(r.rows);
      }
    });
    callback(err, result);
  });
};

// response format:
/*
[
  { ONE: 1 },
  statement: 'SELECT 1 AS ONE FROM SYSIBM.SYSDUMMY1',
  parameters: [],
  return: undefined,
  count: 1,
  columns: [
    {
      name: 'ONE',
      dataType: 4,
      columnSize: 10,
      decimalDigits: 0,
      nullable: false
    }
  ]
]
*/
DBdriver.prototype.ExecStatement = function(con, dbconfig, sql, callback) {
  var _this = this;
  _this.logRawSQL(sql);
  if (sql == 'return_row_count()') {
    return setTimeout(callback, 0, null, {
      xrowcount: true,
      rows: null,
      properties: { count: 1 },
    });
  }
  performance.mark('con.query start');
  con.query(sql, {cursor: true, fetchSize: 100}, function(queryErr, cursor) {
    performance.mark('con.query end');
    performance.measure('con.query', 'con.query start', 'con.query end');

    if (queryErr) { return _this.ExecError(queryErr, callback, 'SQL Query Error: ', sql); }

    var allResults = [];
    async.doUntil(function iter(next) {
      performance.mark('cursor.fetch start');
      cursor.fetch(function(fetchErr, result) {
        performance.mark('cursor.fetch end');
        performance.measure('cursor.fetch', 'cursor.fetch start', 'cursor.fetch end');

        if (fetchErr) { return _this.ExecError(fetchErr, callback, 'SQL Fetch Error: ', sql); }
        allResults.push(result);
        next(fetchErr);
      });
    }, function test(cb) {
      if (cb) cb(null, cursor.noData); // most recent version of async
      else return cursor.noData; // our specified version of async
    }, function done(err) {
      var result = {rows: null, properties: null};
      if (allResults.length < 1) {
        result.rows = null;
        result.properties = null;
      } else {
        var first = allResults[0];
        result.properties = first;
        if (first.columns.length < 1) result.rows = null;
        else {
          result.rows = Array.prototype.concat.apply([], allResults);
        }
      }

      if (result.rows && dbconfig.options.autotrim) trimCharacterFields(result.rows, result.properties.columns);
      normalizeBigInt(result.rows, result.properties.columns);

      performance.mark('cursor.close start');
      cursor.close(function(closeErr) {
        performance.mark('cursor.close end');
        performance.measure('cursor.close', 'cursor.close start', 'cursor.close end');
        performance.measure('fetch total', 'con.query end', 'cursor.close end');
        //console.log(closeErr);
        // closeError might indicate a problem for a non-select statement with no rows to fetch, but we don't have a good way to predict this ahead of time, so we can't really use it
        callback(err, result);
      });
    });
  });
};

function splitSQL(fsql){
  var sql = [];
  while(fsql){
    var nexts = fsql.indexOf(';');
    while((nexts > 0) && (fsql[nexts-1]=="\\")) nexts = fsql.indexOf(';', nexts+1);
    if(nexts < 0){ sql.push(fsql.trim()); fsql = ''; }
    else if(nexts==0) fsql = fsql.substr(1);
    else{ sql.push(fsql.substr(0,nexts).trim()); fsql = fsql.substr(nexts+1); }
  }
  for(var i=0;i<sql.length;i++){
    var stmt = sql[i].trim();
    //Remove starting comments
    while((stmt.indexOf('/*')==0)||(stmt.indexOf('//')==0)||(stmt.indexOf('--')==0)){
      if((stmt.indexOf('//')==0)||(stmt.indexOf('--')==0)){
        var eolpos = stmt.indexOf('\n');
        if(eolpos >= 0) stmt = stmt.substr(eolpos+1);
        else stmt = '';
      }
      else if(stmt.indexOf('/*')==0){
        var eoc = stmt.indexOf('*/');
        if(eoc >= 0) stmt = stmt.substr(eoc+2);
        else stmt = '';
      }
      stmt = stmt.trim();
    }
    //Remove empty statements
    var is_empty = stmt.match(/^(\s)*$/);
    var is_comment = stmt.match(/^(\s)*\/\//);
    is_comment = is_comment || stmt.match(/^(\s)*--/);
    if(is_empty || is_comment){
      sql.splice(i,1);
      i--;
      continue;
    }
    stmt = DB.util.ReplaceAll(stmt, "\\;", ';');
    sql[i] = stmt;
  }
  return sql;
}

DBdriver._splitSQL = splitSQL;

DBdriver.prototype.Exec = function (dbtrans, context, return_type, sql, ptypes, params, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;

  var statements = [sql];

  _this.ExecSession(dbtrans, dbconfig, function (err, con, preStatements, conComplete) {
    if(dbtrans && (dbtrans.dbconfig != dbconfig)) err = new Error('Transaction cannot span multiple database connections');
    if(err) return conComplete(err);

    sql = _this.applySQLParams(sql, ptypes, params);

    if (return_type == 'command' && dbconfig.options.automatic_compound_commands) {
      sql = DB.util.ReplaceAll(sql, "\\;", ';');
      statements = [ 'BEGIN ' + [].concat(preStatements, _this.getContextStatements(context, con), sql).join('; ') + (sql.endsWith(';') ? '' : ';') + ' END' ];
    } else {
      statements = splitSQL(sql);
      statements = [].concat(preStatements, _this.getContextStatements(context, con), statements);
    }

    //console.log(params);
    //console.log(ptypes);

    //Execute sql
    _this.ExecStatements(con, dbconfig, statements, conComplete);

  }, function(err, rslt) {
    if(err) {
      if (callback != null) return callback(err, null);
      else throw err;
    }

    var dbrslt = null;

    //console.log(return_type, typeof(rslt), rslt);

    if (return_type == 'row') { if (rslt[0] && rslt[0].length) dbrslt = rslt[0][0]; }
    else if (return_type == 'recordset') dbrslt = rslt[0];
    else if (return_type == 'multirecordset') dbrslt = rslt;
    else if (return_type == 'scalar') {
      if (rslt[0] && rslt[0].length) {
        var row = rslt[0][0];
        for (var key in row) {
          if (Object.prototype.hasOwnProperty.call(row, key)) dbrslt = row[key];
        }
      }
    }
    var notices = [];
    var warnings = [];
    DB.util.LogDBResult(_this.platform, { sql: sql, dbrslt: dbrslt, notices: notices, warnings: warnings });
    if (callback) callback(null, dbrslt, { notices: notices, warnings: warnings });
  });
};

DBdriver.prototype.ExecTransTasks = function (execTasks, callback, dbconfig) {
  if(!dbconfig) throw new Error('dbconfig is required');
  var _this = this;
  _this.ExecSession(null, dbconfig, function (err, con, preStatements, conComplete) {
    if(err) return conComplete(err, null);
    var trans = new DB.TransactionConnection(con,dbconfig);
    trans.con.beginTransaction(function (err) {
      if (err) return conComplete(err, null);
      var exitTrans = function (dberr, rslt) {
        if (dberr != null) { trans.con.rollback(function (err) { conComplete(dberr, null); }); }
        else { trans.con.commit(function (err) { conComplete(err, rslt); }); }
      };
      _this.ExecStatements(con, dbconfig, preStatements, function(preerr, _) {
        if (preerr) return exitTrans(preerr, null);
        execTasks(trans, exitTrans);
      });
    });
  }, callback);
};

DBdriver.prototype.escape = function(val){ return this.sql.escape(val); };

DBdriver.prototype.getContextStatements = function(context, con) {
  if(!context) return [];

  var _this = this;

  var statements = [
    "UPDATE SESSION.JSHARMONY_META SET CONTEXT = '"+this.escape(context)+"'",
  ];
  if (!con.hasMeta) {
    con.hasMeta = true;
    statements = _this.initStatements.concat(statements);
  }

  return statements;
};

DBdriver.prototype.applySQLParams = function (sql, ptypes, params) {
  var _this = this;

  //Apply ptypes, params to SQL
  var ptypes_ref = {};
  if(ptypes){
    var i = 0;
    for (var p in params) {
      ptypes_ref[p] = ptypes[i];
      i++;
    }
  }
  //Sort params by length
  var param_keys = _.keys(params);
  param_keys.sort(function (a, b) { return b.length - a.length; });
  //Replace params in SQL statement
  for (let i = 0; i < param_keys.length; i++) {
    let p = param_keys[i];
    var val = params[p];
    if (val === '') val = null;
    sql = DB.util.ReplaceAll(sql, '@' + p, _this.getDBParam(ptypes ? ptypes_ref[p] : types.fromValue(val), val));
  }
  return sql;
};

function trimCharacterFields(rows, columns) {
  _.forEach(columns, function(col) {
    if (col.dataType == odbc.SQL_CHAR || col.dataType == odbc.SQL_VARCHAR || col.dataType == odbc.CLOB || col.dataType == odbc.SQL_NCHAR || col.dataType == odbc.SQL_NVARCHAR || col.dataType == odbc.SQL_NCLOB) {
      _.forEach(rows, function(row) {
        if (row[col.name])  row[col.name] = row[col.name].trimEnd();
      });
    }
  });
}

function normalizeBigInt(rows, columns) {
  _.forEach(columns, function(col) {
    var SQL_BIGINT = -5;
    if (col.dataType == SQL_BIGINT) {
      _.forEach(rows, function(row) {
        if ((typeof row[col.name] != 'undefined') && (row[col.name] !== null))  row[col.name] = Number(row[col.name]);
      });
    }
  });
}

DBdriver.trimCharacterFields = trimCharacterFields;

exports = module.exports = DBdriver;
