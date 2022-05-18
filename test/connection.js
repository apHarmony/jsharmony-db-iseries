/*
Copyright 2022 apHarmony

This file is part of jsHarmony.

jsHarmony is free software: you can redistribute it and/or modify
it.skip under the terms of the GNU Lesser General Public License as published by
the Free Software Foundation, either version 3 of the License, or
(at your option) any later version.

jsHarmony is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
GNU Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public License
along with this package.  If not, see <http://www.gnu.org/licenses/>.
*/

var JSHiseries = require('../index');
var JSHdb = require('jsharmony-db');
var types = JSHdb.types;
var _ = require('lodash');
//var moment = require('moment');
var dbconfig = require('./dbconfig');
var async = require('async');
var readline = require('readline');


dbconfig = _.extend({_driver: new JSHiseries(), connectionString: "DSN=ODBC;Uid=DBUSER;pwd=DBPASS", initialSize: 1, options: {pooled: true, automatic_compound_commands: true} }, dbconfig);
var db = new JSHdb(dbconfig);
dbconfig._driver.platform.Config.debug_params.db_error_sql_state = true;
dbconfig._driver.platform.Config.debug_params.db_raw_sql = true;

async.waterfall([

  function(cb){
    console.log('query 1');
    db.Scalar('','select 1 from sysibm.sysdummy1',[],{},function(err,rslt){
      console.log(err);
      return cb();
    });
  },
  pressEnter,
  function(cb){
    console.log('query 2');
    db.Scalar('','select 1 from sysibm.sysdummy1',[],{},function(err,rslt){
      console.log(err);
      return cb();
    });
  },
  function(cb){
    console.log('query 3');
    db.Scalar('','select 1 from sysibm.sysdummy1',[],{},function(err,rslt){
      console.log(err);
      return cb();
    });
  },

  //Close Database Connection
  function(cb){
    db.Close(cb);
  },
], function(err){
  if(err) console.log(err);
  console.log('Done');
});

function pressEnter(cb){
  var rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout
  });

  rl.question('press enter', function() {
    rl.close();
    cb();
  });
}
