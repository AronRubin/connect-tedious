/*!
 * Connect - Tedious
 * MIT Licensed
 */

/**
 * Module dependencies.
 */
var tedious = require('tedious');
var retry = require('retry');
var debug = {
  sql: require('debug')('connect-tedious:sql')
};

/**
 * One day in seconds.
 */
var oneDay = 86400;

function debugSql(req) {
  if (!req || !debug.sql.enabled) {
    return;
  }

  debug.sql( 'Executing '+ req.sqlTextOrProcedure );

  var param_i, num_params;
  for (param_i = 0, num_params = req.parameters.length; param_i<num_params; param_i++ ) {
    var param = req.parameters[param_i];
    debug.sql( '@%s: ', param.name, param.value );
  }
}

/**
 * Return the `TediousStore` extending `connect`'s session Store.
 *
 * @param {object} connect
 * @return {Function}
 * @api public
 */
module.exports = function(connect) {

    /**
     * Connect's Store.
     */
  var Store = connect.Store || connect.session.Store;

    /**
     * Initialize TediousStore with the given `options`.
     *
     * @param {Object} options
     * @param {String} connection
     * @api public
     */
  function TediousStore( options, connection ) {
    this.dbconnection = connection;

    options = options || {};
    options.config = options.config || {};
    options.config.options = options.config.options || {};

    Store.call( this, options );

    this.tableName = options.tableName || '[dbo].[Session]';
    this.sidColumnName = options.sidColumnName || '[sid]';
    this.sessColumnName = options.sessionColumnName || '[session]';
    this.expiresColumnName = options.expiresColumnName || '[expires]';
        
    this.retryOptions = {
      retries: 3,
      minTimeout: 50,
      maxTimeout: 1000
    };
  }

    /**
     * Inherit from `Store`.
     */
  TediousStore.prototype.__proto__ = Store.prototype;

    /**
     * Attempt to fetch session by the given `sid`.
     *
     * @param {String} sid
     * @param {Function} fn
     * @api public
     */
  TediousStore.prototype.get = function( sid, fn ) {
    var self = this;

    var doGet = function (err) {
      if( err ) {
        return fn(err);
      }
      var operation = retry.operation(self.retryOptions);
      operation.attempt(function () {
        var req = new tedious.Request(
          'SELECT s.' + self.expiresColumnName + ', s.' + self.sessColumnName + ' FROM ' + self.tableName + ' s WHERE s.' + self.sidColumnName + '=@sid AND s.' + self.expiresColumnName + '>=SYSUTCDATETIME()',
          function (err, rowCount) {
            debug.sql('Executed SELECT');
            //self.dbconnection.release();
            if (operation.retry(err) || err) {
              return fn(err);
            }
            if (!rowCount || rowCount !== 1) {
              return fn();
            }
          }
        );
        req.on('row', function (columns) {
          if (!columns || columns.length !== 2) {
            return fn();
          }

          var expires = columns[0].value;
          var sessionData = columns[1].value;

          if (!expires || !sessionData) {
            return fn();
          }

          var dExpires = new Date(expires).toISOString();
          var oSess = JSON.parse(sessionData);
          oSess.cookie.expires = dExpires;

          debug.sql('Returning ', oSess);
          return fn(null, oSess);
        });
        req.addParameter('sid', tedious.TYPES.VarChar, sid);

        debugSql(req);
        self.dbconnection.execSql(req);
      });
    };
  
    if( self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN ) {
      self.dbconnection.on( 'connect', doGet );
    } else {
      doGet();
    }
  };

    /**
     * Commit the given `sess` object associated with the given `sid`.
     *
     * @param {String} sid
     * @param {Session} sess
     * @param {Function} fn
     * @api public
     */
  TediousStore.prototype.set = function( sid, sess, fn ) {
    var self = this;

    var doOp = function (err) {
      if (err) {
        return fn(err);
      }
      var operation = retry.operation(self.retryOptions);
      operation.attempt(function () {

        var duration = sess.cookie.maxAge || oneDay;
        var req = new tedious.Request(
          'MERGE INTO ' + self.tableName + ' WITH (HOLDLOCK) s' +
          '  USING (VALUES(@sid, @sess)) ns(' + self.sidColumnName + ', ' + self.sessColumnName + ') ON (s.' + self.sidColumnName + '=ns.' + self.sidColumnName + ')' +
          '  WHEN MATCHED THEN UPDATE SET s.' + self.sessColumnName + '=@sess, s.' + self.expiresColumnName + '=DATEADD(ms, @duration, SYSUTCDATETIME())' +
          '  WHEN NOT MATCHED THEN INSERT (' + self.sidColumnName + ', ' + self.sessColumnName + ', ' + self.expiresColumnName + ') VALUES (@sid, @sess, DATEADD(ms, @duration, SYSUTCDATETIME()));',
          function (err) {
            debug.sql('Executed MERGE');
            //self.dbconnection.release();
            if (operation.retry(err) || err) {
              return fn(err);
            }
            fn.apply(self, arguments);
          }
        );
        req.addParameter('sid', tedious.TYPES.VarChar, sid);
        req.addParameter('sess', tedious.TYPES.NVarChar, JSON.stringify(sess));
        req.addParameter('duration', tedious.TYPES.Int, duration);

        debugSql(req);
        self.dbconnection.execSql(req);
      });
    };
    if (self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN) {
      self.dbconnection.on('connect', doOp);
    } else {
      doOp();
    }

  };

    /**
     * Destroy the session associated with the given `sid`.
     *
     * @param {String} sid
     * @api public
     */
  TediousStore.prototype.destroy = function(sid, fn) {
    var self = this;

    var doOp = function (err) {
      if (err) {
        return fn(err);
      }
      var operation = retry.operation(self.retryOptions);
      operation.attempt(function () {

        var req = new tedious.Request(
          'DELETE s FROM ' + self.tableName + ' s WHERE s.' + self.sidColumnName + '=@sid',
          function (err) {
            debug.sql('Executed DELETE');
            //self.dbconnection.release();
            if (operation.retry(err) || err) {
              return fn(err);
            }
            return fn(null, true);
          }
        );
        req.addParameter('sid', tedious.TYPES.VarChar, sid);

        debugSql(req);
        self.dbconnection.execSql(req);
      });
    };
    if (self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN) {
      self.dbconnection.on('connect', doOp);
    } else {
      doOp();
    }
  };

    /**
     * Fetch number of sessions.
     *
     * @param {Function} fn
     * @api public
     */
  TediousStore.prototype.length = function(fn) {
    var self=this;

    var doOp = function (err) {
      if (err) {
        return fn(err);
      }
      var operation = retry.operation(self.retryOptions);
      operation.attempt(function () {

        var req = new tedious.Request(
          'SELECT @count=COUNT(*) FROM ' + self.tableName,
          function (err, rowCount) {
            debug.sql('Executed SELECT');
            //self.dbconnection.release();
            if (operation.retry(err) || err) {
              return fn(err);
            }
            if (!rowCount || rowCount !== 1)
              return fn();
          }
        );
        req.on('returnValue', function (parameterName, value) {
          if (!value) {
            return fn();
          }
          return fn(null, value);
        });
        req.addOutputParameter('count', tedious.TYPES.Int);

        debugSql(req);
        self.dbconnection.execSql(req);
      });
    };
    if (self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN) {
      self.dbconnection.on('connect', doOp);
    } else {
      doOp();
    }

  };


    /**
     * Clear all sessions.
     *
     * @param {Function} fn
     * @api public
     */
  TediousStore.prototype.clear = function(fn) {
    var self = this;

    var doOp = function (err) {
      if (err) {
        return fn(err);
      }
      var operation = retry.operation(self.retryOptions);
      operation.attempt(function () {
        var req = new tedious.Request(
          'TRUNCATE TABLE ' + self.tableName,
          function (err) {
            debug.sql('Executed TRUNCATE');
            //self.dbconnection.release();
            if (operation.retry(err) || err) {
              return fn(err);
            }
            fn(null, true);
          }
        );

        debugSql(req);
        self.dbconnection.execSql(req);
      });
    };
    if (self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN) {
      self.dbconnection.on('connect', doOp);
    } else {
      doOp();
    }

  };


    /**
     * Update expiration date of the given `sid`.
     *
     * @param {String} sid
     * @param {Object} sess
     * @param {Function} fn
     * @api public
     */
  TediousStore.prototype.touch = function (sid, sess, fn) {
    var self = this;

    var doOp = function (err) {
      if (err) {
        return fn(err);
      }
      var operation = retry.operation(self.retryOptions);
      operation.attempt(function () {
        var duration = sess.cookie.maxAge || oneDay;

        var req = new tedious.Request(
          'UPDATE ' + self.tableName + ' SET ' + self.expiresColumnName + '=DATEADD(ms, @duration, SYSUTCDATETIME()) WHERE ' + self.sidColumnName + '=@sid',
          function (err) {
            debug.sql('Executed UPDATE');
            //self.dbconnection.release();
            if (operation.retry(err) || err) {
              return fn(err);
            }
            fn(null, true);
          }
        );
        req.addParameter('duration', tedious.TYPES.Int, duration);
        req.addParameter('sid', tedious.TYPES.VarChar, sid);

        debugSql(req);
        self.dbconnection.execSql(req);
      });
    };
    if (self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN) {
      self.dbconnection.on('connect', doOp);
    } else {
      doOp();
    }

  };

  /**
     * Remove expired sessions from database.
    * @param {Object} store
    * @api private
    */

  TediousStore.prototype.dbCleanup = function(store, fn) {
    var self = this;
    var doOp = function (err) {
      if (err) {
        return fn(err);
      }

      var req = new tedious.Request(
        'DELETE FROM ' + store.tableName + ' WHERE ' + store.expiresColumnName + '<SYSUTCDATETIME()',
        function (err) {
          debug.sql('Executed DELETE');
          //this.dbconnection.release();
          if (err)
            return fn(err);
          fn(null, true);
        }
      );

      debugSql(req);
      this.dbconnection.execSql(req);
    };
    if (self.dbconnection.state !== self.dbconnection.STATE.LOGGED_IN) {
      self.dbconnection.on('connect', doOp);
    } else {
      doOp();
    }

  };

  return TediousStore;
};


