var fs = require('fs');
const Promise = require('bluebird');
var unlink = Promise.promisify(fs.unlink);
var vows = require('vows');
var assert = require('assert');
var dbmeta = require('db-meta');
var dataType = require('db-migrate-shared').dataType;
var log = require('db-migrate-shared').log;
var driver = require('../');

var config = require('./db.config.json').sqlite3;
var db;

var internals = {};
internals.mod = {
  log: log,
  type: dataType
};
internals.interfaces = {
  SeederInterface: {},
  MigratorInterface: {}
};
internals.migrationTable = 'migrations';

vows
  .describe('sqlite3')
  .addBatch({
    createTable: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true,
                  notNull: true
                },
                str: { type: dataType.STRING, unique: true },
                txt: {
                  type: dataType.TEXT,
                  notNull: true,
                  defaultValue: 'foo'
                },
                intg: dataType.INTEGER,
                rel: dataType.REAL,
                dt: dataType.DATE_TIME,
                bl: dataType.BOOLEAN,
                raw: {
                  type: dataType.DATE_TIME,
                  defaultValue: {
                    raw: 'CURRENT_TIMESTAMP'
                  }
                },
                special: {
                  type: dataType.DATE_TIME,
                  defaultValue: {
                    special: 'CURRENT_TIMESTAMP'
                  }
                }
              },
              this.callback.bind(this, null, db)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has resulting table metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getTables(this.callback);
            }.bind(this)
          );
        },

        'containing the event table': function (err, tables) {
          assert.isNull(err);
          var table = findByName(tables, 'event');
          assert.isNotNull(table);
          assert.strictEqual(table.getName(), 'event');
        }
      },

      'has column metadata for the event table': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getColumns('event', this.callback);
            }.bind(this)
          );
        },

        'with 9 columns': function (err, columns) {
          assert.isNotNull(columns);
          assert.isNull(err);
          assert.strictEqual(columns.length, 9);
        },

        'that has integer id column that is primary key, non-nullable, and auto increments': function (
          err,
          columns
        ) {
          var column = findByName(columns, 'id');
          assert.isNull(err);
          assert.strictEqual(column.getDataType(), 'INTEGER');
          assert.strictEqual(column.isPrimaryKey(), true);
          assert.strictEqual(column.isNullable(), false);
          assert.strictEqual(column.isAutoIncrementing(), true);
        },

        'that has text str column that is unique': function (err, columns) {
          var column = findByName(columns, 'str');
          assert.isNull(err);
          assert.strictEqual(column.getDataType(), 'VARCHAR');
          assert.strictEqual(column.isUnique(), true);
        },

        'that has text txt column that is non-nullable': function (
          err,
          columns
        ) {
          var column = findByName(columns, 'txt');
          assert.isNull(err);
          assert.strictEqual(column.getDataType(), 'TEXT');
          assert.strictEqual(column.isNullable(), false);
          //        assert.strictEqual(column.getDefaultValue(), 'foo');
        },

        'that has integer intg column': function (err, columns) {
          var column = findByName(columns, 'intg');
          assert.isNull(err);
          assert.strictEqual(column.getDataType(), 'INTEGER');
          assert.strictEqual(column.isNullable(), true);
        },

        'that has real rel column': function (err, columns) {
          var column = findByName(columns, 'rel');
          assert.isNull(err);
          assert.strictEqual(column.getDataType(), 'REAL');
          assert.strictEqual(column.isNullable(), true);
        },

        'that has integer dt column': function (err, columns) {
          var column = findByName(columns, 'dt');
          assert.isNull(err);
          assert.strictEqual(column.getDataType(), 'DATETIME');
          assert.strictEqual(column.isNullable(), true);
        },

        'that has boolean bl column': function (err, columns) {
          assert.isNull(err);
          var column = findByName(columns, 'bl');
          assert.strictEqual(column.getDataType(), 'BOOLEAN');
          assert.strictEqual(column.isNullable(), true);
        },

        'that has raw column': function (err, columns) {
          assert.isNull(err);
          var column = findByName(columns, 'raw');
          assert.strictEqual(column.getDefaultValue(), 'CURRENT_TIMESTAMP');
        },

        'that has special CURRENT_TIMESTAMP column': function (err, columns) {
          assert.isNull(err);
          var column = findByName(columns, 'special');
          assert.strictEqual(column.getDefaultValue(), 'CURRENT_TIMESTAMP');
        }
      }
    }
  })
  .addBatch({
    dropTable: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true
                }
              },
              function (err) {
                if (err) {
                  return this.callback(err);
                }
                db.dropTable('event', this.callback.bind(this, null, db));
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has table metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getTables(this.callback);
            }.bind(this)
          );
        },

        'containing no tables': function (err, tables) {
          assert.isNull(err);
          assert.isNotNull(tables);
          assert.strictEqual(tables.length, 1);
        }
      }
    }
  })
  .addBatch({
    renameTable: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true
                }
              },
              function () {
                db.renameTable(
                  'event',
                  'functions',
                  this.callback.bind(this, null, db)
                );
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has table metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getTables(this.callback);
            }.bind(this)
          );
        },

        'containing the functions table': function (err, tables) {
          assert.isNull(err);
          assert.isNotNull(tables);
          var table = findByName(tables, 'functions');
          assert.strictEqual(table.getName(), 'functions');
          assert.isNull(findByName(tables, 'event'));
        }
      }
    }
  })
  .addBatch({
    addColumn: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true
                }
              },
              function () {
                db.addColumn(
                  'event',
                  'title',
                  'string',
                  this.callback.bind(this, null, db)
                );
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has column metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getColumns('event', this.callback);
            }.bind(this)
          );
        },

        'with additional title column': function (err, columns) {
          assert.isNull(err);
          assert.isNotNull(columns);
          assert.strictEqual(columns.length, 2);
          var column = findByName(columns, 'title');
          assert.strictEqual(column.getName(), 'title');
          assert.strictEqual(column.getDataType(), 'VARCHAR');
        }
      }
    }
    // removeColumn
    // renameColumn
    // changeColumn
  })
  .addBatch({
    addIndex: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true
                },
                title: { type: dataType.STRING }
              },
              function () {
                db.addIndex(
                  'event',
                  'event_title',
                  'title',
                  this.callback.bind(this, null, db)
                );
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has resulting index metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getIndexes('event', this.callback);
            }.bind(this)
          );
        },

        'with additional index': function (err, indexes) {
          assert.isNull(err);
          assert.isNotNull(indexes);
          var index = findByName(indexes, 'event_title');
          assert.strictEqual(index.getName(), 'event_title');
          assert.strictEqual(index.getTableName(), 'event');
          assert.strictEqual(index.getColumnName(), 'title');
        }
      }
    }
  })
  .addBatch({
    insert: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, _db) {
            assert.isNull(err);
            db = _db;
            db.createTable('event', {
              id: {
                type: dataType.INTEGER,
                primaryKey: true,
                autoIncrement: true
              },
              title: { type: dataType.STRING }
            })
              .then(function () {
                return db.insert('event', ['id', 'title'], [2, 'title']);
              })
              .then(function () {
                return db.all('SELECT * from event;');
              })
              .then(function (data) {
                return data;
              })
              .nodeify(this.callback);
          }.bind(this)
        );
      },

      teardown: function () {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'with additional row': function (err, data) {
        assert.isNull(err);
        assert.strictEqual(data.length, 1);
      }
    }
  })
  .addBatch({
    insertWithSingleQuotes: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, _db) {
            assert.isNull(err);
            db = _db;
            db.createTable('event', {
              id: {
                type: dataType.INTEGER,
                primaryKey: true,
                autoIncrement: true
              },
              title: { type: dataType.STRING }
            })
              .then(function () {
                return db.insert(
                  'event',
                  ['id', 'title'],
                  [2, "Bill's Mother's House"]
                );
              })
              .then(function () {
                return db.all('SELECT * from event;');
              })
              .nodeify(this.callback);
          }.bind(this)
        );
      },

      teardown: function () {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'with additional row': function (err, data) {
        assert.isNull(err);
        assert.strictEqual(data.length, 1);
      }
    }
  })
  .addBatch({
    removeIndex: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true
                },
                title: { type: dataType.STRING }
              },
              function (err) {
                assert.isNull(err);
                db.addIndex(
                  'event',
                  'event_title',
                  'title',
                  function (err) {
                    assert.isNull(err);
                    db.removeIndex(
                      'event_title',
                      this.callback.bind(this, null, db)
                    );
                  }.bind(this)
                );
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has resulting index metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getIndexes('event', this.callback);
            }.bind(this)
          );
        },

        'without index': function (err, indexes) {
          assert.isNull(err);
          assert.isNotNull(indexes);
          assert.strictEqual(indexes.length, 0);
        }
      }
    }
  })
  .addBatch({
    removeIndexWithTableName: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createTable(
              'event',
              {
                id: {
                  type: dataType.INTEGER,
                  primaryKey: true,
                  autoIncrement: true
                },
                title: { type: dataType.STRING }
              },
              function (err) {
                assert.isNull(err);
                db.addIndex(
                  'event',
                  'event_title',
                  'title',
                  function (err) {
                    assert.isNull(err);
                    db.removeIndex(
                      'event',
                      'event_title',
                      this.callback.bind(this, null, db)
                    );
                  }.bind(this)
                );
              }.bind(this)
            );
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has resulting index metadata': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getIndexes('event', this.callback);
            }.bind(this)
          );
        },

        'without index': function (err, indexes) {
          assert.isNull(err);
          assert.isNotNull(indexes);
          assert.strictEqual(indexes.length, 0);
        }
      }
    }
  })
  .addBatch({
    createMigrationsTable: {
      topic: function () {
        driver.connect(
          config,
          internals,
          function (err, db) {
            assert.isNull(err);
            db.createMigrationsTable(this.callback.bind(this, null, db));
          }.bind(this)
        );
      },

      teardown: function (db) {
        db.close()
          .then(function () {
            return unlink(config.filename);
          })
          .nodeify(this.callback);
      },

      'has migrations table': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getTables(this.callback.bind(this));
            }.bind(this)
          );
        },

        'has migrations table': function (err, tables) {
          assert.isNull(err);
          assert.isNotNull(tables);
          assert.strictEqual(tables.length, 2);
          assert.strictEqual(tables[0].getName(), 'migrations');
        }
      },

      'that has columns': {
        topic: function (db) {
          dbmeta(
            'sqlite3',
            { connection: db.connection },
            function (err, meta) {
              if (err) {
                return this.callback(err);
              }
              meta.getColumns('migrations', this.callback);
            }.bind(this)
          );
        },

        'with names': function (err, columns) {
          assert.isNull(err);
          assert.isNotNull(columns);
          assert.strictEqual(columns.length, 3);
          var column = findByName(columns, 'id');
          assert.strictEqual(column.getName(), 'id');
          assert.strictEqual(column.getDataType(), 'INTEGER');
          column = findByName(columns, 'name');
          assert.strictEqual(column.getName(), 'name');
          assert.strictEqual(column.getDataType(), 'VARCHAR (255)');
          column = findByName(columns, 'run_on');
          assert.strictEqual(column.getName(), 'run_on');
          assert.strictEqual(column.getDataType(), 'DATETIME');
        }
      }
    }
  })
  .export(module);

function findByName (columns, name) {
  for (var i = 0; i < columns.length; i++) {
    if (columns[i].getName() === name) {
      return columns[i];
    }
  }
  return null;
}
