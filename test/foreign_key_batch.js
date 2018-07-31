const dbmeta = require('db-meta')
const dataType = require('db-migrate-shared').dataType
const fs = require('fs')
const assert = require('assert')

module.exports = (driver, config, internals) => ({
  'columnForeignKeySpec': {
    topic: function () {
      driver.connect(config, internals, function (err, db) {
        db.createTable('event_type', {

          id: { type: dataType.INTEGER, primaryKey: true, autoIncrement: true },
          title: { type: dataType.STRING }
        }, function(err) {
          if (err) {
              return this.callback(err);
            }
          db.createTable('event', {
            id: {
              type: dataType.INTEGER,
              primaryKey: true,
              autoIncrement: true
            },
            event_type_id: {
              type: dataType.INTEGER,
              notNull: true,
              foreignKey: {
                name: 'fk_event_event_type',
                table: 'event_type',
                mapping: 'id',
                rules: {
                  onDelete: 'CASCADE'
                },
              } },
            title: {
              type: dataType.STRING
            }
          }, this.callback.bind(this, null, db));
        }.bind(this));
      }.bind(this))
     },

    teardown: function(db) {
        db.close(function (err) {
          fs.unlink(config.filename, this.callback);
        }.bind(this));
    },

    'sets usage and constraints': {
      topic: function (db) {
        dbmeta('sqlite3', {connection: db.connection}, function (err, meta) {
          if (err) {
            return this.callback(err);
          }
          meta.getTables( (err, tables) => {
            if (err) {
              return this.callback(err)
            }
            this.callback( undefined, tables.find( (table) => table.getName() == "event" ) )
          })
        }.bind(this));
      },
      'that has foreign key column with the expected reference': function (err, table) {
        const foreignKeyDefinition = table.meta
          .sql
          .match(/"event_type_id"[^,]+/)[0]
          .replace(/\s{2,}/g,' ')
        
        assert.equal(
          foreignKeyDefinition,
          '"event_type_id" INTEGER NOT NULL REFERENCES event_type(id) ON DELETE CASCADE'
        )
      }

    }
  }
})
