var path = require('path');
var moment = require('moment');
var sprintf = require('sprintf-js').sprintf;
var mkdirp = require('mkdirp');
var Promise = require('bluebird');
var glob = require('glob');

module.exports = function(shipit) {
  shipit.db = shipit.db || {};

  shipit.db.createDirs = async function createDirs() {
    await mkdirp(shipit.db.localDumpDir);
    await mkdirp(shipit.db.localDumpDir + '/local');
    await mkdirp(shipit.db.localDumpDir + '/remote');
    await shipit.remote('mkdir -p ' + shipit.db.remoteDumpDir);
    await shipit.remote('mkdir -p ' + shipit.db.remoteDumpDir + '/local');
    await shipit.remote('mkdir -p ' + shipit.db.remoteDumpDir + '/remote');
    return;
  };

  shipit.db.dumpFile = function dumpFile(environment) {
    let dbFiles = glob.sync(
      path.join(
        shipit.config.db.dumpDir,
        'local',
        '*.sql.bz2'
      )
    );

    if(dbFiles[0]) {
      let file = dbFiles[0];
      return path.parse(file).name;
    }

    return path.join(
      sprintf('%(database)s-%(currentTime)s.sql.bz2', {
        database: shipit.config.db[environment].database,
        currentTime: moment.utc().format('YYYYMMDDHHmmss'),
      })
    );
  };

  shipit.db.remoteDumpFile = function remoteDumpFile(environment, file) {
    return path.join(
      shipit.config.db.dumpDir,
      environment === 'remote' ? 'local' : 'remote',
      file
    );
  }

  shipit.db.localDumpFile = function localDumpFile(environment, file) {
    return path.join(
      shipit.config.db.dumpDir,
      environment === 'local' ? 'local' : 'remote',
      file
    );
  }

  shipit.db.credentialParams = function credentialParams(dbConfig) {
    var params = {
      '-u': dbConfig.username || null,
      '-p': dbConfig.password || null,
      '-h': dbConfig.host || null,
      '-S': dbConfig.socket || null,
      '-P': dbConfig.port || null,
    };

    var paramStr = Object.keys(params).map(function(key) {
      return (params[key]) ? key + '\'' + params[key] + '\'' : false;
    }).filter(function(elem) {
      return !!elem;
    });

    return paramStr.join(' ');
  };

  shipit.db.ignoreTablesArgs = function ignoreTablesArgs(environment) {

    // TODO: ignoreTables should be per-env
    var args = shipit.config.db.ignoreTables.map(function(table) {
      table = table.match(/\./) ? table : [shipit.config.db[environment].database, table].join('.');

      return '--ignore-table=' + table;
    });

    return args.join(' ');
  };

  shipit.db.dumpCmd = function dumpCmd(environment) {
    return sprintf('mysqldump %(credentials)s %(database)s --single-transaction --lock-tables=false --quick --no-tablespaces %(ignoreTablesArgs)s', {
      credentials: shipit.db.credentialParams(shipit.config.db[environment]),
      database: shipit.config.db[environment].database,
      ignoreTablesArgs: shipit.db.ignoreTablesArgs(environment)
    });
  };

  shipit.db.importCmd = function importCmd(environment, file) {
    return sprintf('mysql %(credentials)s -D %(database)s', {
      credentials: shipit.db.credentialParams(shipit.config.db[environment]),
      database: shipit.config.db[environment].database,
      file: path.join(path.dirname(file), path.basename(file, '.bz2')),
    });
  };

  shipit.db.createCmd = function createCmd(environment) {
    return sprintf('mysql %(credentials)s --execute \"CREATE DATABASE IF NOT EXISTS %(database)s;\"', {
      credentials: shipit.db.credentialParams(shipit.config.db[environment]),
      database: shipit.config.db[environment].database,
    });
  };

  shipit.db.unzipCmd = function(file) {
    if (shipit.config.db.shell.unzip) {
      return shipit.config.db.shell.unzip.call(shipit, file);
    }

    return sprintf('bunzip2 -k -f -c %(file)s', {
      file: file,
    });
  };

  shipit.db.zipCmd = function(file) {
    if (shipit.config.db.shell.zip) {
      return shipit.config.db.shell.zip.call(shipit, file);
    }

    return shipit.config.db.shell.zip || sprintf('bzip2 - - > %(dumpFile)s', {
      dumpFile: file,
    });
  };

  shipit.db.dump = function dump(environment, file) {
    if (shipit.config.db.shell.dump) {
      return shipit.config.db.shell.dump.call(shipit, environment, file);
    }

    return shipit[environment](
      sprintf("%(dumpCmd)s | sed -e 's/DEFINER[ ]*=[ ]*[^*]*\\*/\\*/' | %(zipCmd)s", {
        dumpCmd: shipit.db.dumpCmd(environment),
        zipCmd: shipit.db.zipCmd(file),
      })
    );
  };

  shipit.db.load = function load(environment, file) {
    if (shipit.config.db.shell.load) {
      return shipit.config.db.shell.load.call(shipit, environment, file);
    }

    var cmd = sprintf('%(createCmd)s && %(unzipCmd)s | %(importCmd)s', {
      unzipCmd: shipit.db.unzipCmd(file),
      importCmd: shipit.db.importCmd(environment, file),
      createCmd: shipit.db.createCmd(environment)
    });

    return shipit[environment](cmd);
  };

  shipit.db.clean = function clean(environment, path, enabled) {
    if (shipit.config.db.shell.clean) {
      return shipit.config.db.shell.clean.call(shipit, environment, path, enabled);
    }

    return enabled ? shipit[environment]('rm -f ' + path) : Promise.resolve();
  };

  return shipit;
};
