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
    return sprintf('mysqldump %(credentials)s %(database)s %(charSet)s --single-transaction --lock-tables=false --quick --no-tablespaces %(ignoreTablesArgs)s', {
      credentials: shipit.db.credentialParams(shipit.config.db[environment]),
      database: shipit.config.db[environment].database,
      ignoreTablesArgs: shipit.db.ignoreTablesArgs(environment),
      charSet: shipit.config.db.charSet ? `--default-character-set=${shipit.config.db.charSet}`: ''
    });
  };

  shipit.db.importCmd = function importCmd(environment, file) {
    return sprintf('mysql %(credentials)s %(charSet)s -D %(database)s', {
      credentials: shipit.db.credentialParams(shipit.config.db[environment]),
      database: shipit.config.db[environment].database,
      file: path.join(path.dirname(file), path.basename(file, '.bz2')),
      charSet: shipit.config.db.charSet ? `--default-character-set=${shipit.config.db.charSet}`: ''
    });
  };

  shipit.db.createCmd = function createCmd(environment) {
    return sprintf('mysql %(credentials)s %(charSet)s --execute \"CREATE DATABASE IF NOT EXISTS %(database)s;\"', {
      credentials: shipit.db.credentialParams(shipit.config.db[environment]),
      database: shipit.config.db[environment].database,
      charSet: shipit.config.db.charSet ? `--default-character-set=${shipit.config.db.charSet}`: ''
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

    return shipit.config.db.shell.zip || sprintf('bzip2 > %(dumpFile)s', {
      dumpFile: file,
    });
  };

  shipit.db.dump = function dump(environment, file) {
    if (shipit.config.db.shell.dump) {
      return shipit.config.db.shell.dump.call(shipit, environment, file);
    }

    const sedCommands = [
      // Remove DEFINER clauses
      'sed -e "s/DEFINER[ ]*=[ ]*[^*]*\\*\\//\\*\\//g"',
      // Fix client charset
      'sed -e "s/SET character_set_client = [^;*]*\\([;*]\\)/SET character_set_client = utf8mb4\\1/g"',
      // Convert all charsets to utf8mb4
      'sed -e "s/CHARSET=[^; ]*\\([; ]\\)/CHARSET=utf8mb4\\1/g"',
      'sed -e "s/CHARACTER SET [^; ]*\\([; ]\\)/CHARACTER SET utf8mb4\\1/g"',
      // Specifically convert utf8mb3 collations to utf8mb4_general_ci (preserves existing utf8mb4 collations)
      'sed -e "s/COLLATE[ ]*=\\?[ ]*utf8mb3[^; ,)]*\\([; ,)]\\)/COLLATE utf8mb4_general_ci\\1/g"',
      // Add missing collations to specific CHARACTER SET utf8mb4 patterns
      'sed -e "s/CHARACTER SET utf8mb4 DEFAULT/CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci DEFAULT/g"',
      'sed -e "s/CHARACTER SET utf8mb4 NOT NULL/CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci NOT NULL/g"',
      'sed -e "s/CHARACTER SET utf8mb4 COMMENT/CHARACTER SET utf8mb4 COLLATE utf8mb4_general_ci COMMENT/g"',
      // Fix table-level charset declarations
      'sed -e "s/DEFAULT CHARSET=utf8mb4\\([^C;]\\)/DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci\\1/g; s/DEFAULT CHARSET=utf8mb4;/DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;/g"',
      // Only convert non-utf8mb4 collations to utf8mb4_general_ci (conditional - only if table doesn't already have utf8mb4 collation)
      'sed -e "/DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci/!s/COLLATE[ ]*=\\?[ ]*[^; ,)]*\\([; ,)]\\)/COLLATE utf8mb4_general_ci\\1/g"',
      // Fix duplicate collations that might have been created
      'sed -e "s/COLLATE=[^; ,)]* COLLATE=utf8mb4_general_ci/COLLATE=utf8mb4_general_ci/g"',
      'sed -e "s/COLLATE=utf8mb4_general_ci COLLATE=[^; ,)]*/COLLATE=utf8mb4_general_ci/g"'
    ].join(" | ");

    return shipit[environment](
      `{ echo 'SET FOREIGN_KEY_CHECKS = 0;'; ${shipit.db.dumpCmd(environment)} | ${sedCommands}; echo 'SET FOREIGN_KEY_CHECKS = 1;'; } | ${shipit.db.zipCmd(file)}`
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
