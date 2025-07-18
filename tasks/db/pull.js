var utils = require('shipit-utils');
var path = require('path');
var db = require('../../lib/db');

module.exports = function(gruntOrShipit) {
  var task = function task() {
    var shipit = db(utils.getShipit(gruntOrShipit));
    var dumpFile = shipit.db.dumpFile('remote');
    var remoteDumpFilePath = path.join(shipit.sharedPath || shipit.currentPath, shipit.db.remoteDumpFile('remote', dumpFile));
    var localDumpFilePath = path.join(shipit.config.workspace, shipit.db.localDumpFile('remote', dumpFile));

    var download = function download() {
      return shipit.remoteCopy(remoteDumpFilePath, localDumpFilePath, {
        direction: 'remoteToLocal'
      });
    };

    return shipit.db.createDirs()
    .then(function() {
      return shipit.db.dump('remote', remoteDumpFilePath);
    })
    .then(download)
    .then(function() {
      return shipit.db.clean('remote', remoteDumpFilePath, shipit.config.db.cleanRemote);
    })
    .then(function() {
      return shipit.db.load('local', localDumpFilePath);
    })
    .then(function() {
      return shipit.db.clean('local', localDumpFilePath, shipit.config.db.cleanLocal);
    });
  };

  utils.registerTask(gruntOrShipit, 'db:pull:task', task);
};
