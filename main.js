
var util         = require ('util');
var EventEmitter = require ('events').EventEmitter;
var bunyan       = require ('bunyan');
var async        = require ('async');
var MongoDB      = require ('mongodb');
var filth        = require ('filth');
var Transport    = require ('./lib/Transport');
var Backplane    = require ('./lib/Backplane');

module.exports.Agent = require ('./lib/Agent');
module.exports.Reply = require ('./lib/Reply');


/**     @module/class submergence

@argument/.Configuration config
*/
function submergence (config) {
    if (!(this instanceof submergence))
        return new submergence (config);
    EventEmitter.call (this);
    this.config = filth.clone (DEFAULT_CONFIG);
    filth.merge (this.config, config);

    this.logger = new bunyan ({
        name:   this.config.applicationName,
        stream: process.stdout,
        level:  this.config.loggingLevel
    });
    this.transport = new Transport (this, this.config);
    this.backplane = new Backplane (this, this.config.Backplane);
}
util.inherits (submergence, EventEmitter);
module.exports = submergence;

/**     @struct submergence.Configuration
    Configuration options for the [submergence]() service layer.
@member/Boolean allowForeignSockets
    @default `true`
    Whether a user is permitted to open a `Socket.io` connection from a context without same-origin
    privelege, such as an iframe. Such connections perform all actions with [isDomestic]
    (substation.Agent#isDomestic) set to `false`.
@member/Boolean binaryStreams
    If true, accept arbitrary content types and pass them, as well as `multipart/form-data` requests,
    to the Action as streams. The passed `request.body` will be a `ReadableStream` instance.
@member/Number bufferFiles
    @default `64000`
    Prebuffer trivial files into memory, up to the given number of bytes across all uploaded files.
    Handy for handling small file uploads (such as user avatars) in an application where file
    uploads are not a major feature.
@member/String databaseName
    @default `"submergence"`
@member/String databaseAddress
    @default `"127.0.0.1"`
@member/Number databasePort
    @default `27017`
@member/String sessionCollectionName
    @default `"submergence"`
@member/String LinksCollectionName
    @default `"Links"`
    Database collection name for storing "link tokens" which are used to pass WebRTC connection
    traffic after the initial connection has been allowed.
    @default `"Links"`
@member/mongodb:Collection|undefined LinksCollection
    Optionally override MongoDB setup however you want by passing in a pre-configured Collection
    driver instance.
@member/String loggingLevel
    @default `"info"`
@member/String applicationName
    @default `"submergence"`
@member/.Authentication.Configuration Authentication
@member/.Backplane.Configuration Backplane
@member/.Cache.Configuration Cache
*/
var DEFAULT_CONFIG = {
    allowForeignSockets:    true,
    binaryStreams:          false,
    bufferFiles:            64000,
    databaseName:           "submergence",
    databaseAddress:        "127.0.0.1",
    databasePort:           27017,
    sessionCollectionName:  "Session",
    LinksCollectionName:    "Links",
    loggingLevel:           "info",
    applicationName:        "submergence",
    Authentication:         {
        cacheSessions:          100000,
        sessionsCollectionName: "Sessions",
        sessionCacheTimeout:    1000 * 60 * 30, // thirty minutes
        sessionLifespan:        1000 * 60 * 60 * 24, // one day
        sessionRenewalTimeout:  1000 * 60 * 60 * 24 * 3, // three days
        loginLifespan:          1000 * 60 * 60 * 24 * 7 * 2, // two weeks
        cookieLifespan:         1000 * 60 * 60 * 24 * 365 // one year
    },
    Backplane:              {
        port:                   9001,
        collectionName:         "Backplane",
        hostsCollectionName:    "BackplaneHosts",
        cacheLinks:             20480,
        linkCacheTimeout:       1000 * 60 * 5 // five minutes
    },
    Cache:                  {
        maxItems:               512,
        maxDuration:            1000 * 60 * 60 // 1 hour
    }
};

/**     @member/Function listen

@argument/Number port
@argument/:Router router
@callback
*/
submergence.prototype.listen = function (port, router, callback) {
    var config = this.config;
    var self = this;

    function finalize (err) {
        self.backplane.init (function (err) {
            if (err)
                return self.logger.fatal (err);
            self.transport.listen (port, router, function (err) {
                if (err)
                    return self.logger.fatal (err);
                callback();
            });
        });
    }

    if (
        config.SessionsCollection
     && config.BackplaneCollection
     && config.HostsCollection
     && config.LinksCollection
     && config.DomainsCollection
    ) {
        // no need to open a new database connection
        this.SessionsCollection = config.SessionsCollection;
        this.BackplaneCollection = config.BackplaneCollection;
        this.HostsCollection = config.HostsCollection;
        this.LinksCollection = config.LinksCollection;
        this.DomainsCollection = config.DomainsCollection;
        return finalize();
    }

    var Database = new MongoDB.Db (
        config.databaseName,
        new MongoDB.Server (config.databaseAddress, config.databasePort),
        { w:'majority', journal:true }
    );
    Database.open (function (err) {
        if (err) {
            self.logger.fatal (err);
            return;
        }
        async.parallel ([
            function (callback) {
                if (self.SessionsCollection)
                    return callback();
                if (config.SessionsCollection) {
                    self.SessionsCollection = config.SessionsCollection;
                    return callback();
                }
                Database.collection (config.sessionCollectionName, function (err, collection) {
                    if (err) {
                        self.logger.fatal (err);
                        return process.exit (1);
                    }
                    self.SessionsCollection = collection;
                    callback();
                });
            },
            function (callback) {
                if (self.BackplaneCollection)
                    return callback();
                if (config.Backplane.Collection) {
                    self.BackplaneCollection = config.Backplane.Collection;
                    return callback();
                }
                Database.collection (config.Backplane.collectionName, function (err, collection) {
                    if (err) {
                        self.logger.fatal (err);
                        return process.exit (1);
                    }
                    self.BackplaneCollection = collection;
                    callback();
                });
            },
            function (callback) {
                if (self.HostsCollection)
                    return callback();
                if (config.Backplane.HostsCollection) {
                    self.BackplaneHostsCollection = config.Backplane.HostsCollection;
                    return callback();
                }
                Database.collection (config.Backplane.hostsCollectionName, function (err, collection) {
                    if (err) {
                        self.logger.fatal (err);
                        return process.exit (1);
                    }
                    self.BackplaneHostsCollection = collection;
                    callback();
                });
            },
            function (callback) {
                if (self.LinksCollection)
                    return callback();
                if (config.LinksCollection) {
                    self.LinksCollection = config.LinksCollection;
                    return callback();
                }
                Database.collection (config.LinksCollectionName, function (err, collection) {
                    if (err) {
                        self.logger.fatal (err);
                        return process.exit (1);
                    }
                    self.LinksCollection = collection;
                    callback();
                });
            }
        ], finalize);
    });
};
