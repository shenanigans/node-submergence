
var util         = require ('util');
var EventEmitter = require ('events').EventEmitter;
var bunyan       = require ('bunyan');
var async        = require ('async');
var MongoDB      = require ('mongodb');
var filth        = require ('filth');
var Transport    = require ('./lib/Transport');
var Backplane    = require ('./lib/Backplane');

var Agent = require ('./lib/Agent');
var Reply = require ('./lib/Reply');


/**     @module/class submergence
    @super events.EventEmitter
@argument/:Configuration config
    Configuration options to use for this instance.
@event userOnline
    When the first live connection for a user id comes online, exactly one substation instance in
    your cluster will emit this event once.
    @argument/String domain
        The internet hostname where this User is logged in.
    @argument/String user
        The User ID that has just appeared online.
@event clientOnline
    When the first live connection for a client id comes online, exactly one substation instance in
    your cluster will emit this event once. It is emitted for every client belonging to a user.
    @argument/String domain
        The internet hostname where this User is logged in.
    @argument/String user
        The User ID that has just appeared online.
    @argument/String client
        The Client ID that has just appeard online.
@event userOffline
    When the last live connection for a user id goes offline, exactly one substation instance in
    your cluster will emit this event once.
    @argument/String domain
        The internet hostname where this User is logged in.
    @argument/String user
        The User ID that has just appeared offline.
@event clientOffline
    When the last live connection for a client id goes offline, exactly one substation instance in
    your cluster will emit this event once. It is emitted for every client belonging to a user.
    @argument/String domain
        The internet hostname where this User is logged in.
    @argument/String user
        The User ID that has just appeared offline.
    @argument/String client
        The Client ID that has just appeard offline.
@event liveConnection
    Each time a User initiates a new live connection to this `substation` instance, this event is
    emitted. It offers the client an immediate opportunity to emit events targetted to the
    individual connection.
    @argument/submergence:Agent
    @submergence:Reply reply
@event peerRequest
    @argument/submergence:Agent
    @argument/Object query
    @callback connect
        @argument/String user
        @argument/String client
            @optional
        @argument/Object query
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
module.exports.Agent = Agent;
module.exports.Reply = Reply;

/**     @submodule/class Configuration
    Configuration options to use for this instance.
@member/Boolean allowForeignSockets
    @default `true`
    Whether a user is permitted to open a `Socket.io` connection from a context without same-origin
    privelege, such as an iframe. Such connections perform all actions with [isDomestic]
    (:Agent#isDomestic) set to `false`. With `Socket.io` connections, [Agent](:Agent) instances are
    retained between Actions, so note that manual changes to the Agent's status flags will persist
    throughout the connection.
@member/Boolean binaryStreams
    If true, accept arbitrary content types and pass them, as well as `multipart/form-data` requests,
    to the Action as streams. The content type will be passed as [contentType]
    (:Request#contentType), the [request body](:Request#body) will be `undefined` and the [request
    stream](stream.Readable) will be passed as [request.stream](:Request#stream).
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
@member/:Authentication:Configuration Authentication
@member/:Backplane:Configuration Backplane
@member/json Cache
    @member/Number Cache#maxItems
    @member/Number Cache#maxDuration
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
submergence.DEFAULT_CONFIG = DEFAULT_CONFIG;

/**     @member/Function listen
    Connect to MongoDB, establish several Collection instances and open a port by calling [listen]
    (:Transport#listen) on our [Transport](:Transport) instance.
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


/**     @member/Function sendEvent
    Select all active `Socket.io` connections belonging to a User or User/Client pair on a domain
    String.
@argument/String domain
@argument/String user
@argument/String client
    @optional
@argument/Object info
@callback
    @optional
    @argument/Error|undefined err
    @argument/Boolean didSend
*/
submergence.prototype.sendEvent = function (domain, user, client, info, callback) {
    this.backplane.sendEvent (domain, user, client, info, callback);
};


/**     @member/Function isActive
    Determine whether there is at least one active `Socket.io` connection belonging to a User or a
    User/Client pair on a domain String.
@argument/String domain
@argument/String user
@argument/String client
    @optional
@callback
    @argument/Error|undefined err
    @argument/Boolean isActive
*/
submergence.prototype.isActive = function (domain, user, client, callback) {
    this.backplane.isActive (domain, user, client, callback);
};

/**     @interface :Action

@member/Function run
    @argument/submergence station
    @argument/submergence:Agent agent
    @argument/submergence:Request request
    @argument/submergence:Reply reply
*/
/**     @class :Action:Configuration
@member/String|RegExp|undefined route
    As an alternative to setting the route [when activating the Action](substation#addAction) you
    may set it in the configuration. Routes specified in `addAction` override configured routes.

    When a route is set as a String, it is prepended with a forward slash if none is present and
    converted to a regular expression of the form `/pathname(?:/(.*))?`.
@member/Object|undefined context
    When generating html with a template but never when generating a JSON response, [response
    content](submergence:Reply#content) is non-destructively deep-merged over this Object. To put
    it another way, `context` sets default content for html pages.
@member/Function|undefined setup
    Called during the parent server's initialization stage, before the server has begun accepting
    requests. It's usually the best time to establish database access. Two arguments are passed:
     * [Object]() `configuration` The Action's configuration Object. You may still edit the
        configuration at this point.
     * [Function]() `callback` Call when the Action is ready to use.
@member/json authentication
    @property/Boolean authentication.isLoggedIn
        @default `false`
        Require that the Agent be logged into the application to use access this Action.
    @property/Boolean authentication.isDomestic
        @default `false`
        Require that the Agent have same-origin access priveleges in the client context to use this
        Action. This secures against XSS attacks in the browser.
    @property/Boolean authentication.allowGuests
        @default `true`
        Requires that an Agent be at least an Idle Agent to use this Action. An Idle Agent is one
        that presents an expired session token. It is possible for an Idle Agent to be Domestic.
@member/Boolean hidden
    @default `false`
    This Action will not appear during OPTIONS requests.
@member/Boolean hideSchema
    @default `false`
    This Action's schemata will not be reported during OPTIONS requests.
@member/Boolean binaryStreams
    @default `false`
    If true, accept unknown content types and pass them, as well as `multipart/form-data` requests,
    to the Action as streams. The passed `request.body` will be a `ReadableStream` instance.
@member/Boolean neverBuffer
    @default `false`
    When [binaryStreams](#binaryStreams) is set and so is `neverBuffer`, the [request body]
    (.Request.body) is always a [stream](stream.Readable).
@member/Number bufferFiles
    @default `64000`
    Prebuffer trivial files into memory, up to the given number of bytes across all uploaded files.
    Handy for handling small file uploads (such as user avatars) in an application where file
    uploads are not a significant feature.
*/

/**     @interface :Router

@member/Function init
    @callback
@member/Function getAction
    @argument/http.IncomingMessage request
    @argument/String pathstr
    @callback
        @argument/:Action|undefined action
        @argument/Array<String>|undefined params
*/

/**     @submodule/class Request
    Incoming Action requests, whether from `http` or `Socket.io` are mapped to a Request instance.
    If [bodySchema](:Configuration#bodySchema) and/or [querySchema](:Configuration#querySchema) are
    set, the `body` and/or `query` properties can be assumed to be conformant by the time the
    reaction Function is called.
@member/String method
    The method used to access this Action. Used when the Action is [mounted](substation#addAction)
    without a method specified.
@member/Object query
    Request url terms, GET parameters, etc. mapped as a simple flat Object and validated by the
    [query schema](:Configuration#querySchema) if specified.
@member/Array params
    If the route regex selected text after the route pattern or the route regex contains matching
    groups, the additional matched strings are passed in an Array.
@member/Object|undefined body
    The request body. Form requests are mapped to a simple flat Object and validated by the
    [body schema](:Configuration#bodySchema) if specified. JSON bodies are parsed and validated. If
    the [binary streams option](:Configuration#binaryStreams) is set, the body may be a [readable
    stream](stream.Readable). These actions should type-check with `instanceof` and dispatch
    accordingly, or use the [never buffer option](:Configuration#neverBuffer).
@member/Array|undefined files
    When [cacheFiles](:Configuration#cacheFiles) is set, an Array of file info Objects containing
    Buffers is passed. This is part of an unstable feature.
@member/stream.Readable|undefined stream
    When [binaryStreams](:Configuration#binaryStreams) is set, content types other than lightweight
    forms and JSON will be streamed instead of cached. If you do not consume the body before
    [replying](submergence:Reply#done) the incoming body stream is automatically closed with the
    'Connection:close' header.
@member/String|undefined contentType
    If the remote client has provided it, the value of the `Content-Type` header is included
    whenever `stream` is used.
*/
