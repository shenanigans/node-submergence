
var crypto = require ('crypto');
var async = require ('async');
var nssocket = require ('nssocket');
var cachew = require ('cachew');
var filth = require ('filth');
var uid = require ('infosex').uid.craft;

/**     @module/class submergence:Backplane
    @root

@argument/submergence parent
@argument/:Configuration config
*/
function Backplane (parent, config) {
    this.parent = parent;
    this.config = filth.clone (DEFAULT_CONFIG);
    if (config)
        filth.merge (this.config, config);

    this.connectionState = {};
    this.backplaneConnections = {};
    this.backplaneConnectionQueues = {};
    this.nextConnectionID = 1;
}


/**     @submodule/class Configuration
    Configuration options for the service Backplane.
@member/Number port
    @default `9001`
    Other `submergence` instances connect to the Backplane port to target events to Clients connected
    to this instance.
@member/String collectionName
    @default `"Backplane"`
    Name of the MongoDB collection used to store live connection records. Ignored when using
    `SessionsCollection`.
@member/String hostsCollectionName
    @default `"BackplaneHosts"`
    Name of the MongoDB collection used to coordinate Backplane instances.
@member/mongodb.Collection|undefined Collection
    Optionally override MongoDB setup however you want by passing in a pre-configured Collection
    driver instance.
@member/mongodb.Collection|undefined HostsCollection
    Optionally override MongoDB setup however you want by passing in a pre-configured Collection
    driver instance.
@member/Number cacheLinks
    Maximum number of Link tokens to cache. Set to `0` or any falsey value to disable Link caching.
@member/Number linkCacheTimeout
    Maximum time, in milliseconds, to cache Link tokens.
*/
var DEFAULT_CONFIG = {
    port:                   9001,
    collectionName:         "Backplane",
    hostsCollectionName:    "BackplaneHosts",
    cacheLinks:             20480,
    linkCacheTimeout:       1000 * 60 * 5 // five minutes
};


/**     @member/Function init
    Prepare the Backplane for communication with other server instances.
@callback
    @argument/Error|undefined err
*/
Backplane.prototype.init = function (callback) {
    if (this.initialized)
        return process.nextTick (callback);
    this.initialized = true;

    var backplaneConnections = this.backplaneConnections;
    var self = this;
    var BackplaneCollection = this.parent.BackplaneCollection;
    var BackplaneHostsCollection = this.parent.BackplaneHostsCollection;

    if (this.config.cacheLinks)
        this.linkCache = new cachew.ChainCache (
            this.config.linkCacheTimeout,
            this.config.cacheLinks
        );

    var backplaneID;
    async.parallel ([
        function (callback) {
            BackplaneCollection.ensureIndex (
                { user:1, 'domain':1 },
                { unique:true, name:'User/Client' },
                callback
            );
        },
        function (callback) {
            BackplaneCollection.ensureIndex (
                { user:1, 'user':1 },
                { unique:true, name:'User/Client' },
                callback
            );
        },
        function (callback) {
            BackplaneCollection.ensureIndex (
                { user:1, 'live.client':1 },
                { unique:true, name:'User/Client' },
                callback
            );
        },
        function (callback) {
            BackplaneCollection.ensureIndex (
                { user:1, 'link.client':1 },
                { unique:true, name:'User/Client' },
                callback
            );
        },
        function (callback) {
            BackplaneCollection.ensureIndex (
                { created:1 },
                { name:'CreationTime' },
                callback
            );
        },
        function (callback) {
            BackplaneCollection.ensureIndex (
                { 'live.bid':1 },
                { name:'BackplaneID' },
                callback
            );
        },
        function (callback) {
            BackplaneHostsCollection.ensureIndex (
                { address:1, port:1 },
                { name:'NetworkLocation' },
                callback
            );
        },
        // establish BID, destroy stranded BID
        function (callback) {
            uid (function (id) {
                backplaneID = id;
                self.backplaneID = backplaneID;
                BackplaneHostsCollection.findAndModify (
                    { address:self.config.address || '127.0.0.1', port:self.config.port },
                    { address:1, port:1 },
                    { $set:{ BID:backplaneID } },
                    { upsert:true },
                    function (err, oldHost) {
                        if (err) {
                            self.parent.logger.fatal ('failed to access database', err);
                            return process.exit(1);
                        }
                        if (!oldHost || oldHost.BID == backplaneID)
                            return callback();
                        BackplaneCollection.find (
                            { 'live.bid':oldHost.BID },
                            function (err, cursor) {
                                if (err) {
                                    self.parent.logger.fatal ('failed to access database', err);
                                    return process.exit(1);
                                }
                                cursor.each (function (err, rec) {
                                    if (err) {
                                        self.parent.logger.fatal ('failed to access database', err);
                                        return process.exit(1);
                                    }

                                    if (!rec) return callback();

                                    var live = rec.live;
                                    if (!live || !live.length) return;
                                    var count = 0;
                                    for (var i=0,j=live.length; i<j; i++)
                                        if (live[i].bid == oldHost.BID)
                                            count--;
                                    BackplaneCollection.update (
                                        { domain:rec.domain, user:rec.user },
                                        {
                                            $pull:  { live:{ bid:oldHost.BID } },
                                            $inc:   { count:count }
                                        },
                                        { w:0 }
                                    );
                                });
                            }
                        );
                    }
                );
            });
        }
    ], function (err) {
        if (err)
            return callback (err);

        // create a reusable subdocument that describes this Backplane instance
        self.record = {
            address:    self.config.address || '127.0.0.1',
            port:       self.config.port,
            bid:        backplaneID
        };

        // open event receiver port
        self.server = nssocket.createServer (function (socket) {
            socket.data ([ 'event' ], function (info) {
                self.fireEvent (info.domain, info.user, info.client, info.event);
            });
            socket.data ([ 'peer' ], function (info) {
                self.firePeerEvent (info.domain, info.user, info.client, info.event, info.exclude);
            });
            socket.data ([ 'kick' ], function (info) {
                self.killConnections (info.domain, info.user, info.client);
            });
            socket.data ([ 'open' ], function (info) {
                if (!Object.hasOwnProperty.call (backplaneConnections, info.id)) {
                    backplaneConnections[info.id] = socket;
                    socket.send ('ready', true);
                    return;
                }

                var existing = backplaneConnections[info.id];
                if (existing.locked || info.fortune < existing.fortune)
                    return socket.destroy();
                if (info.fortune == existing.fortune)
                    return crypto.randomBytes (4, function sendFortune (err, fortune) {
                        if (err)
                            return crypto.randomBytes (4, sendFortune);
                        socket.data ([ 'open' ], {
                            id:         backplaneID,
                            fortune:    fortune.readUInt32 (fortune)
                        });
                    });

                existing.destroy();
            });
        });
        self.server.listen (self.config.port, callback);
    });
};


/**     @member/Function connect
    Establish a connection to a remote Backplane, negotiating colisions if necessary.
@argument/String address
@argument/Number port
@argument/String backplaneID
@callback
    @argument/Error|undefined err
    @argument/nssocket|undefined socket
*/
Backplane.prototype.connect = function (address, port, backplaneID, callback) {
    var backplaneConnections = this.backplaneConnections;
    var backplaneConnectionQueues = this.backplaneConnectionQueues;
    var self = this;

    if (Object.hasOwnProperty.call (backplaneConnections, backplaneID)) {
        var socket = backplaneConnections[backplaneID];
        return process.nextTick (function(){ callback (undefined, socket); });
    }

    if (Object.hasOwnProperty.call (backplaneConnectionQueues, backplaneID)) {
        backplaneConnectionQueues[backplaneID].push (callback);
        return;
    }

    var queue = backplaneConnectionQueues[backplaneID] = [ callback ];
    crypto.randomBytes (4, function createFortune (err, fortune) {
        if (err)
            return crypto.randomBytes (4, createFortune);

        var socket = new nssocket.NsSocket();
        socket.data ([ 'event' ], function (info) {
            self.fireEvent (info.domain, info.userID, info.clientID, info.info);
        });
        socket.data ([ 'peer' ], function (info) {
            self.firePeerEvent (info.domain, info.userID, info.clientID, info.agent, info.event, info.exclude);
        });
        socket.data ([ 'open' ], function (info) {
            // this always means a colision occured
            crypto.randomBytes (4, function compareFortune (err, fortune) {
                if (err)
                    return crypto.randomBytes (4, compareFortune);
                logger.info ('negotiating Backplane peer colision', { fortune:fortune });
                socket.send ([ 'open' ], { id:backplaneID, fortune:fortune.readUInt32() });

                if (fortune == info.fortune) // another colision! Wait for the next event
                    return;

                var finalSocket;
                if (fortune > info.fortune) {
                    if (Object.hasOwnProperty.call (backplaneConnections, backplaneID))
                        backplaneConnections[backplaneID].destroy();
                    backplaneConnections[backplaneID] = socket;
                    finalSocket = socket;
                } else if (!Object.hasOwnProperty.call (backplaneConnections, backplaneID)) {
                    backplaneConnections[backplaneID] = socket;
                    finalSocket = socket;
                } else {
                    socket.destroy();
                    finalSocket = backplaneConnections[backplaneID];
                }

                delete backplaneConnectionQueues[backplaneID];
                for (var i=0,j=queue.length; i<j; i++)
                    queue[i] (undefined, finalSocket);
            });
        });
        socket.data ([ 'ready' ], function(){
            backplaneConnections[backplaneID] = socket;
            delete backplaneConnectionQueues[backplaneID];
            if (err) {
                for (var i=0,j=queue.length; i<j; i++)
                    queue[i] (err);
                return;
            }
            for (var i=0,j=queue.length; i<j; i++)
                queue[i] (undefined, socket);
        });

        socket.connect (port, address, function (err) {
            socket.send ([ 'open' ], { id:backplaneID, fortune:fortune });
        });
    });
};



/**     @member/Function sendEvent

@argument/String user
@argument/String|undefined client
    Fires events on all live sessions attached to a specific client on all `submergence` instances.
    If ommitted, all active live sessions belonging to the `user` receive events.
@argument/Array event
@callback
    @argument/Error|undefined err
*/
Backplane.prototype.sendEvent = function (domain, user, client, event, callback) {
    var config = this.config;
    var self = this;

    // hit local connections
    var didReceive = this.fireEvent (domain, user, client, event);

    if (client) {
        this.parent.BackplaneCollection.aggregate ([
            { $match:{ domain:domain || null, user:user, 'live.client':client } },
            { $project:{ live:true } },
            { $unwind:'$live' },
            { $match:{ 'live.client':client } }
        ], function (err, connections) {
            async.each (connections, function (connection, callback) {
                var host = connection.live;
                self.connect (host.address, host.port, host.bid, function (err, backplaneSocket) {
                    if (err) {
                        if (callback)
                            callback (err);
                        return;
                    }
                    var eventID = self.nextEventID++;
                    backplaneSocket.send ([ 'sendEvent' ], {
                        domain:     domain,
                        user:       user,
                        client:     client,
                        event:      event
                    });
                    didReceive = true;
                });
            }, function (err) {
                if (err) {
                    if (callback)
                        callback (err);
                    return;
                }
                if (callback)
                    callback (undefined, didReceive);
            });
        });
        return;
    }

    this.parent.BackplaneCollection.findOne (
        { user:user, domain:domain || null },
        function (err, userRecord) {
            if (err) return callback (err);
            if (!userRecord) {
                if (callback)
                    callback (undefined, false);
                return;
            }
            if (!userRecord.live || !userRecord.live.length) {
                if (callback)
                    callback (undefined, didReceive);
                return;
            }
            async.each (userRecord.live, function (host, callback) {
                if (host.address == self.record.address && host.port == self.record.port)
                    // that's just us
                    return callback();

                self.connect (host.address, host.port, host.bid, function (err, backplaneSocket) {
                    if (err) {
                        if (callback)
                            callback (err);
                        return;
                    }

                    backplaneSocket.send ([ 'event' ], {
                        domain: domain,
                        user:   user,
                        client: client,
                        event:  event
                    });
                    didReceive = true;
                    if (callback)
                        callback();
                });
            }, function (err) {
                if (err) {
                    if (callback)
                        callback (err);
                    return;
                }
                if (callback)
                    callback (undefined, didReceive);
            });
        }
    );
};


/**     @member/Function routePeerEvent

@argument/Object event
@argument/submergence:Agent agent
@callback
*/
Backplane.prototype.routePeerEvent = function (event, agent, callback) {
    if (event.from && event.from === event.to) {
        if (callback)
            process.nextTick (callback);
        return;
    }
    var linkRec, sender, recipient;
    var linkCache = this.linkCache;
    var self = this;
    function shipInit (err, linkRec) {
        if (err) {
            self.parent.logger.error ('failed to send peer offer echoes', err);
            return;
        }
        linkRec = linkRec.value;
        if (!linkRec) // socket has already sent or received an init
            return;

        if (linkCache)
            linkCache.set (event.token, linkRec);
        self.sendPeerEvent (
            recipient.domain,
            recipient.user,
            recipient.client,
            { token:event.token, from:event.from, init:true, query:sender.query },
            linkRec.init,
            function (err, didReceive) {
                if (didReceive)
                    return;

                // nobody awake to connect to link
                // cull the link record and subdocs
                self.parent.BackplaneCollection.update (
                    { user:agent.user },
                    { $pull:{ link:{
                        client:     linkRec.client,
                        tgtUser:    linkRec.tgtUser,
                        tgtClient:  linkRec.tgtClient
                    } } },
                    function (err) {
                        if (err) {
                            self.parent.logger.error (
                                'error while culling defunct Link metadata',
                                err
                            );
                            return;
                        }
                    }
                );
                self.parent.BackplaneCollection.update (
                    { user:linkRec.tgtUser },
                    { $pull:{ link:{
                        client:     linkRec.tgtClient,
                        tgtUser:    agent.user,
                        tgtClient:  linkRec.client
                    } } },
                    function (err) {
                        if (err) {
                            self.parent.logger.error (
                                'error while culling defunct Link metadata',
                                err
                            );
                            return;
                        }
                    }
                );
                self.parent.LinksCollection.update (
                    { _id:linkRec.token },
                    { $set:{ closed:true } },
                    function (err) {
                        if (err) {
                            self.parent.logger.error (
                                'error while culling defunct Link metadata',
                                err
                            );
                            return;
                        }
                    }
                );
            }
        );
    }

    // cached token record?
    if (linkCache && (linkRec = linkCache.get (event.token))) {
        // choose recipient
        // if client specified, worry about case: user_A/client_1 -> user_A/client_2
        if (linkRec.party[0].user == agent.user) {
            sender = linkRec.party[0];
            recipient = linkRec.party[1];
            if (sender.client && sender.client != agent.client) {
                sender = recipient;
                recipient = linkRec.party[0];
            }
        } else {
            recipient = linkRec.party[0];
            sender = linkRec.party[1];
        }
        if (sender.user != agent.user)
            // token doesn't belong to this user
            return;
        if (sender.client && sender.client != agent.client)
            // token doesn't belong to this client
            return;

        event.query = sender.query;
        self.sendPeerEvent (
            recipient.domain,
            recipient.user,
            recipient.client,
            event,
            undefined,
            callback
        );

        // any chance we need to ship additional `init` messages for this event?
        if (!event.ICE && (
            !linkRec.init
         || !Object.hasOwnProperty.call (linkRec.init, event.from)
        )) { // confirm with the db and lock the sender
            self.parent.LinksCollection.findAndModify (
                { _id:event.token, init:{ $ne:event.from }, closed:false },
                { _id:1 },
                { $push:{ init:{ $each:[ event.from ], $slice:-30 } } },
                shipInit
            );
        }

        return;
    }

    // ask the database about the presented token
    this.parent.LinksCollection.findOne ({ _id:event.token, closed:false }, function (err, linkRec) {
        if (err || !linkRec)
            return;

        // choose recipient
        // if client specified, worry about case: user_A/client_1 -> user_A/client_2
        if (linkRec.party[0].user == agent.user) {
            sender = linkRec.party[0];
            recipient = linkRec.party[1];
            if (sender.client && sender.client != agent.client) {
                sender = recipient;
                recipient = linkRec.party[0];
            }
        } else {
            recipient = linkRec.party[0];
            sender = linkRec.party[1];
        }
        if (sender.user != agent.user)
            // token doesn't belong to this user
            return;
        if (sender.client && sender.client != agent.client)
            // token doesn't belong to this client
            return;

        if (linkCache)
            linkCache.set (event.token, linkRec);

        event.query = sender.query;
        self.sendPeerEvent (
            recipient.domain,
            recipient.user,
            recipient.client,
            event,
            undefined,
            callback
        );

        // any chance we need to ship additional `init` messages for this event?
        if (!event.ICE && (
            !linkRec.init
         || !Object.hasOwnProperty.call (linkRec.init, event.from)
        )) // confirm with the db and lock the sender
            self.parent.LinksCollection.findAndModify (
                { _id:event.token, init:{ $ne:event.from }, closed:false },
                { _id:1 },
                { $push:{ init:{ $each:[ event.from ], $slice:-30 } } },
                shipInit
            );
    });
}


/**     @member/Function sendPeerEvent

@argument/String user
@argument/String|undefined client
@argument/Object event
@argument/Array|undefined exclude
@callback
    @argument/Error|undefined err
*/
Backplane.prototype.sendPeerEvent = function (domain, user, client, event, exclude, callback) {
    var config = this.config;
    var self = this;

    // hit local connections
    var didReceive = this.firePeerEvent (domain, user, client, event, exclude);
    if (event.to && didReceive)
        return process.nextTick (function(){ callback (undefined, true); });

    if (client) {
        this.parent.BackplaneCollection.aggregate ([
            { $match:{ user:user, domain:domain || null, 'live.client':client } },
            { $unwind:'$live' },
            { $match:{ 'live.client':client } },
            { $project:{ live:true } }
        ], function (err, connections) {
            async.each (connections, function (connection, callback) {
                var host = connection.live;
                self.connect (host.address, host.port, host.bid, function (err, backplaneSocket) {
                    if (err) {
                        self.parent.logger.error (
                            'failed to reach Backplane host',
                            err
                        );
                        return;
                    }
                    backplaneSocket.send ([ 'peer' ], {
                        domain:     domain,
                        user:       user,
                        client:     client,
                        event:      event,
                        exclude:    exclude
                    });
                    didReceive = true;
                });
            }, function (err) {
                if (err) {
                    self.parent.logger.error (
                        'failed to retrieve Links by Client',
                        err
                    );
                    return callback (err);
                }
                callback (undefined, true);
            });
        });
        return;
    }

    this.parent.BackplaneCollection.findOne (
        { user:user, domain:domain || null },
        function (err, userRecord) {
            if (err) return callback (err);
            if (!userRecord)
                return callback (undefined, didReceive);
            if (!userRecord.live || !userRecord.live.length)
                return callback (undefined, didReceive);

            async.each (userRecord.live, function (host, callback) {
                if (host.address == self.record.address && host.port == self.record.port)
                    // that's this Backplane node
                    return callback();

                self.connect (host.address, host.port, host.bid, function (err, backplaneSocket) {
                    if (err)
                        return callback (err);
                    backplaneSocket.send ([ 'peer' ], {
                        domain:     domain || null,
                        user:       user,
                        client:     client,
                        event:      event,
                        exclude:    exclude
                    });
                    callback();
                });
            }, function (err) {
                if (err)
                    return callback (err);
                callback (undefined, true);
            });
        }
    );
};


/**     @member/Function fireEvent
    @development
    Sends an incoming event message to the relavant local sockets.
@argument/String user
@argument/String|undefined client
@argument/Array event
*/
Backplane.prototype.fireEvent = function (domain, user, client, event) {
    var connectionState = this.connectionState;
    var clientConnections;

    var pointer = connectionState;
    if (domain)
        if (!Object.hasOwnProperty.call (pointer, domain))
            return false;
        else
            pointer = pointer[domain];
    if (!Object.hasOwnProperty.call (pointer, user))
        return false; // no connections for user
    else
        pointer = pointer[user];

    if (client) {
        if (!Object.hasOwnProperty.call (pointer, client))
            return false; // no connections for client
        clientConnections = pointer[client];
        for (var i=0,j=clientConnections.length; i<j; i++)
            clientConnections[i].emit ('event', event);
        return true;
    }

    // send to every connection on every client
    var clientIDs = Object.keys (pointer);
    var sent = false;
    for (var i=0,j=clientIDs.length; i<j; i++) {
        clientConnections = pointer[clientIDs[i]];
        if (clientConnections.length)
            sent = true;
        for (var k=0,l=clientConnections.length; k<l; k++)
            clientConnections[k].emit ('event', event);
    }
    return sent;
};


/**     @member/Function firePeerEvent
    @development
    Sends an incoming peer event message to the relavant local sockets.
@argument/String user
@argument/String|undefined client
@argument/Object event
@argument/Array|undefined exclude
*/
Backplane.prototype.firePeerEvent = function (domain, user, client, event, exclude) {
    var connectionState = this.connectionState;
    var clientConnections;

    var pointer = connectionState;
    if (domain)
        if (!Object.hasOwnProperty.call (pointer, domain))
            return false;
        else
            pointer = pointer[domain];
    if (!Object.hasOwnProperty.call (pointer, user))
        return false; // no connections for user
    else
        pointer = pointer[user];
    if (client) {
        if (!Object.hasOwnProperty.call (pointer, client))
            return false; // no connections for client
        clientConnections = pointer[client];
        if (event.to) {
            for (var i=0,j=clientConnections.length; i<j; i++)
                if (clientConnections[i].SID == event.to) {
                    clientConnections[i].emit ('peer', event);
                    return true;
                }
        } else if (exclude) {
            for (var i=0,j=clientConnections.length; i<j; i++)
                if (exclude.indexOf (clientConnections[i].SID) < 0)
                    clientConnections[i].emit ('peer', event);
        } else
            for (var i=0,j=clientConnections.length; i<j; i++)
                clientConnections[i].emit ('peer', event);
        return true;
    }

    // send to every connection on every client
    var clientIDs = Object.keys (pointer);

    // filter for one connection
    if (event.to) {
        for (var i=0,j=clientIDs.length; i<j; i++) {
            clientConnections = pointer[clientIDs[i]];
            for (var k=0,l=clientConnections.length; k<l; k++)
                if (clientConnections[k].SID == event.to) {
                    clientConnections[k].emit ('peer', event);
                    return true;
                }
        }
        return false;
    }

    var sent = false;
    if (exclude) {
        for (var i=0,j=clientIDs.length; i<j; i++) {
            clientConnections = pointer[clientIDs[i]];
            for (var k=0,l=clientConnections.length; k<l; k++)
                if (exclude.indexOf (clientConnections[k].SID) < 0) {
                    sent = true;
                    clientConnections[k].emit ('peer', event);
                }
        }
        return sent;
    }

    for (var i=0,j=clientIDs.length; i<j; i++) {
        clientConnections = pointer[clientIDs[i]];
        if (clientConnections.length)
            sent = true;
        for (var k=0,l=clientConnections.length; k<l; k++)
            clientConnections[k].emit ('peer', event);
    }

    return sent;
};


/**     @member/Function setLive
    @development
    Notify that a live session has gone on or off line.
@argument/String user
@argument/String client
@argument/socketio.Socket socket
@argument/Boolean status
*/
Backplane.prototype.setLive = function (domain, user, client, socket, status, callback) {
    var self = this;
    var BackplaneCollection = this.parent.BackplaneCollection;
    var address = this.config.BackplaneAddress;
    var port = this.config.port;
    var backplaneID = this.backplaneID;

    // attach a new connection to an ongoing link
    function joinLinks (err, bpRec) {
        if (!bpRec || !bpRec.link || !bpRec.link.length) {
            if (callback)
                callback();
            return;
        }

        async.each (bpRec.link, function (link, callback) {
            var eventMsg = {
                token:  link.token,
                from:   socket.SID,
                init:   true
            };
            self.parent.LinksCollection.update (
                { _id:link.token, init:{ $ne:socket.SID }, closed:false },
                { $push:{ init:{ $each:[ socket.SID ], $slice:-30 } } },
                function (err) {
                    if (err)
                        return callback (err);

                    self.routePeerEvent (domain, eventMsg, socket.agent, function (err, didReceive) {
                        if (err) return callback (err);

                        callback();
                        if (didReceive)
                            return;

                        // nobody received the link rejoin request
                        // cull the Link record and subdocs on each BPHost record
                        // self.parent.logger.info ("Link culled", { token:link.token });
                        BackplaneCollection.update (
                            { user:socket.agent.user, domain:domain || null },
                            { $pull:{ link:{
                                client:     link.client,
                                tgtUser:    link.tgtUser,
                                tgtClient:  link.tgtClient
                            } } },
                            function (err) {
                                if (err) {
                                    self.parent.logger.error (
                                        'error while culling defunct Link metadata',
                                        err
                                    );
                                    return;
                                }
                            }
                        );
                        BackplaneCollection.update (
                            { user:link.tgtUser, domain:domain },
                            { $pull:{ link:{
                                client:     link.tgtClient,
                                tgtUser:    socket.agent.user,
                                tgtClient:  link.client
                            } } },
                            function (err) {
                                if (err) {
                                    self.parent.logger.error (
                                        'error while culling defunct Link metadata',
                                        err
                                    );
                                    return;
                                }
                            }
                        );
                        self.parent.LinksCollection.update (
                            { _id:link.token },
                            { $set:{ closed:true } },
                            function (err) {
                                if (err) {
                                    self.parent.logger.error (
                                        'error while culling defunct Link metadata',
                                        err
                                    );
                                    return;
                                }
                            }
                        );
                    });
                }
            );
        }, function (err) {
            if (err) {
                if (callback)
                    callback (err);
                return;
            }
            if (callback)
                callback();
        });
    }

    // local connection info already present?
    // if opening or closing the first/last socket on a client or user, fall through to database
    var pointer = this.connectionState;
    if (domain)
        if (Object.hasOwnProperty.call (pointer, domain))
            pointer = pointer[domain];
        else {
            if (!status) {
                if (callback)
                    process.nextTick (callback);
                return;
            }
            ((pointer[domain] = {})[user] = {})[client] = [ socket ];
            return updateDB();
        }

    var userPointer = pointer;
    if (Object.hasOwnProperty.call (pointer, user))
        pointer = pointer[user];
    else {
        if (!status) {
            if (callback)
                process.nextTick (callback);
            return;
        }
        (pointer[user] = {})[client] = [ socket ];
        return updateDB();
    }

    if (!Object.hasOwnProperty.call (pointer, client)) {
        if (!status) {
            if (callback)
                process.nextTick (callback);
            return;
        }
        pointer[client] = [ socket ];
        return updateDB();
    } else {
        var connections = pointer[client];

        if (status) {
            if (connections.indexOf (socket) < 0)
                connections.push (socket);
            if (callback)
                process.nextTick (callback);
            BackplaneCollection.findOne ({ user:user, domain:domain || null }, { link:true }, joinLinks);
            return;
        }

        // drop local connection
        var position = connections.indexOf (socket);
        if (position >= 0)
            connections.splice (position, 1);
        if (connections.length) {
            if (callback)
                process.nextTick (callback);
            BackplaneCollection.findOne ({ user:user, domain:domain || null }, { link:true }, joinLinks);
            return;
        }

        // all local connections offline, notify database
        delete pointer[client];
        if (!Object.keys (userPointer).length)
            delete userPointer[user];
        updateDB();
    }

    function updateDB(){
        // User or Client has gone on or offline for this server instance
        // update the database
        var query, update;
        if (status)
            update = {
                $inc:{ count:1 },
                $push:{ live:{
                    client:     client,
                    address:    self.record.address,
                    port:       self.record.port,
                    bid:        self.record.bid
                } }
            };
        else
            update = {
                $inc:   { count:-1 },
                $pull:  { live:{ client:client, bid:backplaneID } }
            };
        BackplaneCollection.findAndModify (
            { user:user, domain:domain || null },
            { user:1 },
            update,
            { upsert:true, fields:{
                link:   true,
                count:  true,
                live:   { $elemMatch:{ bid:{ $ne:backplaneID }, client:client } }
            } },
            function checkResult (err, oldRecord) {
                if (err) {
                    self.parent.logger.error ('unable to set client live', err);
                    if (callback)
                        callback (err);
                    return;
                }

                // var pointer = self.connectionState;
                // var statusNow =
                //     ( !domain || (
                //         Object.hasOwnProperty.call (pointer, domain)
                //      && pointer = pointer[domain]
                //     ) )
                //  && Object.hasOwnProperty.call (pointer, user)
                //  && Object.hasOwnProperty.call (pointer[user], client)
                //  ;
                // if (status != statusNow) // things have changed since the last DB update went out
                //     return;

                // status changed on db, possibly send events
                if (status) {
                    if (oldRecord) {
                        if (oldRecord.link && oldRecord.link.length)
                            joinLinks (undefined, oldRecord);
                        if (!oldRecord.live || !oldRecord.live.length)
                            self.parent.emit ('clientOnline', domain, user, client);
                        if (!oldRecord.count)
                            // count was set to 0 and we can see the dropped record
                            self.parent.emit ('userOnline', domain, user);
                    } else {
                        self.parent.emit ('userOnline', domain, user);
                        self.parent.emit ('clientOnline', domain, user, client);
                    }
                    if (callback)
                        callback();
                    return;
                }

                if (!oldRecord) {
                    if (callback)
                        callback();
                    return;
                }

                if (oldRecord.count == 1) {
                    self.parent.emit ('userOffline', domain, user);
                    cullLinks = oldRecord.link;
                } else {
                    if (!oldRecord.live || !oldRecord.live.length)
                        self.parent.emit ('clientOffline', domain, user, client);
                    if (!oldRecord.link || !oldRecord.link.length) {
                        if (callback)
                            callback();
                        return;
                    }
                    cullLinks = [];
                    for (var i=0,j=oldRecord.link.length; i<j; i++)
                        if (oldRecord.link[i].client == client)
                            cullLinks.push (oldRecord.link[i]);
                }

                if (callback)
                    callback();

                if (!cullLinks || !cullLinks.length)
                    return;

                cullLinks.forEach (function (link) {
                    // cull the link record and subdocs
                    BackplaneCollection.update (
                        { user:user, domain:domain || null },
                        { $pull:{ link:{
                            client:     link.client,
                            tgtUser:    link.tgtUser,
                            tgtClient:  link.tgtClient
                        } } },
                        function (err) {
                            if (err) {
                                self.parent.logger.error (
                                    'error while culling defunct Link metadata',
                                    err
                                );
                                return;
                            }
                        }
                    );
                    BackplaneCollection.update (
                        { user:link.tgtUser, domain:domain || null },
                        { $pull:{ link:{
                            client:     link.tgtClient,
                            tgtUser:    user,
                            tgtClient:  link.client
                        } } },
                        function (err) {
                            if (err) {
                                self.parent.logger.error (
                                    'error while culling defunct Link metadata',
                                    err
                                );
                                return;
                            }
                        }
                    );
                    self.parent.LinksCollection.update (
                        { _id:link.token },
                        { $set:{ closed:true } },
                        function (err) {
                            if (err) {
                                self.parent.logger.error (
                                    'error while culling defunct Link metadata',
                                    err
                                );
                                return;
                            }
                        }
                    );
                });
            }
        );
    }
};


/**     @member/Function isActive

@argument/String user
@argument/String|undefined client
@callback
*/
Backplane.prototype.isActive = function (domain, user, client, callback) {
    var query = { user:user, domain:domain || null };
    if (client)
        query['live.client'] = client;
    else
        query.live = { $elemMatch:{} };

    this.parent.BackplaneCollection.findOne (
        query,
        function (err, rec) {
            if (err)
                return callback (err);
            callback (undefined, Boolean (rec));
        }
    );
};


/**     @member/Function kick
    Close connections and cull Links belonging to a User or Client. [Logging out]
    (submergence:Authentication#logout) triggers `kick` automatically.
@argument/String user
@argument/String|undefined client
@callback
*/
Backplane.prototype.kick = function (domain, user, client, callback) {
    var self = this;
    if (client) {
        this.parent.BackplaneCollection.findOne (
            { user:user, domain:domain || null },
            { live:true },
            function (err, rec) {
                if (err)
                    return callback (err);
                if (!rec || !rec.live || !rec.live.length)
                    return callback();
                var hostsToKick = [];
                for (var i=0,j=rec.live.length; i<j; i++) {
                    var host = rec.live[i];
                    if (
                        host.client == client
                     && host.address != self.record.address
                     && host.port != self.record.port
                    )
                        hostsToKick.push (host);
                }
                if (!hostsToKick.length)
                    return callback();
                async.each (hostsToKick, function (host, callback) {
                    self.connect (host.address, host.port, function (err, socket) {
                        if (err) return callback (err);
                        socket.send ([ 'kick' ], { domain:domain, user:user, client:client });
                        callback();
                    });
                }, callback);
            }
        );
        this.killConnections (domain, user, client);
        return;
    }

    this.parent.BackplaneCollection.findAndModify (
        { user:user, domain:domain || null },
        { user:1 },
        { $set:{ count:0, live:[], link:[] } },
        function (err, userRecord) {
            if (err) return callback (err);
            if (!userRecord) // who?
                return callback (new Error ('user not found'));

            // cull links - do not confirm
            var linkIDs = [];
            if (userRecord.link)
                for (var i=0,j=userRecord.link.length; i<j; i++)
                    linkIDs.push (userRecord.link[i].token);
            self.parent.LinksCollection.update (
                { _id:{ $in:linkIDs } },
                { $set:{ closed:true }},
                { safe:false }
            );

            // notify any other Backplane Hosts with connections to kick the User
            async.each (userRecord.live, function (host, callback) {
                if (host.address == self.record.address && host.port == self.record.port)
                    return callback();
                self.connect (host.address, host.port, host.bid, function (err, socket) {
                    if (err) return callback (err);
                    socket.send ([ 'kick' ], { domain:domain, user:user });
                    callback();
                });
            }, callback);
        }
    );

    // kill local connections
    this.killConnections (domain, user);
};


/**     @member/Function killConnections
    The cutting edge of [#kick](). Closes all local connections belonging to a User or Client.
@argument/String user
@argument/String|undefined client
*/
Backplane.prototype.killConnections = function (domain, user, client) {
    var pointer = this.connectionState;
    if (domain)
        if (!Object.hasOwnProperty.call (pointer, domain))
            return;
        else
            pointer = pointer[domain];

    if (!Object.hasOwnProperty.call (pointer, user))
        return; // no connections for user

    var clients = pointer[user];
    delete pointer[user];
    for (var client in clients) {
        var connections = clients[client];
        if (this.parent.transport.authentication.sessionCache) { // hack burrito with hack sauce
            var sessionCache = this.parent.transport.authentication.sessionCache;
            for (var i=0,j=connections.length; i<j; i++) {
                var connection = connections[i];
                sessionCache.drop (connection.session);
                connection.disconnect ('logout');
            }
        } else {
            for (var i=0,j=connections.length; i<j; i++)
                connections[i].disconnect ('logout');
        }
    }
};


module.exports = Backplane;
