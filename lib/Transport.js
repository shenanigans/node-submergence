
var url             = require ('url');
var fs              = require ('fs');
var Path            = require ('path');
var http            = require ('http');
var https           = require ('https');
var async           = require ('async');
var SocketIO        = require ('socket.io');
var ss              = require ('socket.io-stream');
var Busboy          = require ('busboy');
var Cookies         = require ('cookies');
var Likeness        = require ('likeness');
var cachew          = require ('cachew');
var uid             = require ('infosex').uid;
var filth           = require ('filth');
var Authentication  = require ('./Authentication');
var Reply           = require ('./Reply');

var actionPackValidator = new Likeness ({
    _id:            {
        '.type':        'integer',
        '.gte':         0,
        '.optional':    true
    },
    method:         {
        '.type':        'string'
    },
    path:           {
        '.type':        'string'
    },
    query:          {
        '.type':        'object',
        '.optional':    true,
        '.arbitrary':   true,
        '.all':         {
            '.type':        'string'
        }
    },
    body:           {
        '.type':        'object',
        '.optional':    true,
        '.arbitrary':   true
    }
});

var peerEventValidator = new Likeness ({
    token:  { '.type':'string', '.length':uid.length },
    to:     { '.type':'string', '.length':uid.length },
    init:   { '.type':'boolean', '.value':true, '.optional':true },
    sdp:    {
        '.optional':    true,
        type:   { '.type':'string', '.anyValue':[ 'offer', 'answer' ] },
        sdp:    { '.type':'string', '.lt':204800 }
    },
    ICE:    { '.optional':true, '.type':'string', '.lt':204800 }
});

// var standalone =
//     '<script type="text/javascript">'
//   + fs.readFileSync (Path.resolve (__dirname, '../build/bundle.js')).toString()
//   + '</script>'
//   ;

// for handling cookies during Socket.io handshake
var FauxResponse = function(){};
FauxResponse.prototype.setHeader = function(){};
FauxResponse.prototype.getHeader = function(){ return []; };


/**     @struct substation.Context
    When a template is executed, the context is seeded with several properties. These properties are
    overriden by the [configuration](substation:Action:Configuration#context) and/or [reply content]
    (substation:Action#content) of the Action.
@member/String Standalone
    The prebuilt and minified client library, compiled by [browserify](http://browserify.org/) in
    standalone mode. Any other script on the page can access the library with
    `require ('substation');`.
@member/Error|undefined ActionError
    If the Action synchronously throws an Error, it is included here.
@member/String|undefined ServerError
    When the server throws `403` or `502` within the [Transport](substation.Transport) layer, a
    short message is provided.
*/


/**     @module/class substation:Request
    Incoming Action requests, whether from `http` or `Socket.io` are mapped to a Request instance.
    If [bodySchema](.Configuration#bodySchema) and/or [querySchema](.Configuration#querySchema) are
    set, the `body` and/or `query` properties can be assumed to be conformant by the time the
    reaction Function is called.
@member/String method
    The method used to access this Action. Used when the Action is [mounted](substation#addAction)
    without a method specified.
@member/Object query
    Request url terms, GET parameters, etc. mapped as a simple flat Object and validated by the
    [query schema](.Configuration#querySchema) if specified.
@member/Array params
    If the route regex selected text after the route pattern or the route regex contains matching
    groups, the additional matched strings are passed in an Array.
@member/Object|streams.ReadableStream|undefined body
    The request body. Form requests are mapped to a simple flat Object and validated by the
    [body schema](.Configuration#bodySchema) if specified. JSON bodies are parsed and validated. If
    the [binary streams option](.Configuration#binaryStreams) is set, the body may be a [readable
    stream](streams.ReadableStream). These actions should type-check with `instanceof` and dispatch
    accordingly, or use the [never buffer option](.Configuration#neverBuffer).
@member/Array|undefined files
    When [cacheFiles](.Configuration#cacheFiles) is set, an Array of file info Objects containing
    Buffers is passed. This is part of an unstable feature.
@member/stream.Readable|undefined stream
    When [binaryStreams](.Configuration#binaryStreams) is set, content types other than lightweight
    forms and JSON will be streamed instead of cached. If you do not consume the body before
    [replying](substation.Reply#done) the incoming body stream is automatically closed with the
    'Connection:close' header.
@member/String|undefined contentType
    If the remote client has provided it, the value of the `Content-Type` header is included
    whenever `stream` is used.
*/


/**     @module/class substation.Transport
    @root
    Manages client connections over REST and Socket.io. Parses credentials and validates through
    [Authentication] (substation.Authentication). Builds [Replies](substation.Reply) and feeds them
    to [Actions](substation.Action) acquired from [Router](substation.Router).
@argument/substation parent
@argument/substation.Configuration config
*/
function Transport (parent, config) {
    this.parent = parent;
    this.config = config;
}


/**     @member/Function listen
    Begins listening on the configured port(s) and serving [Actions](substation.Action). If SSL is
    configured, keys and certificates will be loaded at this time. The server instance will be ready
    to accept requests before the callback goes off.
@callback
    @argument/Error|undefined err
*/
Transport.prototype.listen = function (port, router, callback) {
    this.authentication = new Authentication (this.parent, this.config.Authentication);
    this.createReactions (router);
    var self = this;
    this.authentication.init (function(){
        var server = http.createServer (self.reaction_REST);
        var io = SocketIO (server, {
            pingInterval:   5000,
            pingTimeout:    10000
        });
        io.use (self.handshake_SocketIO);
        io.on ('connection', self.reaction_SocketIO);
        server.listen (port, callback);
    });
};


function react404 (request, response) {
    var headers;
    if (this.config.CORS)
        headers = {
            "Access-Control-Allow-Origin":      self.config.CORS.domains,
            "Access-Control-Allow-Methods":     self.config.CORS.methods,
            "Access-Control-Allow-Headers":     self.config.CORS.headers,
            "Content-Type":                     "text/plain; charset=utf-8",
            "Content-Length":                   9,
            "Connection":                       "close"
        };
    else
        headers = {
            "Content-Type":     "text/plain; charset=utf-8",
            "Content-Length":   9,
            "Connection":       "close"
        };
    response.writeHead (code, headers);
    response.end ('not found');
}

function timeDifference (start, end) {
    var micro = Math.floor (( end[1] - start[1] ) / 1000 );
    micro += 1000000 * ( end[0] - start[0] );
    return micro;
}

Transport.prototype.createReactions = function (router) {
    var backplane = this.parent.backplane;
    var authentication = this.authentication;
    var config = this.config;
    var station = this.parent;
    var globalTemplates = this.globalTemplates;
    var linkCache = this.linkCache;

    /**     @member/Function reaction_REST

    */
    this.reaction_REST = function (request, response) {
        var startTime = process.hrtime();
        var streamClosed = false;
        var headers;
        if (config.CORS)
            headers = {
                "Access-Control-Allow-Origin":      config.CORS.domains,
                "Access-Control-Allow-Methods":     config.CORS.methods,
                "Access-Control-Allow-Headers":     config.CORS.headers
            };
        else
            headers = {};

        var action;
        var path = url.parse (request.url, true);
        var isJSON = Boolean (
            request.headers.accept
         && request.headers.accept.match (/application\/json/)
        );
        function rejectRequest (code, msg) {
            if (!streamClosed)
                headers['Connection'] = "close";

            var msgStr;
            if (isJSON) {
                headers['Content-Type'] = 'application/json; charset=utf-8';
                msgStr = JSON.stringify (msg);
            } else {
                // plain output
                headers['Content-Type'] = 'text/plain; charset=utf-8';
                msgStr = msg;
            }

            headers['Content-Length'] = Buffer.byteLength (msgStr);
            response.writeHead (code, headers);
            response.end (msgStr);
        }

        if (request.method == 'OPTIONS') {
            headers['Allow'] = Object.keys (body).join (',');
            headers['Content-Type'] = 'application/json; charset=utf-8';
            if (!streamClosed)
                headers['Connection'] = "close";

            router.getOptions (request, function (options) {
                var msgStr = JSON.stringify ({ content:options });
                headers['Content-Length'] = Buffer.byteLength (msgStr);
                response.writeHead (200, headers);
                response.end (msgStr);
            });
            return;
        }

        var requestDomain = Object.hasOwnProperty.call (request.headers, 'host') ?
            request.headers.host
          : ( config.domain || null)
          ;

        router.getAction (request, path.pathname, function (action, params) {
            if (!action) {
                rejectRequest (404, 'unknown action');
                return;
            }

            var cookies = new Cookies (request, response);
            authentication.getSession (requestDomain, path, cookies, function (err, authInfo) {
                if (err) {
                    station.logger.error ('session acquisition error', err);
                    rejectRequest (403, 'unknown authentication failure');
                    return;
                }

                var reply = new Reply (function (status, events, content, html) {
                    // every future ending point from here uses this function to close the response
                    function closeResponse (code, type, msg) {
                        station.logger.info ({
                            domain:     requestDomain,
                            transport:  'http',
                            method:     request.method,
                            path:       path.path,
                            action:     action.name,
                            status:     status,
                            latency:    timeDifference (startTime, process.hrtime()),
                            format:     isJSON ? 'json' : 'html'
                        }, 'action');

                        headers['Content-Type'] = type;
                        headers["Content-Length"] = Buffer.byteLength (msg);
                        if (reply.redirectURL)
                            headers.Location = reply.redirectURL;
                        if (!streamClosed)
                            headers.Connection = "close";

                        response.writeHead (code, headers);
                        response.end (msg);
                    }

                    if (!action.template || isJSON)
                        return closeResponse (
                            status,
                            'application/json; charset=utf-8',
                            JSON.stringify ({ events:events, content:content })
                        );

                    // assemble the template context
                    var templateContext;
                    if (config.context) {
                        templateContext = filth.clone (config.context);
                        if (action.config.context)
                            filth.merge (templateContext, action.config.context);
                        filth.merge (templateContext, content);
                    } else if (action.config.context) {
                        templateContext = filth.clone (action.config.context);
                        filth.merge (templateContext, content);
                    } else
                        templateContext = filth.clone (content);
                    // inject authentication information into the context
                    templateContext.authentication = authInfo.export();

                    // "substationEvents" boilerplate injects events into template context
                    if (events.length) {
                        var eventScript =
                            '<script type="text/javascript">(function(){'
                          + 'var substation=require("substation");substation.sendEvents('
                          + JSON.stringify (events)
                          + ');})()</script>'
                          ;
                        templateContext.SubstationEvents = eventScript;
                    }

                    if (html)
                        return closeResponse (status, 'text/html', html);

                    action.toHTML (station, status, templateContext, function (err, html) {
                        if (err)
                            return rejectRequest (502, 'rendering error');
                        closeResponse (status, 'text/html', html);
                    });
                }, function (status, stream, length, type) {
                    station.logger.info ({
                        domain:     requestDomain,
                        transport:  'http',
                        format:     isJSON ? 'json' : 'html',
                        method:     request.method,
                        path:       path.path,
                        action:     action.name,
                        status:     status,
                        latency:    timeDifference (startTime, process.hrtime())
                    }, 'action');

                    if (type)
                        headers['Content-Type'] = type;
                    if (length)
                        headers["Content-Length"] = length;
                    if (reply.redirectURL)
                        headers.Location = reply.redirectURL;
                    if (!streamClosed)
                        headers.Connection = "close";
                    response.writeHead (status, headers);
                    stream.pipe (response);
                });

                // is the Action authorized for this agent?
                if (action.config.Authentication &&
                    (
                        ( !action.config.Authentication.allowGuests && !authInfo.user )
                     || ( action.config.Authentication.isLoggedIn && !authInfo.isLoggedIn )
                     || ( action.config.Authentication.isDomestic && !authInfo.isDomestic )
                    )
                ) {
                    // not authorized
                    reply.done (403);
                    return;
                }

                // build up the request object
                queryDoc = path.query || {};
                delete queryDoc._domestic;
                var actionRequest = {
                    transport:  'http',
                    format:     'html',
                    method:     request.method,
                    query:      queryDoc,
                    params:     params || [],
                    domain:     requestDomain
                };

                // reject body
                if (action.rejectBody) {
                    action.run (station, authInfo, actionRequest, reply);
                    return;
                }

                // binary streams
                var contentType = request.headers['content-type'];
                if (
                    action.binaryStreams
                 && contentType != 'application/json'
                 && contentType != 'application/x-www-form-urlencoded'
                ) {
                    actionRequest.stream = request;
                    actionRequest.contentType = contentType;

                    // when this stream closes, disarm the connection terminator
                    request.on ('end', function(){
                        streamClosed = true;
                    });

                    action.run (station, authInfo, actionRequest, reply);
                    return;
                }

                // pass form requests over to busboy
                if (
                    contentType == 'application/x-www-form-urlencoded'
                 || contentType == 'multipart/form-data'
                ) {
                    var boy = new Busboy ({
                        headers:    request.headers,
                        limits:     {

                        }
                    });

                    var body = {};
                    boy.on ('field', function (key, value) {
                        body[key] = value;
                    });

                    var files = [];
                    boy.on ('file', function (key, stream, filename, encoding, mimetype) {
                        var fileDoc = {
                            filename:       filename,
                            encoding:       encoding,
                            contentType:    mimetype
                        };
                        files.push (fileDoc);

                        var chunks = [];
                        stream.on ('data', function (chunk) {
                            chunks.push (chunk);
                        });
                        stream.on ('end', function(){
                            fileDoc.data = Buffer.concat (chunks);
                        });
                    });

                    boy.on ('finish', function(){
                        actionRequest.body = body;
                        actionRequest.files = files;

                        action.run (station, authInfo, actionRequest, reply);
                    });

                    boy.on ('error', function (err) {
                        station.logger.warn ('form parsing error', {
                            path:   action.route,
                            method: action.method,
                            status: status
                        });
                        return closeRequest (400, { ClientError:'malformed request' });
                    });
                    request.pipe (boy);
                    return;
                }

                // buffer and parse the body
                var bodyChunks = [];
                var total = 0;
                request.on ('data', function (buf) {
                    total += buf.length;
                    if (total > action.maxBodyLength)
                        return closeRequest (413, { ClientError:'request entity too large' });
                    bodyChunks.push (buf);
                });

                request.on ('end', function(){
                    streamClosed = true;
                    if (bodyChunks.length) {
                        var fullBody = Buffer.concat (bodyChunks);

                        // if (contentType == 'application/json') {
                            var reqStr = fullBody.toString();
                            try {
                                actionRequest.body = JSON.parse (reqStr);
                            } catch (err) {
                                return closeRequest (400, { ClientError:'malformed request' });
                            }
                        // } else actionRequest.body = fullBody;
                    }
                    action.run (station, authInfo, actionRequest, reply);
                });

                request.on ('error', function (err) {
                    logger.warn ({
                        url:        request.url,
                        ip:         request.connection.remoteAddress,
                        headers:    request.headers,
                        action:     actionName
                    }, 'action failed to send body');
                });
            });
        });
    };


    /**     @member/Function handshake_SocketIO

    */
    this.handshake_SocketIO = function (socket, callback) {
        var cookies = new Cookies (socket.request, new FauxResponse());
        var path = url.parse (socket.request.url, true);
        socket.requestDomain = Object.hasOwnProperty.call (socket.request.headers, 'host') ?
            socket.request.headers.host
          : ( config.domain || null)
          ;

        authentication.getSession (socket.requestDomain, path, cookies, function (err, agent) {
            if (err) return callback (err);
            if (!agent.isLoggedIn || (!config.allowForeignSockets && !agent.isDomestic))
                return callback (new Error ('not authorized'));

            socket.agent = agent;
            socket.session = cookies.get ('session');

            uid.craft (function (SID) {
                socket.SID = SID;
                backplane.setLive (
                    socket.requestDomain,
                    agent.user,
                    agent.client,
                    socket,
                    true
                );
                socket.on ('disconnect', function(){
                    backplane.setLive (
                        socket.requestDomain,
                        agent.user,
                        agent.client,
                        socket,
                        false
                    );
                });
                callback();
            });
        });
    };


    /**     @member/Function reaction_SocketIO

    */
    this.reaction_SocketIO = function (socket, callback) {
        var initReply = new Reply (function (status, events, content) {
            if (!events) return;
            socket.emit ('reply', { status:200, events:events });
        });
        station.emit ('liveConnection', socket.requestDomain, socket.agent, initReply);

        socket.on ('action', function (actionDoc) {
            var startTime = process.hrtime();
            try {
                actionPackValidator.validate (actionDoc);
            } catch (err) {
                // invalid request pack
                station.logger.warn ('invalid action', { transport: 'Socket.io' });
                return;
            }

            if (actionDoc.method == 'OPTIONS') {
                station.router.getOptions (actionDoc, function (options) {
                    var response = { status:200, path:path, content:options };
                    if (actionDoc._id)
                        response._id = actionDoc._id;
                    socket.emit ('reply', response);
                });
                return;
            }

            router.getAction (actionDoc, actionDoc.url, function (action, params) {
                if (!action) {
                    // reply 404
                    station.logger.info ({
                        domain:     socket.requestDomain,
                        transport:  'socket.io',
                        method:     actionDoc.method,
                        path:       path,
                        action:     null,
                        status:     404,
                        latency:    timeDifference (startTime, process.hrtime())
                    }, 'action');
                    return;
                }

                var reply = new Reply (function (status, events, content) {
                    var replyDoc = { status:status };
                    if (events)
                        replyDoc.events = events;
                    if (content)
                        replyDoc.content = content;
                    if (actionDoc._id !== undefined)
                        replyDoc._id = actionDoc._id;

                    station.logger.info ({
                        domain:     socket.requestDomain,
                        transport:  'socket.io',
                        method:     actionDoc.method,
                        path:       path,
                        action:     action.name,
                        status:     status,
                        latency:    timeDifference (startTime, process.hrtime())
                    }, 'action');

                    socket.emit ('reply', replyDoc);
                }, function (status, stream) {
                    ss(socket).emit ('reply', stream, { status:status });
                });

                action.run (
                    station,
                    socket.agent,
                    {
                        domain:     socket.requestDomain,
                        transport:  'socket.io',
                        method:     actionDoc.method,
                        query:      actionDoc.query || {},
                        params:     params,
                        body:       actionDoc.body || {}
                    },
                    reply
                );
            })
        });

        socket.on ('link', function (query) {
            var startTime = process.hrtime();
            // pass to the application for processing
            station.emit (
                'peerRequest',
                socket.requestDomain,
                socket.agent,
                query,
                function (/* userID, clientID, aliceInfo */) {
                    var userID, clientID, aliceInfo;
                    userID = arguments[0];
                    if (arguments.length == 2)
                        aliceInfo = arguments[1];
                    else {
                        clientID = arguments[1];
                        aliceInfo = arguments[2];
                    }
                    var bobDef = {
                        domain: socket.requestDomain,
                        query:  query,
                        user:   userID
                    };
                    if (clientID)
                        bobDef.client = clientID;
                    var aliceDef = {
                        domain: socket.requestDomain,
                        query:  aliceInfo,
                        user:   socket.agent.user
                    };
                    // user -> user or client -> client, no mixed links
                    if (clientID)
                        aliceDef.client = socket.agent.client;

                    var tryNum = 1;
                    uid.craft (function writeLink (token) {
                        if (tryNum++ > 3)
                            return;

                        function cleanup (err, didReceive) {
                            if (didReceive) return;

                            // nobody awake to connect to link
                            // cull the link record and subdoc
                            station.logger.warn ('Link failed', {
                                token:      token,
                                sender:     aliceDef,
                                receiver:   bobDef
                            });
                            station.LinksCollection.update (
                                { _id:token },
                                { $set:{ closed:true } },
                                { w:0 }
                            );
                            station.BackplaneCollection.update (
                                { _id:userID },
                                { $pull:{ link:{ token:token } } },
                                { w:0 }
                            );
                            station.BackplaneCollection.update (
                                { _id:socket.agent.user },
                                { $pull:{ link:{ token:token } } },
                                { w:0 }
                            );
                        }

                        // find the Alice user IFF they don't have a link already selected
                        station.BackplaneCollection.findAndModify (
                            { user:socket.agent.user, domain:socket.requestDomain, link:{ $not:{ $elemMatch:{
                                client:     socket.agent.client,
                                tgtUser:    userID,
                                tgtClient:  clientID
                            } } } },
                            { user:1 },
                            { $push:{ link:{
                                token:      token,
                                client:     clientID ? socket.agent.client : null,
                                tgtUser:    userID,
                                tgtClient:  clientID
                            } } },
                            { fields:{ _id:true } },
                            function (err, rec) {
                                if (err) return callback (err);
                                if (!rec) {
                                    // they DO have a link already selected
                                    // abandon token and use existing Link
                                    station.BackplaneCollection.findOne (
                                        {
                                            domain: socket.requestDomain,
                                            user:   socket.agent.user,
                                            link:   { $elemMatch:{
                                                client:     socket.agent.client,
                                                tgtUser:    userID,
                                                tgtClient:  clientID
                                            } }
                                        },
                                        { 'link.$':true },
                                        function (err, rec) {
                                            if (err) return callback (err);
                                            if (!rec || !rec.link || !rec.link.length)
                                                return writeLink (token);
                                            station.LinksCollection.update (
                                                { _id:rec.link[0].token },
                                                { $set:{ closed:false } },
                                                function (err) {
                                                    var peerEvent = {
                                                        init:   true,
                                                        token:  rec.link[0].token,
                                                        query:  aliceInfo,
                                                        from:   socket.SID
                                                    };
                                                    station.backplane.routePeerEvent (
                                                        peerEvent,
                                                        socket.agent,
                                                        function (err) {
                                                            if (err) station.logger.error (
                                                                'peer event error',
                                                                err
                                                            );
                                                        }
                                                    );
                                                }
                                            );
                                        }
                                    );
                                    return;
                                }

                                // new Link accepted, finish database updates
                                async.parallel ([
                                    function (callback) {
                                        station.logger.info ({
                                            token:      token,
                                            sender:     {
                                                user:       aliceDef.user,
                                                client:     aliceDef.client,
                                            },
                                            receiver:   {
                                                user:       bobDef.user,
                                                client:     bobDef.client,
                                            },
                                            latency:    timeDifference (startTime, process.hrtime())
                                        }, 'link opened');
                                        station.LinksCollection.insert ({
                                            _id:        token,
                                            party:      [ aliceDef, bobDef ],
                                            closed:     false
                                        }, callback);
                                    },
                                    function (callback) {
                                        station.BackplaneCollection.update (
                                            { domain:socket.requestDomain, user:userID },
                                            { $push:{ link:{
                                                client:     clientID,
                                                token:      token,
                                                tgtUser:    socket.agent.user,
                                                tgtClient:  clientID ? socket.agent.client : null
                                            } } },
                                            { upsert:true, w:0 },
                                            callback
                                        );
                                    }
                                ], function (err) {
                                    if (err)
                                        return;

                                    var peerEvent = {
                                        init:   true,
                                        token:  token,
                                        query:  aliceInfo,
                                        from:   socket.SID
                                    };
                                    station.backplane.sendPeerEvent (
                                        socket.requestDomain,
                                        userID,
                                        clientID,
                                        peerEvent,
                                        undefined,
                                        cleanup
                                    );
                                });
                            }
                        );
                    });
                }
            );
        });

        socket.on ('peer', function (info) {
            if (!socket.agent.isLoggedIn) {
                socket.emit ('peer', { error:'FORBIDDEN' });
                return;
            }

            if ((function(){
                try {
                    peerEventValidator.validate (info);
                } catch (err) {
                    // invalid peer event message
                    socket.emit ('peer', { error:'INVALID' });
                    return true;
                }
            })())
                return;
            if (( info.sdp || info.ICE ) && !info.to) {
                socket.emit ('peer', { token:info.token, error:'INVALID' });
                return;
            }

            function cleanup (err, received) {
                if (err || !received)
                    station.emit ('peer', { token:info.token, error:'OFFLINE' });
            }

            // evaluate the token for pass-through service
            var passDoc = peerEventValidator.transform ({}, info);
            passDoc.from = socket.SID;
            station.backplane.routePeerEvent (passDoc, socket.agent, function (err) {
                if (err)
                    station.logger.error ('peer event routing error', err);
            });
        });
    };
};


module.exports = Transport;
