
var url = require ('url');
var nssocket = require ('nssocket');
var cachew = require ('cachew');
var sexxion = require ('infosex').session;
var filth = require ('filth');
var Agent = require ('./Agent');


/**     @module/class submergence:Authentication
    @root
    Looks up, creates and manages sessions on the database as well as managing browser cookies.
@argument/submergence parent
@argument/:Configuration config
*/
function Authentication (parent, config) {
    this.parent = parent;
    this.config = filth.clone (DEFAULT_CONFIG);
    if (config)
        filth.merge (this.config, config);
}


/**     @submodule/class Configuration
    Options for authenticating users and keeping them authentic (or not).
@member/mongodb.Collection|undefined SessionsCollection
    Optionally override MongoDB setup however you want by passing in a pre-configured Collection
    driver instance.
@member/Number|Boolean cacheSessions
    Maximum number of sessions to cache in memory, or Boolean false to disable. Setting to `0` also
    disables the cache.
@member/String sessionsCollectionName
    @default `"Sessions"`
    Name of the MongoDB collection used to store session records. Ignored when using
    `SessionsCollection`.
@member/Number sessionCacheTimeout
    @default `thirty minutes`
    Maximum time (milliseconds) to cache a session without confirming its database record. Setting
    to `0` disables cache timeouts entirely. Calling [setIdle](#setIdle) or
    [logout](.logout) will distribute the session update to affected server
    instances, so the cache timeout is just a safety net.
@member/Number sessionLifespan
    @default `one day`
    Maximum time (milliseconds) that a session token remains valid after it is created. This timeout
    produces a fresh token without interrupting the active login.
@member/Number sessionRenewalTimeout
    @default `three days`
    Maximum time (milliseonds) since the user's last period of activity until a new session token
    can no longer be generated. The Client will be `idle` until [setActive](.Agent#setActive) is
    called.
@member/Number loginLifespan
    @default `two weeks`
    Maximum time (milliseconds) since the user's last [login event](.Agent#setActive) until their
    active session ends and cannot be renewed. Set to `0` (or another untruthy value) to disable,
    allowing a sufficiently active session to remain logged in until time stops.
@member/Number cookieLifespan
    @default `one year`
    When the "remember me" flag is set, cookies are saved for this duration, in milliseconds. The
    Client will be [idle](.Agent#setIdle) for as long as it retains this cookie.
*/
var DEFAULT_CONFIG = {
    cacheSessions:          100000,
    sessionsCollectionName: "Sessions",
    sessionCacheTimeout:    1000 * 60 * 30, // thirty minutes
    sessionLifespan:        1000 * 60 * 60 * 24, // one day
    sessionRenewalTimeout:  1000 * 60 * 60 * 24 * 3, // three days
    loginLifespan:          1000 * 60 * 60 * 24 * 7 * 2, // two weeks
    cookieLifespan:         1000 * 60 * 60 * 24 * 365 // one year
}


/**     @member/Function init
    When clustering, prepares the cluster port for authentication requests from other processes.
    Otherwise, `Authentication` doesn't require setup and simply nextTick's the callback.
@callback
*/
Authentication.prototype.init = function (callback) {
    if (this.config.cacheSessions)
        this.sessionCache = new cachew.ChainCache (
            this.config.cacheSessions,
            this.config.sessionCacheTimeout
        );

    return process.nextTick (callback);
};


/**     @member/Function setActive
    @development
    Create a new valid session and give the user some cookies to authenticate it.
@argument/String user
@argument/String client
@argument/Boolean rememberMe
@argument/cookies cookies
@callback
*/
Authentication.prototype.setActive = function (domain, user, client, rememberMe, cookies, callback) {
    var config = this.config;
    var sessionCache = this.sessionCache;
    var SessionsCollection = this.parent.SessionsCollection;
    sexxion.craft (function (newSession, domesticate) {
        var now = (new Date()).getTime();
        var newRecord = {
            _id:    newSession,
            c:      now,        // created
            a:      now,        // activeTime
            v:      true,       // isValid
            D:      domain,     // domain ID
            U:      user,       // userID
            C:      client,     // clientID
            l:      null,       // lastSession
            f:      newSession, // firstSession
            L:      now,        // loginTime
            r:      rememberMe, // "remember me"
        };
        SessionsCollection.insert (newRecord, { w:1 }, function (err) {
            if (err) return callback (err);

            if (rememberMe) {
                cookies.set (
                    'session',
                    newSession,
                    { httpOnly:true, maxAge:config.cookieLifespan }
                );
                cookies.set (
                    'domestic',
                    domesticate,
                    { httpOnly:false, maxAge:config.cookieLifespan }
                );
            } else {
                cookies.set ('session', newSession, { httpOnly:true });
                cookies.set ('domestic', domesticate, { httpOnly:false });
            }
            // delete the loggedOut cookie, if present
            cookies.set ('loggedOut');

            if (sessionCache)
                sessionCache.set (newSession, newRecord);

            callback (undefined, newRecord);
        });
    });
};


/**     @member/Function setIdle
    @development
    End the current session, converting the user to `idle` status. If there is no current session,
    a pre-expired session is created and the user is given its authentication cookies.
@argument/String user
@argument/String client
@argument/cookies cookies
@callback
*/
Authentication.prototype.setIdle = function (domain, user, client, rememberMe, cookies, callback) {
    var config = this.config;
    var sessionCache = this.sessionCache;
    var backplane = this.parent.backplane;
    var SessionsCollection = this.parent.SessionsCollection;

    // create a new, pre-expired session
    var config = this.config;
    var sessionCache = this.sessionCache;
    sexxion.craft (function (newSession, domesticate) {
        var now = (new Date()).getTime();
        var newRecord = {
            _id:    newSession,
            c:      now,        // created
            a:      now,        // activeTime
            v:      false,      // isValid
            D:      domain,     // domain ID
            U:      user,       // userID
            C:      client,     // clientID
            l:      null,       // lastSession
            f:      null,       // firstSession
            L:      null,       // loginTime
            r:      rememberMe
        };
        SessionsCollection.insert (newRecord, function (err) {
            if (err) return callback (err);

            if (rememberMe) {
                cookies.set (
                    'session',
                    newSession,
                    { httpOnly:true, maxAge:config.cookieLifespan }
                );
                cookies.set (
                    'domestic',
                    domesticate,
                    { httpOnly:false, maxAge:config.cookieLifespan }
                );
            } else {
                cookies.set ('session', newSession, { httpOnly:true });
                cookies.set ('domestic', domesticate, { httpOnly:false });
            }

            if (sessionCache)
                sessionCache.set (newSession, newRecord);

            backplane.kick (domain, user, client, function (err) {
                if (err) {
                    logger.error ('failed to kick user from Backplane', err);
                    return callback (new Error ('internal error'));
                }
                callback();
            });
        });
    });
};


/**     @member/Function logout
    @development
    End the current session. If the "remember me" flag was not set, the client is asked to delete
    their cookies.
@argument/String user
@argument/String client
@argument/cookies cookies
@callback
*/
Authentication.prototype.logout = function (domain, user, client, cookies, callback) {
    var config = this.config;
    var backplane = this.parent.backplane;
    var currentSession = cookies.get ('session');
    if (!currentSession)
        return callback();
    if (this.sessionCache)
        this.sessionCache.drop (currentSession);

    var query = { D:domain, U:user };
    if (client)
        query.C = client;

    this.parent.SessionsCollection.update (
        query,
        { $set:{ v:false } },
        { multi:true },
        function (err) {
            if (err) return callback (err);
            backplane.kick (domain, user, client, function (err) {
                if (err) {
                    logger.error ('failed to kick user from Backplane', err);
                    return callback (new Error ('internal error'));
                }
                callback();
            });
        }
    );
};


/**     @member/Function getSession

@argument/Object path
    `request.url` preparsed by the [url module](url.parse). Just an efficiency hack to avoid
    reparsing.
@argument/http.IncomingMessage request
@argument/http.ServerResponse response
@callback
    @argument/Error|undefined err
    @argument/submergence:Agent auth
*/
Authentication.prototype.getSession = function (domain, path, cookies, callback) {
    var session = cookies.get ('session');
    var confirm = path.query._domestic;
    var isDomestic = false;
    var config = this.config;
    var self = this;

    if (!session)
        return callback (undefined, new Agent (this, cookies, domain));

    // cached session?
    var sessionRecord;
    if (this.sessionCache)
        sessionRecord = this.sessionCache.get (session);
    if (sessionRecord) {
        if (path.query._domestic) {
            var sessionInfo = sexxion.parse (session);
            if (sessionInfo.domesticate != path.query._domestic)
                return callback (undefined, new Agent (this, cookies, domain));
            isDomestic = true;
        }

        var now = (new Date()).getTime();
        // login event forced invalid or past hard timeout?
        if (!sessionRecord.v || now - sessionRecord.L >= config.loginLifespan)
            return callback (undefined, new Agent (
                this,
                cookies,
                domain,
                sessionRecord.U,
                sessionRecord.C,
                false,
                isDomestic,
                sessionRecord.r
            ));

        // still in date?
        if (now - sessionRecord.c < config.sessionLifespan)
            return callback (undefined, new Agent (
                this,
                cookies,
                domain,
                sessionRecord.U,
                sessionRecord.C,
                true,
                isDomestic,
                sessionRecord.r
            ));

        // fresh enough to renew?
        if (now - sessionRecord.a < config.sessionRenewalTimeout)
            return this.renewSession (domain, sessionRecord, cookies, function (err, newSession) {
                if (err) return callback (err);

                callback (undefined, new Agent (
                    self,
                    cookies,
                    domain,
                    sessionRecord.U,
                    sessionRecord.C,
                    true,
                    isDomestic,
                    sessionRecord.r
                ));
            });

        // expired session
        return callback (undefined, new Agent (
            this,
            cookies,
            domain,
            sessionRecord.U,
            sessionRecord.C,
            false,
            isDomestic,
            sessionRecord.r
        ));
    }

    // fetch session record from database, if able
    this.parent.SessionsCollection.findOne ({ _id:session, D:domain }, function (err, sessionRecord) {
        if (err)
            return callback (err);
        if (!sessionRecord) // invalid session
            return callback (undefined, new Agent (self, cookies, domain));

        var now = (new Date()).getTime();

        if (path.query._domestic) {
            var sessionInfo = sexxion.parse (session);
            if (sessionInfo.domesticate != path.query._domestic)
                return callback (undefined, new Agent (self, cookies, domain));
            isDomestic = true;
        }

        // login event forced invalid or past hard timeout?
        if (!sessionRecord.v || now - sessionRecord.L >= config.loginLifespan)
            return callback (undefined, new Agent (
                self,
                cookies,
                domain,
                sessionRecord.U,
                sessionRecord.C,
                false,
                isDomestic,
                sessionRecord.r
            ));

        // still in date?
        if (now - sessionRecord.c < config.sessionLifespan)
            return callback (undefined, new Agent (
                self,
                cookies,
                domain,
                sessionRecord.U,
                sessionRecord.C,
                true,
                isDomestic,
                sessionRecord.r
            ));

        // fresh enough to renew?
        if (now - sessionRecord.a < config.sessionRenewalTimeout)
            return self.renewSession (domain, sessionRecord, cookies, function (err, newSession) {
                if (err) return callback (err);

                callback (undefined, new Agent (
                    self,
                    cookies,
                    domain,
                    sessionRecord.U,
                    sessionRecord.C,
                    true,
                    isDomestic,
                    sessionRecord.r
                ));
            });

        // expired session
        return callback (undefined, new Agent (
            self,
            cookies,
            domain,
            sessionRecord.U,
            sessionRecord.C,
            false,
            isDomestic,
            sessionRecord.r
        ));
    });
};


/**     @member/Function renewSession
    @development

*/
Authentication.prototype.renewSession = function (domain, session, cookies, callback) {
    var config = this.config;
    var sessionCache = this.sessionCache;
    var SessionsCollection = this.parent.SessionsCollection;
    sexxion.craft (function (newSession, domesticate) {
        var newRecord = {
            _id:    newSession,
            c:      (new Date()).getTime(),
            a:      session.a,      //
            U:      session.U,      // User ID
            C:      session.C,      // Client ID
            l:      session._id,    // previous session in chain
            f:      session.f,      // first session in chain
            L:      session.L,      // timestamp of session chain initial login
            r:      session         //
        };
        SessionsCollection.insert (newRecord, function (err) {
            if (err) return callback (err);

            if (session.r) {
                cookies.set (
                    'session',
                    newSession,
                    { httpOnly:true, maxAge:config.cookieLifespan }
                );
                cookies.set (
                    'domestic',
                    domesticate,
                    { httpOnly:false, maxAge:config.cookieLifespan }
                );
            } else {
                cookies.set ('session', newSession, { httpOnly:true });
                cookies.set ('domestic', domesticate, { httpOnly:false });
            }

            if (sessionCache)
                sessionCache.set (newSession, newRecord);

            callback (undefined, newRecord);
        });
    });
};


module.exports = Authentication;
