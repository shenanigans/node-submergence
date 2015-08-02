
var uid = require ('infosex').uid.craft;

/**     @module/class substation:Agent
    @root
    Central controller for viewing or changing authentication status during an Action. An Agent that
    is logged into the application will *always* have both a `user` and `client` id configured. The
    minimum qualifications for an Agent may already be [configured]
    (submergence:Action:Configuration#Authentication) for the Action.
@property/String|undefined user
    ID String of the current user, if any.
@property/String|undefined client
    ID String of the current client, if any.
@property/Boolean isLoggedIn
    Whether the user is currently logged in. An Action may occur with a `user` and `client`
    without being an active session - this user presented a previously active session which
    has not received any security flags.
@property/Boolean isDomestic
    Whether the user copied their "domestication cookie" into the request to confirm the
    request has same-origin priveleges for this domain. In a high-security app, this confirmation is
    absolutely critical for any secure Action as it will prevent an XSS vulnerability in another
    domain from affecting your users. The chief downside is that domestic requests cannot be
    generated without using javascript.
@property/Number|undefined sessionCreated
    Epoch time that the user's current session (if any) was initialized. This refers to a
    [login](substation.login) event, not an automatic session key change.
*/
function Agent (parent, cookies, domain, user, client, isLoggedIn, isDomestic, rememberMe) {
    this.parent = parent;
    this.cookies = cookies;
    this.domain = domain;

    if (arguments.length == 4) {
        this.user = user.user;
        this.client = user.client;
        this.isLoggedIn = user.isLoggedIn;
        this.isDomestic = user.isDomestic;
        this.rememberMe = user.rememberMe;
    } else {
        this.user = user;
        this.client = client;
        this.isLoggedIn = Boolean (isLoggedIn);
        this.isDomestic = Boolean (isDomestic);
        this.rememberMe = Boolean (rememberMe);
    }
}


/**     @member/Function setActive

*/
Agent.prototype.setActive = function (/* user, client, rememberMe, callback */) {
    var user, client, rememberMe, callback;
    switch (arguments.length) {
        case 1:
            callback    = arguments[0];
            break;
        case 2:
            rememberMe  = arguments[0];
            callback    = arguments[1];
            break;
        case 3:
            user        = arguments[0];
            client      = arguments[1];
            callback    = arguments[2];
            break;
        default:
            user        = arguments[0];
            client      = arguments[1];
            rememberMe  = arguments[2];
            callback    = arguments[3];
    }

    if (user)
        this.user = user;
    if (client)
        this.client = client;

    if (!this.client || !this.user)
        return process.nextTick (function(){
            callback (new Error ('user and client ID required'))
        });

    this.parent.setActive (this.domain, this.user, this.client, rememberMe, this.cookies, callback);
};


/**     @member/Function setIdle

*/
Agent.prototype.setIdle = function (/* user, client, rememberMe, callback */) {
    var user, client, rememberMe, callback;
    switch (arguments.length) {
        case 1:
            callback    = arguments[0];
            break;
        case 2:
            rememberMe  = arguments[0];
            callback    = arguments[1];
            break;
        case 3:
            user        = arguments[0];
            client      = arguments[1];
            callback    = arguments[2];
            break;
        default:
            user        = arguments[0];
            client      = arguments[1];
            rememberMe  = arguments[2];
            callback    = arguments[3];
    }

    if (user)
        this.user = user;
    if (client)
        this.client = client;

    if (!this.client || !this.user)
        return process.nextTick (function(){
            callback (new Error ('user and client ID required'))
        });

    this.parent.setIdle (this.domain, this.user, this.client, rememberMe, this.cookies, callback);
};


/**     @member/Function logout

*/
Agent.prototype.logout = function (/* client, callback */) {
    var client, callback;
    if (arguments.length == 1)
        callback = arguments[0];
    else {
        client = arguments[0];
        callback = arguments[1];
    }
    if (!this.client || !this.user)
        return process.nextTick (callback);

    // local setup
    this.isLoggedIn = false;
    this.isDomestic = false;
    var user = this.user;
    delete this.user;
    delete this.client;

    this.parent.logout (this.domain, user, client, this.cookies, callback);
};


/**     @member/Function export

*/
Agent.prototype.export = function(){
    return {
        user:           this.user,
        client:         this.client,
        isLoggedIn:     this.isLoggedIn,
        isDomestic:     this.isDomestic
    };
};

module.exports = Agent;
