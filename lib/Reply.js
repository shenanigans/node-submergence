
var filth = require ('filth');

function timeDifference (start, end) {
    var micro = Math.floor (( end[1] - start[1] ) / 1000 );
    micro += 1000000 * ( end[0] - start[0] );
    return micro;
}

/**     @class substation.Action.Reply
    @root
    Accumulates information and events to send to the client.
@callback exporter
    Receives information to ship to the user after [done](#done) is called.
    @argument/Number status
    @argument/Object info
*/
function Reply (exporter, streamExporter, latencies) {
    this.events = [];
    this.exporter = exporter;
    this.streamExporter = streamExporter;
    this.latencyTime = process.hrtime();
    this.latencies = latencies || {};
}


/**     @member/Function latency
    Log a latency number for this Reply relative to its start time. Latencies can **not** be
    overwitten.
returns/Boolean written
    Whether a latency value was stored. `false` when a name conflict occurs.
*/
Reply.prototype.latency = function (name) {
    if (Object.hasOwnProperty.call (this.latencies, name))
        return false;
    var now = process.hrtime();
    this.latencies[name] = timeDifference (this.latencyTime, now);
    this.latencyTime = now;
    return true;
};


/**     @member/Function event
    Fire an event on the client. May fire any number of the same event.
@argument/String name
@argument content
    Any number of additional arguments containing JSON-serializable data to ship as arguments to the
    triggered event.
*/
Reply.prototype.event = function(){
    if (this.isClosed)
        return;
    if (!this.events)
        this.events = [ Array.apply ([], arguments) ];
    else
        this.events.push (Array.apply ([], arguments));
};


/**     @member/Function event
    Fire multiple events on the client. May fire any number of the same event.
@argument/String name
@argument content
    Any number of additional arguments containing JSON-serializable data to ship as arguments to the
    triggered event.
*/
Reply.prototype.sendEvents = function (events) {
    if (this.isClosed)
        return;
    var eventsClone = events.map (function (item) { return Array.apply ([], item); });
    if (!this.events)
        this.events = eventsClone;
    else
        this.events.push.apply (eventsClone);
};


/**     @member/Function content
    Set content information for this reply. If called multiple times, the current content is deep
    copied, then additional content is deep merged in.
@throws/Error ClosedError
    More information cannot be written to the reply once [done](#done) is called.
@argument data
    Content information to send.
*/
Reply.prototype.content = function (data) {
    if (this.isClosed)
        return;
    if (!this.contentData)
        this.contentData = data;
    else {
        if (!this.wasCloned) {
            this.contentData = filth.clone (this.contentData);
            this.wasCloned = true;
        }
        filth.merge (this.contentData, data);
    }
};


/**     @member/Function html

*/
Reply.prototype.html = function (html) {
    if (this.isClosed)
        return;
    if (this.contentHTML)
        this.contentHTML += html;
    else
        this.contentHTML = html;
};


/**     @member/Function stream
    Close the response with a [Stream](streams:ReadableStream) that will be piped to the client.
    Rejects any other content added to the reply and prevents any more content from being added.
*/
Reply.prototype.stream = function (status, stream, length, type) {
    if (this.isClosed)
        return;
    this.isClosed = true;

    status = status || this.status || '200';
    this.streamExporter (status, stream, length, type);
};



/**     @member/Function done
    Send information and events to the client. Closes the reply to further content. Same as
    `close()`.
@argument/Number|String status
    @optional
    Specify a status code other than 200.
*/
Reply.prototype.done = function (status) {
    status = status || this.status || 200;
    if (this.isClosed)
        return;
    this.isClosed = true;

    this.exporter (String (status), this.events, this.contentData || {}, this.contentHTML);
};


/**     @member/Function close
    Send information and events to the client. Closes the reply to further content. Same as
    `done()`.
*/
Reply.prototype.close = Reply.prototype.done;


/**     @member/Function redirect
    Set the `Redirect` header to a target url and inject an event called `redirect` passing the
    target url as an argument.
@argument/String targetURL
    This value is either set to the `location` header to redirect a browser client or passed to the
    `redirect` event on the application client.
*/
Reply.prototype.redirect = function (targetURL) {
    this.redirectURL = targetURL;
    this.status = 302;
};


/**     @member/Function clear
    Abandon accumulated information and events. Start over with an empty reply. No longer available
    once [done](#done) has been called.
*/
Reply.prototype.clear = function(){
    if (this.isClosed)
        throw new Error ('reply is already closed');
    this.wasCloned = false;
    this.events = [];
    delete this.content;
};


module.exports = Reply;
