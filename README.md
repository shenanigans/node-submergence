submergence
===========
Client authentication and messaging backplane service API.

The state of the art in realtime, and particularly in peer to peer, is that even with the best shims
easy jobs never are. "Room"-based libraries make lovely tutorials and demos but the leap from these
barely-functional user experiences to a usable, scalable social application is too complex for
anyone but the top competitive voip companies to manage.

The goal of `substation` is to bridge that gap by reversing signal flow. A user logged in to your
application no longer has to ask to be reachable, they are reachable by identity as soon as their
Socket.io connection becomes active. Connection multiplicity is embraced by shipping events to
groups of related useragents (usually multiple tabs). Robust WebRTC peer "Links" are provided that
automatically connect and reconnect new connections to the Link as long as both peers maintain at
least one connection to the server.

Whether your application is a game server, a social application, a collaborative editing tool, a
telecom service or something totally novel to Planet Earth, `substation` aims to support your
signaling requirements, at scale, out of the box.

Deployment
----------
A MongoDB cluster is required for storing session and live connection metadata.

`substation` runs on [Node.js](https://nodejs.org/) and installs with [npm](https://www.npmjs.com/).
It is configured and launched from a parent script and does not have a CLI tool. A simple, robust,
cross-platform way to keep your server running is to launch it with [forever]
(https://github.com/foreverjs/forever).
```bash
npm install --save substation
npm install -g forever
forever myApp.js
```

Like most webapp servers, `substation` must live behind a gateway server for load-balancing. The
load balancer must be "sticky" - a frequent stream of requests from the same agent must be routed to
the same service node. This is a requirement of Socket.io. `substation` also currently expects the
load-balancer to terminate `ssl` connections.

The recommended load balancer for `substation` is [nginx](http://nginx.org/). Your configuration
should contain something like this:
```
upstream myapp {
    ip_hash;
    server alfa.myapp.com;
    server sierra.myapp.com;
    server hotel.myapp.com;
}

server {
    Listen              443 ssl;
    server_name         myapp.com;
    ssl_certificate     myapp.com.crt;
    ssl_certificate_key myapp.com.key;

    location / {
        proxy_set_header    X-Real-IP $remote_addr;
        proxy_set_header    X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header    Host $http_host;
        proxy_set_header    X-NginX-Proxy true;
        proxy_http_version  1.1;
        proxy_set_header    Upgrade $http_upgrade;
        proxy_set_header    Connection "upgrade";
        proxy_redirect      off;
        proxy_pass          http://myapp/;
    }

    location /static {
        root                www/myapp;
    }
}
```

Application Notes
-----------------
Vital information relating to the deployment of a `submergence` environment.

###Proxy Settings
The recommended proxy is `nginx`.
 * "sticky load balancing" must be used.
 * The http header "Host" **must** be overwritten. Do not trust a client's headers!

###Database Setup
`submergence` relies on `MongoDB` and is designed to scale by sharding.
