submergence
===========
Client authentication and messaging backplane service API.

Application Notes
-----------------
Vital information relating to the deployment of a `submergence` environment.

###Proxy Settings
The recommended proxy is `nginx`.
 * "sticky load balancing" must be used.
 * The http header "Host" **must** be overwritten. Do not trust a client's headers!

###Database Setup
`submergence` relies on `MongoDB` and is designed to scale by sharding.
