'use strict';
/**
 * ## All the routes are joined
 *
 */
var  ApiRoutes = require('./routes/endpoints');

var internals = {};

//Concatentate the routes into one array
internals.routes = [].concat(ApiRoutes.endpoints
                             );

//set the routes for the server
internals.init = function (server) {
  server.route(internals.routes);
};

module.exports = internals;
