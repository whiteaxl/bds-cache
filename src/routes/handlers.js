"use strict";

var internals = {};

internals.hello = function(request, reply ) {
  reply( 'Hello World!' );
};

module.exports = internals;