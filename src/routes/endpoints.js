import handlers from "./handlers";

var internals = {};

internals.endpoints = [
  {
    method: 'GET',
    path: '/api/hello',
    handler: handlers.hello,
    config: {
      description: 'Test',
      tags: ['api']
    }
  }
];

module.exports = internals;