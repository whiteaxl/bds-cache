'use strict';

import Hapi from 'hapi';
import Routes from './routes';
import DBCache from './DBCache';


const server = new Hapi.Server();

server.connection( {
  port: 8082
});

Routes.init(server);

server.start(err => {
  if (err) {
    console.error( 'Error was handled!', err );
  }

  DBCache.reloadAds(() => {
    console.log("Done reloadAds!");
  });

  console.log( `Server started at ${ server.info.uri }` );
});