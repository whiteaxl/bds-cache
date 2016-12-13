'use strict';

var moment = require("moment");

var internals = {};

internals.warn = function(msg, msg2, msg3) {
	internals.doLog("[WARN] ", msg, msg2, msg3);
};

internals.error = function(msg, msg2, msg3) {
	internals.doLog("[ERROR] ", msg, msg2, msg3);
};


internals.info = function(msg, msg2, msg3) {
	internals.doLog("[INFO] ", msg, msg2, msg3);
};

internals.enter = function(msg, msg2, msg3) {
	internals.doLog("[ENTER] ", msg, msg2, msg3);
};

internals.doLog = function(type, msg, msg2, msg3) {
	type = "[" + moment().format('h:mm:ss.SSS') + "]" + type;

	if (msg3) {
		console.log(type, msg, msg2, msg3);
	} else if (msg2) {
			console.log(type, msg, msg2);
	} else {
		console.log(type, msg);
	}
};



module.exports = internals;