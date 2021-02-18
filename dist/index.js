"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var poller_1 = require("./poller");
require('dotenv').config();
var poller = new poller_1.Poller();
//poller.saveStreams();
//poller.saveTags();
poller.updateUsers();
//# sourceMappingURL=index.js.map