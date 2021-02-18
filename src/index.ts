import { Poller } from "./poller";

require('dotenv').config();

const poller = new Poller();

//poller.saveStreams();
//poller.saveTags();
poller.updateUsers();