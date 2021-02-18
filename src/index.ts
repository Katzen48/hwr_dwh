import { Poller } from "./poller";

require('dotenv').config();

const poller = new Poller();

(async function () {
    await poller.saveStreams();
})();