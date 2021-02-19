import { Poller } from "./poller";
import * as fs from "fs";

require('dotenv').config();

const poller = new Poller();

let tagsPromise = poller.saveTags();

poller.saveStreams().then(async () => {
    await poller.updateUsers();
    await tagsPromise;

    process.exit(0);
});