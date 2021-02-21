import { Poller } from "./poller";
import * as fs from "fs";

require('dotenv').config();

const poller = new Poller();

let tagsPromise = poller.saveTags();

poller.saveStreams().then(async () => {
    let usersPromise = poller.updateUsers();
    let gamesPromise = (async function () {
        await poller.updateGames();
        await poller.updateGamesPlayerCount();
    })();

    await Promise.all([tagsPromise, usersPromise, gamesPromise]);

    process.exit(0);
});