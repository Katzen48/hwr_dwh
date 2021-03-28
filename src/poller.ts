import {ApiClient, HelixStream, HelixTag, HelixUser} from "twitch";
import { ClientCredentialsAuthProvider } from 'twitch-auth';
import IGDB from 'igdb-api-node';
import axios from 'axios';

import * as Client from 'mariadb';
import * as moment from 'moment';
import * as axiosRateLimit from 'axios-rate-limit';

export class Poller {
    authProvider: ClientCredentialsAuthProvider;
    apiClient: ApiClient;
    db: Client.Pool;

    constructor() {
        const authProvider = this.authProvider = new ClientCredentialsAuthProvider(process.env.TWITCH_CLIENT_ID, process.env.TWITCH_CLIENT_SECRET);
        this.apiClient = new ApiClient({authProvider});
        this.db = Client.createPool({host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE, connectionLimit: 10});
    }

    async saveStreams() {
        console.log(moment().format('HH:mm:ss') + ': Started polling Streams');

        let paginatedStreams = await this.apiClient.helix.streams.getStreamsPaginated();
        let streamsComplete: HelixStream[] = await paginatedStreams.getAll();

        let connection = await this.db.getConnection();

        let users = [];
        let streams = [];
        let games = [];
        let streamStats = [];
        let streamTags = [];

        console.log(moment().format('HH:mm:ss') + ': Finished Polling Streams');

        streamsComplete.forEach(stream => {
            users.push([stream.userId, stream.userDisplayName]);
            streams.push([stream.id, stream.userId, stream.startDate]);

            if(stream.gameId != '' && stream.gameId != null) {
                games.push([stream.gameId]);
            }

            streamStats.push([(stream.gameId == '' ? null : stream.gameId), stream.id, stream.viewers, stream.language]);

            stream.tagIds.forEach(tagId => {
                if(tagId){
                    streamTags.push([stream.id, tagId]);
                }
            });
        });

        console.log(moment().format('HH:mm:ss') + ': Saving Streams');

        await connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
        await connection.beginTransaction();
        await connection.batch(`INSERT IGNORE INTO User (id, display_name) VALUES (?, ?)`, users);
        await connection.batch(`INSERT INTO Stream (id, user_id, started_at, ended_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE ended_at=CURRENT_TIMESTAMP()`, streams);
        await connection.batch(`INSERT IGNORE INTO Game (id) VALUES (?)`, games);
        await connection.batch(`INSERT INTO Stream_Stats (game_id, stream_id, viewer_count, language) VALUES (?, ?, ?, ?)`, streamStats);
        await connection.batch(`INSERT IGNORE INTO Stream_Tag (stream_id, tag_id) VALUES (?, ?)`, streamTags);

        await connection.commit();

        await connection.release();

        console.log(moment().format('HH:mm:ss') + ': Finished Saving Streams');
    }

    async saveTags()
    {
        console.log(moment().format('HH:mm:ss') + ': Started polling Tags');
        let paginatedTags = await this.apiClient.helix.tags.getAllStreamTagsPaginated();
        let tagsComplete: HelixTag[] = await paginatedTags.getAll();

        console.log(moment().format('HH:mm:ss') + ': Finished Polling Tags');
        let connection = await this.db.getConnection();
        let tags = [];
        tagsComplete.forEach(tag => {
            if(tag.id && tag.getName('de-de')) {
                tags.push([tag.id, tag.getName('de-de')]);
            }
        });

        console.log(moment().format('HH:mm:ss') + ': Started Saving Tags');
        await connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
        await connection.beginTransaction();
        await connection.batch(`INSERT IGNORE INTO Tag (id, name) VALUES (?, ?)`, tags);

        await connection.commit();

        await connection.release();
        console.log(moment().format('HH:mm:ss') + ': Finished Saving Tags');
    }

    async updateUsers()
    {
        let connection = await this.db.getConnection();

        console.log(moment().format('HH:mm:ss') + ': Started Selecting Users');
        await connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
        await connection.beginTransaction();
        let userIds = await connection.query(`SELECT id FROM User WHERE type IS NULL`);

        let currentUsers = [];
        for(let i = 0; i < userIds.length; i += 1 ){
            if(i%100 == 0){
                currentUsers.push([]);
            }

            currentUsers[Math.floor(i/100)].push(userIds[i].id);
        }

        console.log(moment().format('HH:mm:ss') + ': Finished Selecting Users');

        let userPromises = [];
        let users: HelixUser[] = [];

        await this.apiClient.getTokenInfo();

        console.log(moment().format('HH:mm:ss') + ': Starting Updating Users');
        for(let j = 0; j < currentUsers.length; j++){
            let e = currentUsers[j];

            userPromises.push(this.apiClient.helix.users.getUsersByIds(e).then(result => Array.prototype.push.apply(users, result)));
        }

        await Promise.all(userPromises);

        let sqlTypeCases = [];
        let sqlBroadcasterTypeCases = [];
        let sqlIds = [];

        users.forEach(user => {
            let id = user.id;
            let type = user.type;
            let broadcasterType = user.broadcasterType;

            sqlTypeCases.push(`WHEN '${id}' THEN '${type}'`);
            sqlBroadcasterTypeCases.push(`WHEN '${id}' THEN '${broadcasterType}'`);
            sqlIds.push(id);
        });

        let sqlTypeCase = sqlTypeCases.join(' ');
        let sqlBroadcasterTypeCase = sqlBroadcasterTypeCases.join(' ');
        let sqlIdClause = sqlIds.join(',');

        console.log(moment().format('HH:mm:ss') + ': Built SQL Cases');

        let sqlStatement = `UPDATE User SET type = CASE id ${sqlTypeCase} ELSE type END, broadcaster_type = CASE id ${sqlBroadcasterTypeCase} ELSE broadcaster_type END WHERE id IN (${sqlIdClause})`;
        await connection.query(sqlStatement);
        await connection.commit();
        await connection.release();

        console.log(moment().format('HH:mm:ss') + ': Finished Updating Users');
    }

    async updateGames() {
        let connection = await this.db.getConnection();

        console.log(moment().format('HH:mm:ss') + ': Started Selecting Games');
        await connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
        await connection.beginTransaction();
        let gameIds = await connection.query(`SELECT id FROM Game`);

        let currentGames = [];
        for(let i = 0; i < gameIds.length; i += 1){
            if(i%500 == 0){
                currentGames.push([]);
            }

            currentGames[Math.floor(i/500)].push('"' + gameIds[i].id + '"');
        }

        console.log(moment().format('HH:mm:ss') + ': Finished Selecting Games');

        const appAccessToken = await this.authProvider.getAccessToken();
        const igdb = new IGDB(process.env.TWITCH_CLIENT_ID, appAccessToken.accessToken);

        let sqlIds = [];
        let sqlNameCases = [];
        let sqlSteamIdCases = [];
        let sqlFirstReleaseDateCases = [];
        let sqlRatingCases = [];

        let genres = [];
        let gameGenres = [];

        console.log(moment().format('HH:mm:ss') + ': Starting Updating Games');
        for(let i = 0 ; i < currentGames.length ; i++) {
            let gameIdBatch = currentGames[i];
            let gameIdFilter = gameIdBatch.join(',');

            let response = await igdb
                .fields(['uid', 'game.name' ,'game.first_release_date', 'game.rating', 'game.external_games.category', 'game.external_games.uid', 'game.genres.*'])
                .limit(500)
                .where(`category = 14 & uid = (${gameIdFilter}) & game.first_release_date != null & game.rating != null`)
                .request('/external_games');

            response.data.forEach((game) => {
                let steamGame = game.game.external_games.find(externalGame => externalGame.category == 1);

                let id = game.uid;
                let name = game.game.name.replace(new RegExp('\'', 'g'), '\'\'');
                let steamId = steamGame ? steamGame.uid : null;
                let firstReleaseDate = game.game.first_release_date ? "'" + moment(new Date(game.game.first_release_date * 1000)).format('YYYY-MM-DD HH:mm:ss') + "'" : 'NULL';
                let rating = game.game.rating ? game.game.rating : 0.00;

                if(game.game.genres) {
                    for(let j = 0 ; j < game.game.genres.length ; j++) {
                        let genre = game.game.genres[j];

                        let genreId = genre.id;
                        let genreName = genre.name;

                        genres.push([genreId, genreName]);
                        gameGenres.push([id, genreId]);
                    }
                }

                sqlIds.push(id);
                sqlNameCases.push(`WHEN '${id}' THEN '${name}'`);
                sqlSteamIdCases.push(`WHEN '${id}' THEN ` + (steamId ? `'${steamId}'` : 'NULL'));
                sqlFirstReleaseDateCases.push(`WHEN '${id}' THEN ${firstReleaseDate}`);
                sqlRatingCases.push(`WHEN '${id}' THEN ${rating}`);
            });
        }

        let sqlNameCase = sqlNameCases.join(' ');
        let sqlSteamIdCase = sqlSteamIdCases.join(' ');
        let sqlFirstReleaseDateCase = sqlFirstReleaseDateCases.join(' ');
        let sqlRatingCase = sqlRatingCases.join(' ');
        let sqlIdClause = sqlIds.join(',');

        console.log(moment().format('HH:mm:ss') + ': Built SQL Cases');

        await connection.batch('INSERT IGNORE INTO Genre (id, name) VALUES (?, ?)', genres);
        await connection.batch('INSERT IGNORE INTO Game_Genre (game_id, genre_id) VALUES (?, ?)', gameGenres);

        let sqlStatement = `UPDATE Game SET name = CASE id ${sqlNameCase} ELSE name END, steam_id = CASE id ${sqlSteamIdCase} ELSE steam_id END, first_release_date = CASE id ${sqlFirstReleaseDateCase} ELSE first_release_date END, ` +
            `rating = CASE id ${sqlRatingCase} ELSE rating END WHERE id IN (${sqlIdClause})`;
        await connection.query(sqlStatement);
        await connection.commit();
        await connection.release();

        console.log(moment().format('HH:mm:ss') + ': Finished Updating Games');
    }

    async updateGamesPlayerCount() {
        let connection = await this.db.getConnection();
        // @ts-ignore
        let http = axiosRateLimit(axios.create(), { maxRequests: 60, perMilliseconds: 1000 });

        console.log(moment().format('HH:mm:ss') + ': Started Selecting Games');
        await connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED');
        await connection.beginTransaction();
        let games = await connection.query(`SELECT id, steam_id FROM Game WHERE NOT steam_id IS NULL`);

        let stats = [];
        let statsPromises = [];

        for(let i = 0 ; i < games.length ; i++) {
            let game = games[i];
            let id = game.id;
            let steamId = game.steam_id;

            statsPromises.push((async function () {
                try {
                    let response = await http.get(`https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=${steamId}`);
                    let playerCount = response.data.response.player_count;

                    stats.push([id, playerCount]);
                } catch (e) {
                    if(e.response) {
                        let statusCode = e.response.status;

                        if(statusCode == 404) {
                            console.log(moment().format('HH:mm:ss') + `: Game '${id}' does not exist on steam`);
                        } else {
                            console.log(moment().format('HH:mm:ss') + `: Error ${statusCode} while checking players for id '${id}'`);
                        }
                    } else {
                        console.error(e);
                    }
                }

            })());
        }

        await Promise.all(statsPromises);

        await connection.batch('INSERT INTO Current_Players (game_id, `count`) VALUES (?, ?)', stats);
        await connection.commit();
        await connection.release();

        console.log(moment().format('HH:mm:ss') + ': Finished inserting Current Players');
    }
}