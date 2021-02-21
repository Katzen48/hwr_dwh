import {ApiClient, HelixStream, HelixTag, HelixUser} from "twitch";
import { ClientCredentialsAuthProvider } from 'twitch-auth';

import * as Client from 'mariadb';
import * as moment from 'moment';

export class Poller {
    apiClient: ApiClient;
    db: Client.Pool;

    constructor() {
        const authProvider = new ClientCredentialsAuthProvider(process.env.TWITCH_CLIENT_ID, process.env.TWITCH_CLIENT_SECRET);
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

        //console.log(">>>" + JSON.stringify(tagsComplete[160]));
        console.log(moment().format('HH:mm:ss') + ': Started Saving Tags');
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

        console.log(moment().format('HH:mm:ss') + ': Build SQL Cases');

        let sqlStatement = `UPDATE User SET type = CASE id ${sqlTypeCase} ELSE type END, broadcaster_type = CASE id ${sqlBroadcasterTypeCase} ELSE broadcaster_type END WHERE id IN (${sqlIdClause})`;
        await connection.query(sqlStatement);
        await connection.commit();
        await connection.release();

        console.log(moment().format('HH:mm:ss') + ': Finished Updating Users');
    }
}