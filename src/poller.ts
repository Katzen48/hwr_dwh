import {ApiClient, HelixStream} from "twitch";
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
        let paginatedStreams = await this.apiClient.helix.streams.getStreamsPaginated();

        let page: HelixStream[];
        while ((page = await paginatedStreams.getNext()).length) {
            const self = this;

            (async () => {
                let users = [];
                let streams = [];
                let games = [];
                let streamStats = [];

                page.forEach(stream => {
                    users.push([stream.userId, stream.userDisplayName]);

                    streams.push([stream.id, stream.userId, stream.startDate]);

                    games.push([stream.gameId]);

                    streamStats.push([stream.gameId, stream.id, stream.viewers, stream.language]);
                });

                const connection = await self.db.getConnection();

                await connection.beginTransaction();
                await connection.batch(`INSERT IGNORE INTO User (id, display_name) VALUES (?, ?)`, users);
                await connection.batch(`INSERT INTO Stream (id, user_id, started_at, ended_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE ended_at=CURRENT_TIMESTAMP()`, streams);
                await connection.batch(`INSERT IGNORE INTO Game (id) VALUES (?)`, games);
                await connection.batch(`INSERT INTO Stream_Stats (game_id, stream_id, viewer_count, language) VALUES (?, ?, ?, ?)`, streamStats);

                await connection.commit();

                await connection.release();
            })();
        }
    }
}