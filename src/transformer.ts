import * as Client from 'mariadb';
import * as moment from 'moment';

export class Transformer {
    originalDb: Client.Pool;
    targetDb: Client.Pool;

    constructor() {
        this.originalDb = Client.createPool({host: process.env.ORIGINAL_DB_HOST, user: process.env.ORIGINAL_DB_USER, password: process.env.ORIGINAL_DB_PASSWORD, database: process.env.ORIGINAL_DB_DATABASE, connectionLimit: 5});
        this.targetDb = Client.createPool({host: process.env.TARGET_DB_HOST, user: process.env.TARGET_DB_USER, password: process.env.TARGET_DB_PASSWORD, database: process.env.TARGET_DB_DATABASE, connectionLimit: 5});
    }

    async transform() {
        this.transformGames();

        await this.transformStreams();
        await this.transformFacts();
    }

    async transformGames() {
        console.log(moment().format('HH:mm:ss') + " Starting transform Games!");

        // Param
        let originalConnection = await this.originalDb.getConnection();
        let targetConnection = await this.targetDb.getConnection();

        // 1. Games aus Orginal Tabelle laden
        await originalConnection.beginTransaction();

        let originalGames = await originalConnection.query(`SELECT * FROM games_transformed`);

        await originalConnection.release();
        // 2. Insert Statement blablabla, und ausführen
        let targetGames = [];
        originalGames.forEach(g => {
            // TODO: Überhaupt nötig oder kann das Result aus der Query nicht direkt ins Array gepusht werden?
            targetGames.push([
                g.id,
                g.name,
                g.steam_id,
                g.first_release_date,
                g.rating,
                g.genre_name
            ]);
        });

        await targetConnection.beginTransaction();
        await targetConnection.batch('INSERT IGNORE INTO Game (id, `name`, steam_id, first_release_date, rating, genre_name) VALUES (?, ?, ?, ?, ?, ?)', targetGames);

        // 3. Commiten
        await targetConnection.commit();

        // 4. fertig
        await targetConnection.release();

        console.log(moment().format('HH:mm:ss') + " Transform Games finished");
    }

    async transformStreams() {
        console.log(moment().format('HH:mm:ss') + " Starting transform Streams!");

        let originalConnection = await this.originalDb.getConnection();
        let targetConnection = await this.targetDb.getConnection();

        await originalConnection.beginTransaction();

        let originalStreams = await originalConnection.query(`SELECT * FROM streams_transformed`);

        let targetStreamIds = [];
        let targetStreams = [];

        let s;
        while((s = originalStreams.shift())) {
            targetStreamIds.push('\'' + s.id + '\'');

            targetStreams.push([
                s.id,
                s.user_id,
                s.started_at,
                s.ended_at,
                s.tag,
                s.user_name,
                s.user_type,
                s.user_broadcaster_type
            ]);
        }

        let sqlIds = targetStreamIds.join(',');
        await originalConnection.query(`UPDATE Stream SET transformed_at = CURRENT_TIMESTAMP() WHERE id IN (${sqlIds})`);

        await targetConnection.beginTransaction();
        await targetConnection.batch('INSERT INTO Stream (id, user_id, started_at, ended_at, tag, user_name, user_type, user_broadcaster_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ended_at = VALUES(ended_at)', targetStreams);

        await targetConnection.commit();
        await targetConnection.release();

        await originalConnection.commit();
        await originalConnection.release();

        console.log(moment().format('HH:mm:ss') + " Transform Streams finished");
    }

    async transformFacts() {
        console.log(moment().format('HH:mm:ss') + " Starting transform Facts!");

        let originalConnection = await this.originalDb.getConnection();
        let targetConnection = await this.targetDb.getConnection();

        await targetConnection.beginTransaction();
        let result = await targetConnection.query('SELECT MAX(created_at) as max_date FROM Facts');
        let maxDate = result[0].max_date;

        if(maxDate) {
            maxDate = '\'' + maxDate + '\''
        }

        let targetStats = [];

        await originalConnection.beginTransaction()
        let originalCurrentPlayers = await originalConnection.query(`SELECT * FROM Current_Players WHERE created_at > ${maxDate}`);
        let p;
        while((p = originalCurrentPlayers.shift())) {
            targetStats.push([
                0,
                p.count,
                p.game_id,
                null,
                p.created_at
            ]);
        }

        let originalStreamStats = await originalConnection.query(`SELECT * FROM Stream_Stats WHERE created_at > ${maxDate}`);
        let s;
        while((s = originalStreamStats.shift())) {
            targetStats.push([
                s.viewer_count,
                0,
                s.game_id,
                s.stream_id,
                s.created_at
            ]);
        }

        await originalConnection.release();

        await targetConnection.batch('INSERT INTO Facts (viewer_count, players_count, game_id, stream_id, created_at) VALUES (?, ?, ?, ?, ?)', targetStats);

        await targetConnection.commit();
        await targetConnection.release();

        console.log(moment().format('HH:mm:ss') + " Transform Facts finished");
    }
}
