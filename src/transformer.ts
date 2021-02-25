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
        await Promise.all([
            this.transformGames(),
            this.transformStreams()
        ]);

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

        let targetStreams = [];

        originalStreams.forEach(s => {
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
        });

        await targetConnection.beginTransaction();
        await targetConnection.batch('INSERT INTO Stream (id, user_id, started_at, ended_at, tag, user_name, user_type, user_broadcaster_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', targetStreams);

        await targetConnection.commit();

        await targetConnection.release();

        await originalConnection.release();

        console.log(moment().format('HH:mm:ss') + " Transform Streams finished");

    }
}
