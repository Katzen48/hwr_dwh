const { SQLDataSource } = require("datasource-sql");

const TTL = 60;

class SqlDatabase extends SQLDataSource {
    get(entity, fields = '*', limit = 1000, orderBy, filter, page = 0) {
        let query = this.knex
                    .distinct(fields)
                    .from(entity)

        if(filter) {
            for (let key in filter) {
                query = query.where(key, filter[key]);
            }
        }

        if(orderBy) {
            for (let key in orderBy) {
                query = query.orderBy(key, orderBy[key]);
            }
        }

        return query.limit(limit).offset(limit * page);
    }

    getFromFactsView(view, fields = '*', limit = 1, orderBy, fromDate, toDate, filter, page = 0) {
        let query = this.knex
            .distinct(fields)
            .from(view)

        if(filter) {
            for (let key in filter) {
                query = query.where(key, filter[key]);
            }
        }

        if(orderBy) {
            for (let key in orderBy) {
                query = query.orderBy(key, orderBy[key]);
            }
        }

        query = query.where('created_at', '>=', fromDate)
                     .where('created_at', '<=', toDate);

        query.limit(limit).offset(limit * page);

        return query;
    }

    getFirst(entity, fields = '*', filter) {
        let query = this.knex
            .distinct(fields)
            .from(entity)

        if(filter) {
            for (let key in filter) {
                query = query.where(key, filter[key]);
            }
        }

        return query;
    }

    getGames(fields) {
        return this.knex
            .distinct(fields)
            .from("Game")
            .limit(1000);
    }

    getStreams(fields) {
        return this.knex
            .distinct(fields)
            .from("Stream")
            .limit(1000);
    }
}

module.exports = SqlDatabase;