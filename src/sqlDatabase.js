const { SQLDataSource } = require("datasource-sql");

class SqlDatabase extends SQLDataSource {
    get(entity, fields = '*', limit = 1000, orderBy, filter, page = 0) {
        let query = this.knex
                    .distinct(fields)
                    .from(entity)

        if(filter) {
            for (let key in filter) {
                if(filter[key] === null) {
                    query = query.whereNull(key);
                } else if (filter[key].toLowerCase() === 'not null') {
                    query = query.whereNotNull(key);
                } else {
                    query = query.where(key, 'like', filter[key]);
                }
            }
        }

        if(orderBy) {
            for (let key in orderBy) {
                query = query.orderBy(key, orderBy[key]);
            }
        }

        return query.limit(limit).offset(limit * page);
    }

    getFromFacts({sqlFieldName, gqlFieldName}, limit = 1, orderBy, fromDate, toDate, filter, page = 0) {
        let query = this.knex
            .select(this.knex.raw(`SQL_CACHE SUM(${sqlFieldName}) as ${gqlFieldName}, game_id, created_at`))
            .groupBy(['game_id', 'created_at'])
            .from('Facts');

        if(filter) {
            for (let key in filter) {
                if(filter[key] === null) {
                    query = query.whereNull(key);
                } else if (filter[key].toLowerCase() === 'not null') {
                    query = query.whereNotNull(key);
                } else {
                    query = query.where(key, 'like', filter[key]);
                }
            }
        }

        if(orderBy) {
            for (let key in orderBy) {
                query = query.orderBy(key, orderBy[key]);
            }
        }

        query = query.where('created_at', '>=', fromDate)
                     .where('created_at', '<', toDate);

        query.limit(limit).offset(limit * page);

        return query;
    }

    getFirst(entity, fields = '*', filter) {
        let query = this.knex
            .distinct(fields)
            .from(entity)

        if(filter) {
            for (let key in filter) {
                query = query.where(key, 'like', filter[key]);
            }
        }

        return query;
    }
}

module.exports = SqlDatabase;