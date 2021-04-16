require('dotenv').config();

const fs = require('fs');
const { GraphQLDate, GraphQLTime, GraphQLDateTime } = require('graphql-iso-date');
const { ApolloServer, gql, AuthenticationError } = require('apollo-server');
const responseCachePlugin = require('apollo-server-plugin-response-cache');
const graphqlFields = require('graphql-fields');
const SqlDatabase = require('./sqlDatabase');

const knexConfig = {
    client: 'mysql',
    connection: {
        host: process.env.DB_HOST,
        user: process.env.DB_USER,
        password: process.env.DB_PASSWORD,
        database: process.env.DB_DATABASE,
        timezone: '+00:00',
        pool: {
            min: 0,
            max: 1
        },
        ping: function (conn, cb) { conn.query('SELECT VERSION()', cb) },
    },
}

const db = new SqlDatabase(knexConfig);

const typeDefs = gql(fs.readFileSync(__dirname + '/schema.graphql', 'utf8'));

function resolveFromDb(dataSources, entity, {first, orderBy, filter, page = 0}, info) {
    let fields = Object.keys(graphqlFields(info)).filter(value => value !== 'game' && value !== 'stream' && value !== 'facts' && value !== 'players' && value !== 'viewers');
    let limit = (first && first <= 1000 ? first : 1000);
    info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

    page = page >= 0 ? page : 0;

    return dataSources.db.get(entity, fields, limit, orderBy, filter, page);
}

function resolveCountFromDb(dataSources, {sqlFieldName, gqlFieldName}, {first, orderBy, fromDate, toDate, filter, page = 0}) {
    let limit = (first && first <= 1000 ? first : 1000);

    page = page >= 0 ? page : 0;

    return dataSources.db.getFromFacts({sqlFieldName, gqlFieldName}, limit, orderBy, fromDate, toDate, filter, page);
}

function resolveFirstFromDb(dataSources, entity, {id}, info) {
    info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

    let fields = Object.keys(graphqlFields(info)).filter(value => value !== 'game' && value !== 'stream' && value !== 'facts' && value !== 'players' && value !== 'viewers');

    return dataSources.db.getFirst(entity, fields, {id: id});
}

const resolvers = {
    Query: {
        games: (root, {first, orderBy, filter, page = 0}, {dataSources}, info) => (resolveFromDb(dataSources, 'Game', {first, orderBy, filter, page}, info)),
        game: (root, {id}, {dataSources}, info) => (resolveFirstFromDb(dataSources, 'Game', {id}, info)),
        streams: (root, {first, orderBy, filter, page = 0}, {dataSources}, info) => (resolveFromDb(dataSources, 'Stream', {first, orderBy, filter, page}, info)),
        stream: (root, {id}, {dataSources}, info) => (resolveFirstFromDb(dataSources, 'Stream', {id}, info)),
        facts: (root, {first, orderBy, page = 0}, {dataSources}, info) => (resolveFromDb(dataSources, 'Facts', {first, orderBy, page}, info)),
        players: (parent, {first, orderBy, fromDate = "CUR_DATE()", toDate = "CUR_DATE()"}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                stream_id: null,
            };

            return resolveCountFromDb(dataSources, {sqlFieldName: 'players_count', gqlFieldName: 'players'}, {first, orderBy, fromDate, toDate, filter});
        },
        viewers: (parent, {first, orderBy, fromDate = "CUR_DATE()", toDate = "CUR_DATE()"}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                stream_id: 'not null',
            };

            return resolveCountFromDb(dataSources, {sqlFieldName: 'viewer_count', gqlFieldName: 'viewers'}, {first, orderBy, fromDate, toDate, filter});
        },
    },
    Facts: {
        game: (parent, {}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                id: parent.game_id,
            }

            if(!filter.id) {
                return [];
            }

            return resolveFromDb(dataSources, 'Game', {filter}, info);
        },

        stream: (parent, {}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                id: parent.stream_id,
            }

            if(!filter.id) {
                return [];
            }

            return resolveFromDb(dataSources, 'Stream', {filter}, info);
        },
    },
    Game: {
        facts: (parent, {first = 12, orderBy = {}, page = 0}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                game_id: parent.id,
            }

            first = first <= 12 ? first : 12;
            orderBy.created_at = orderBy.created_at ? orderBy.created_at : 'desc';

            return resolveFromDb(dataSources, 'Facts', {first, orderBy, filter, page}, info);
        },
        viewers: (parent, {first = 12, fromDate = "CUR_DATE()", toDate = "CUR_DATE()"}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            if(!parent.id) {
                return [];
            }

            let filter = {
                game_id: parent.id,
                stream_id: 'not null',
            }

            first = first <= 12 ? first : 12;

            return resolveCountFromDb(dataSources, {sqlFieldName: 'viewer_count', gqlFieldName: 'viewers'}, {first, fromDate, toDate, filter});
        },
        players: (parent, {first = 12, fromDate = "CUR_DATE()", toDate = "CUR_DATE()"}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            if(!parent.id) {
                return [];
            }

            let filter = {
                game_id: parent.id,
                stream_id: null,
            }

            first = first <= 12 ? first : 12;

            return resolveCountFromDb(dataSources, {sqlFieldName: 'players_count', gqlFieldName: 'players'}, {first, fromDate, toDate, filter});
        }
    },
    Stream: {
        facts: (parent, {first = 12, orderBy = {}, page = 0}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                stream_id: parent.id,
            }

            first = first <= 12 ? first : 12;
            orderBy.created_at = orderBy.created_at ? orderBy.created_at : 'desc';

            return resolveFromDb(dataSources, 'Facts', {first, orderBy, filter, page}, info);
        },
    },
    PlayerCount: {
        game: (parent, {}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                id: parent.game_id,
            }

            if(!filter.id) {
                return [];
            }

            return resolveFromDb(dataSources, 'Game', {filter}, info);
        },
    },
    ViewerCount: {
        game: (parent, {}, {dataSources}, info) => {
            info.cacheControl.setCacheHint({maxAge: 1800, scope: 'PUBLIC'});

            let filter = {
                id: parent.game_id,
            }

            if(!filter.id) {
                return [];
            }

            return resolveFromDb(dataSources, 'Game', {filter}, info);
        },
    },
    Date: GraphQLDate,
    DateTime: GraphQLDateTime,
    Time: GraphQLTime,
}

const server = new ApolloServer({
    typeDefs,
    resolvers,
    dataSources : () => ({db}),
    cacheControl: {
        defaultMaxAge: 1800
    },
    plugins: [responseCachePlugin()],
    context: ({req}) => {
        const token = req.headers.authorization || '';

        if(!token.toLowerCase().startsWith('bearer ') || token.substr(7) !== process.env.AUTH_TOKEN) {
            throw new AuthenticationError('No valid Token provided');
        }
    },
    introspection: true
});

server.listen().then(({ url }) => {
    console.log(`🚀  Server ready at ${url}`);
});