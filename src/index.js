require('dotenv').config();

const fs = require('fs');
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
        database: process.env.DB_DATABASE
    }
}

const db = new SqlDatabase(knexConfig);

const typeDefs = gql(fs.readFileSync(__dirname + '/schema.graphql', 'utf8'));

function resolveFromDb(dataSources, entity, {first, orderBy, filter, page = 0}, info) {
    let fields = Object.keys(graphqlFields(info)).filter(value => value !== 'game' && value !== 'stream' && value !== 'facts');
    let limit = (first && first <= 1000 ? first : 1000);

    page = page >= 0 ? page : 0;

    return dataSources.db.get(entity, fields, limit, orderBy, filter, page);
}

function resolveFirstFromDb(dataSources, entity, {id}, info) {
    let fields = Object.keys(graphqlFields(info));

    return dataSources.db.getFirst(entity, fields, {id: id});
}

const resolvers = {
    Query: {
        games: (root, {first, orderBy, filter, page = 0}, {dataSources}, info) => (resolveFromDb(dataSources, 'Game', {first, orderBy, filter, page}, info)),
        game: (root, {id}, {dataSources}, info) => (resolveFirstFromDb(dataSources, 'Game', {id}, info)),
        streams: (root, {first, orderBy, filter, page = 0}, {dataSources}, info) => (resolveFromDb(dataSources, 'Stream', {first, orderBy, filter, page}, info)),
        stream: (root, {id}, {dataSources}, info) => (resolveFirstFromDb(dataSources, 'Stream', {id}, info)),
        facts: (root, {first, orderBy, page = 0}, {dataSources}, info) => (resolveFromDb(dataSources, 'Facts', {first, orderBy, page}, info)),
    },
    Facts: {
        game: (parent, {}, {dataSources}, info) => {
            let filter = {
                id: parent.game_id,
            }

            if(!filter.id) {
                return [];
            }

            return resolveFromDb(dataSources, 'Game', {filter}, info);
        },

        stream: (parent, {}, {dataSources}, info) => {
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
        facts: (parent, {first = 10, orderBy = {}, page = 0}, {dataSources}, info) => {
            let filter = {
                game_id: parent.id,
            }

            first = first <= 10 ? first : 10;
            orderBy.created_at = orderBy.created_at ? orderBy.created_at : 'desc';

            return resolveFromDb(dataSources, 'Facts', {first, orderBy, filter, page}, info);
        },
    },
    Stream: {
        facts: (parent, {first = 10, orderBy = {}, page = 0}, {dataSources}, info) => {
            let filter = {
                stream_id: parent.id,
            }

            first = first <= 10 ? first : 10;
            orderBy.created_at = orderBy.created_at ? orderBy.created_at : 'desc';

            return resolveFromDb(dataSources, 'Facts', {first, orderBy, filter, page}, info);
        },
    }
}

const server = new ApolloServer({
    typeDefs,
    resolvers,
    dataSources : () => ({db}),
    cacheControl: {
        calculateHttpHeaders: false,
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