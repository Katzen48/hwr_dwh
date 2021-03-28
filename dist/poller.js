"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (_) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.Poller = void 0;
var twitch_1 = require("twitch");
var twitch_auth_1 = require("twitch-auth");
var igdb_api_node_1 = require("igdb-api-node");
var axios_1 = require("axios");
var Client = require("mariadb");
var moment = require("moment");
var axiosRateLimit = require("axios-rate-limit");
var Poller = /** @class */ (function () {
    function Poller() {
        var authProvider = this.authProvider = new twitch_auth_1.ClientCredentialsAuthProvider(process.env.TWITCH_CLIENT_ID, process.env.TWITCH_CLIENT_SECRET);
        this.apiClient = new twitch_1.ApiClient({ authProvider: authProvider });
        this.db = Client.createPool({ host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE, connectionLimit: 10 });
    }
    Poller.prototype.saveStreams = function () {
        return __awaiter(this, void 0, void 0, function () {
            var paginatedStreams, streamsComplete, connection, users, streams, games, streamStats, streamTags;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + ': Started polling Streams');
                        return [4 /*yield*/, this.apiClient.helix.streams.getStreamsPaginated()];
                    case 1:
                        paginatedStreams = _a.sent();
                        return [4 /*yield*/, paginatedStreams.getAll()];
                    case 2:
                        streamsComplete = _a.sent();
                        return [4 /*yield*/, this.db.getConnection()];
                    case 3:
                        connection = _a.sent();
                        users = [];
                        streams = [];
                        games = [];
                        streamStats = [];
                        streamTags = [];
                        console.log(moment().format('HH:mm:ss') + ': Finished Polling Streams');
                        streamsComplete.forEach(function (stream) {
                            users.push([stream.userId, stream.userDisplayName]);
                            streams.push([stream.id, stream.userId, stream.startDate]);
                            if (stream.gameId != '' && stream.gameId != null) {
                                games.push([stream.gameId]);
                            }
                            streamStats.push([(stream.gameId == '' ? null : stream.gameId), stream.id, stream.viewers, stream.language]);
                            stream.tagIds.forEach(function (tagId) {
                                if (tagId) {
                                    streamTags.push([stream.id, tagId]);
                                }
                            });
                        });
                        console.log(moment().format('HH:mm:ss') + ': Saving Streams');
                        return [4 /*yield*/, connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 4:
                        _a.sent();
                        return [4 /*yield*/, connection.beginTransaction()];
                    case 5:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT IGNORE INTO User (id, display_name) VALUES (?, ?)", users)];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT INTO Stream (id, user_id, started_at, ended_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE ended_at=CURRENT_TIMESTAMP()", streams)];
                    case 7:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT IGNORE INTO Game (id) VALUES (?)", games)];
                    case 8:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT INTO Stream_Stats (game_id, stream_id, viewer_count, language) VALUES (?, ?, ?, ?)", streamStats)];
                    case 9:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT IGNORE INTO Stream_Tag (stream_id, tag_id) VALUES (?, ?)", streamTags)];
                    case 10:
                        _a.sent();
                        return [4 /*yield*/, connection.commit()];
                    case 11:
                        _a.sent();
                        return [4 /*yield*/, connection.release()];
                    case 12:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished Saving Streams');
                        return [2 /*return*/];
                }
            });
        });
    };
    Poller.prototype.saveTags = function () {
        return __awaiter(this, void 0, void 0, function () {
            var paginatedTags, tagsComplete, connection, tags;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + ': Started polling Tags');
                        return [4 /*yield*/, this.apiClient.helix.tags.getAllStreamTagsPaginated()];
                    case 1:
                        paginatedTags = _a.sent();
                        return [4 /*yield*/, paginatedTags.getAll()];
                    case 2:
                        tagsComplete = _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished Polling Tags');
                        return [4 /*yield*/, this.db.getConnection()];
                    case 3:
                        connection = _a.sent();
                        tags = [];
                        tagsComplete.forEach(function (tag) {
                            if (tag.id && tag.getName('de-de')) {
                                tags.push([tag.id, tag.getName('de-de')]);
                            }
                        });
                        console.log(moment().format('HH:mm:ss') + ': Started Saving Tags');
                        return [4 /*yield*/, connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 4:
                        _a.sent();
                        return [4 /*yield*/, connection.beginTransaction()];
                    case 5:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT IGNORE INTO Tag (id, name) VALUES (?, ?)", tags)];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, connection.commit()];
                    case 7:
                        _a.sent();
                        return [4 /*yield*/, connection.release()];
                    case 8:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished Saving Tags');
                        return [2 /*return*/];
                }
            });
        });
    };
    Poller.prototype.updateUsers = function () {
        return __awaiter(this, void 0, void 0, function () {
            var connection, userIds, currentUsers, i, userPromises, users, j, e, sqlTypeCases, sqlBroadcasterTypeCases, sqlIds, sqlTypeCase, sqlBroadcasterTypeCase, sqlIdClause, sqlStatement;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.db.getConnection()];
                    case 1:
                        connection = _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Started Selecting Users');
                        return [4 /*yield*/, connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, connection.beginTransaction()];
                    case 3:
                        _a.sent();
                        return [4 /*yield*/, connection.query("SELECT id FROM User WHERE type IS NULL")];
                    case 4:
                        userIds = _a.sent();
                        currentUsers = [];
                        for (i = 0; i < userIds.length; i += 1) {
                            if (i % 100 == 0) {
                                currentUsers.push([]);
                            }
                            currentUsers[Math.floor(i / 100)].push(userIds[i].id);
                        }
                        console.log(moment().format('HH:mm:ss') + ': Finished Selecting Users');
                        userPromises = [];
                        users = [];
                        return [4 /*yield*/, this.apiClient.getTokenInfo()];
                    case 5:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Starting Updating Users');
                        for (j = 0; j < currentUsers.length; j++) {
                            e = currentUsers[j];
                            userPromises.push(this.apiClient.helix.users.getUsersByIds(e).then(function (result) { return Array.prototype.push.apply(users, result); }));
                        }
                        return [4 /*yield*/, Promise.all(userPromises)];
                    case 6:
                        _a.sent();
                        sqlTypeCases = [];
                        sqlBroadcasterTypeCases = [];
                        sqlIds = [];
                        users.forEach(function (user) {
                            var id = user.id;
                            var type = user.type;
                            var broadcasterType = user.broadcasterType;
                            sqlTypeCases.push("WHEN '" + id + "' THEN '" + type + "'");
                            sqlBroadcasterTypeCases.push("WHEN '" + id + "' THEN '" + broadcasterType + "'");
                            sqlIds.push(id);
                        });
                        sqlTypeCase = sqlTypeCases.join(' ');
                        sqlBroadcasterTypeCase = sqlBroadcasterTypeCases.join(' ');
                        sqlIdClause = sqlIds.join(',');
                        console.log(moment().format('HH:mm:ss') + ': Built SQL Cases');
                        sqlStatement = "UPDATE User SET type = CASE id " + sqlTypeCase + " ELSE type END, broadcaster_type = CASE id " + sqlBroadcasterTypeCase + " ELSE broadcaster_type END WHERE id IN (" + sqlIdClause + ")";
                        return [4 /*yield*/, connection.query(sqlStatement)];
                    case 7:
                        _a.sent();
                        return [4 /*yield*/, connection.commit()];
                    case 8:
                        _a.sent();
                        return [4 /*yield*/, connection.release()];
                    case 9:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished Updating Users');
                        return [2 /*return*/];
                }
            });
        });
    };
    Poller.prototype.updateGames = function () {
        return __awaiter(this, void 0, void 0, function () {
            var connection, gameIds, currentGames, i, appAccessToken, igdb, sqlIds, sqlNameCases, sqlSteamIdCases, sqlFirstReleaseDateCases, sqlRatingCases, genres, gameGenres, i, gameIdBatch, gameIdFilter, response, sqlNameCase, sqlSteamIdCase, sqlFirstReleaseDateCase, sqlRatingCase, sqlIdClause, sqlStatement;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.db.getConnection()];
                    case 1:
                        connection = _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Started Selecting Games');
                        return [4 /*yield*/, connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, connection.beginTransaction()];
                    case 3:
                        _a.sent();
                        return [4 /*yield*/, connection.query("SELECT id FROM Game")];
                    case 4:
                        gameIds = _a.sent();
                        currentGames = [];
                        for (i = 0; i < gameIds.length; i += 1) {
                            if (i % 500 == 0) {
                                currentGames.push([]);
                            }
                            currentGames[Math.floor(i / 500)].push('"' + gameIds[i].id + '"');
                        }
                        console.log(moment().format('HH:mm:ss') + ': Finished Selecting Games');
                        return [4 /*yield*/, this.authProvider.getAccessToken()];
                    case 5:
                        appAccessToken = _a.sent();
                        igdb = new igdb_api_node_1.default(process.env.TWITCH_CLIENT_ID, appAccessToken.accessToken);
                        sqlIds = [];
                        sqlNameCases = [];
                        sqlSteamIdCases = [];
                        sqlFirstReleaseDateCases = [];
                        sqlRatingCases = [];
                        genres = [];
                        gameGenres = [];
                        console.log(moment().format('HH:mm:ss') + ': Starting Updating Games');
                        i = 0;
                        _a.label = 6;
                    case 6:
                        if (!(i < currentGames.length)) return [3 /*break*/, 9];
                        gameIdBatch = currentGames[i];
                        gameIdFilter = gameIdBatch.join(',');
                        return [4 /*yield*/, igdb
                                .fields(['uid', 'game.name', 'game.first_release_date', 'game.rating', 'game.external_games.category', 'game.external_games.uid', 'game.genres.*'])
                                .limit(500)
                                .where("category = 14 & uid = (" + gameIdFilter + ") & game.first_release_date != null & game.rating != null")
                                .request('/external_games')];
                    case 7:
                        response = _a.sent();
                        response.data.forEach(function (game) {
                            var steamGame = game.game.external_games.find(function (externalGame) { return externalGame.category == 1; });
                            var id = game.uid;
                            var name = game.game.name.replace(new RegExp('\'', 'g'), '\'\'');
                            var steamId = steamGame ? steamGame.uid : null;
                            var firstReleaseDate = game.game.first_release_date ? "'" + moment(new Date(game.game.first_release_date * 1000)).format('YYYY-MM-DD HH:mm:ss') + "'" : 'NULL';
                            var rating = game.game.rating ? game.game.rating : 0.00;
                            if (game.game.genres) {
                                for (var j = 0; j < game.game.genres.length; j++) {
                                    var genre = game.game.genres[j];
                                    var genreId = genre.id;
                                    var genreName = genre.name;
                                    genres.push([genreId, genreName]);
                                    gameGenres.push([id, genreId]);
                                }
                            }
                            sqlIds.push(id);
                            sqlNameCases.push("WHEN '" + id + "' THEN '" + name + "'");
                            sqlSteamIdCases.push("WHEN '" + id + "' THEN " + (steamId ? "'" + steamId + "'" : 'NULL'));
                            sqlFirstReleaseDateCases.push("WHEN '" + id + "' THEN " + firstReleaseDate);
                            sqlRatingCases.push("WHEN '" + id + "' THEN " + rating);
                        });
                        _a.label = 8;
                    case 8:
                        i++;
                        return [3 /*break*/, 6];
                    case 9:
                        sqlNameCase = sqlNameCases.join(' ');
                        sqlSteamIdCase = sqlSteamIdCases.join(' ');
                        sqlFirstReleaseDateCase = sqlFirstReleaseDateCases.join(' ');
                        sqlRatingCase = sqlRatingCases.join(' ');
                        sqlIdClause = sqlIds.join(',');
                        console.log(moment().format('HH:mm:ss') + ': Built SQL Cases');
                        return [4 /*yield*/, connection.batch('INSERT IGNORE INTO Genre (id, name) VALUES (?, ?)', genres)];
                    case 10:
                        _a.sent();
                        return [4 /*yield*/, connection.batch('INSERT IGNORE INTO Game_Genre (game_id, genre_id) VALUES (?, ?)', gameGenres)];
                    case 11:
                        _a.sent();
                        sqlStatement = "UPDATE Game SET name = CASE id " + sqlNameCase + " ELSE name END, steam_id = CASE id " + sqlSteamIdCase + " ELSE steam_id END, first_release_date = CASE id " + sqlFirstReleaseDateCase + " ELSE first_release_date END, " +
                            ("rating = CASE id " + sqlRatingCase + " ELSE rating END WHERE id IN (" + sqlIdClause + ")");
                        return [4 /*yield*/, connection.query(sqlStatement)];
                    case 12:
                        _a.sent();
                        return [4 /*yield*/, connection.commit()];
                    case 13:
                        _a.sent();
                        return [4 /*yield*/, connection.release()];
                    case 14:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished Updating Games');
                        return [2 /*return*/];
                }
            });
        });
    };
    Poller.prototype.updateGamesPlayerCount = function () {
        return __awaiter(this, void 0, void 0, function () {
            var connection, http, games, stats, statsPromises, _loop_1, i;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.db.getConnection()];
                    case 1:
                        connection = _a.sent();
                        http = axiosRateLimit(axios_1.default.create(), { maxRequests: 60, perMilliseconds: 1000 });
                        console.log(moment().format('HH:mm:ss') + ': Started Selecting Games');
                        return [4 /*yield*/, connection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, connection.beginTransaction()];
                    case 3:
                        _a.sent();
                        return [4 /*yield*/, connection.query("SELECT id, steam_id FROM Game WHERE NOT steam_id IS NULL")];
                    case 4:
                        games = _a.sent();
                        stats = [];
                        statsPromises = [];
                        _loop_1 = function (i) {
                            var game = games[i];
                            var id = game.id;
                            var steamId = game.steam_id;
                            statsPromises.push((function () {
                                return __awaiter(this, void 0, void 0, function () {
                                    var response, playerCount, e_1, statusCode;
                                    return __generator(this, function (_a) {
                                        switch (_a.label) {
                                            case 0:
                                                _a.trys.push([0, 2, , 3]);
                                                return [4 /*yield*/, http.get("https://api.steampowered.com/ISteamUserStats/GetNumberOfCurrentPlayers/v1/?appid=" + steamId)];
                                            case 1:
                                                response = _a.sent();
                                                playerCount = response.data.response.player_count;
                                                stats.push([id, playerCount]);
                                                return [3 /*break*/, 3];
                                            case 2:
                                                e_1 = _a.sent();
                                                if (e_1.response) {
                                                    statusCode = e_1.response.status;
                                                    if (statusCode == 404) {
                                                        console.log(moment().format('HH:mm:ss') + (": Game '" + id + "' does not exist on steam"));
                                                    }
                                                    else {
                                                        console.log(moment().format('HH:mm:ss') + (": Error " + statusCode + " while checking players for id '" + id + "'"));
                                                    }
                                                }
                                                else {
                                                    console.error(e_1);
                                                }
                                                return [3 /*break*/, 3];
                                            case 3: return [2 /*return*/];
                                        }
                                    });
                                });
                            })());
                        };
                        for (i = 0; i < games.length; i++) {
                            _loop_1(i);
                        }
                        return [4 /*yield*/, Promise.all(statsPromises)];
                    case 5:
                        _a.sent();
                        return [4 /*yield*/, connection.batch('INSERT INTO Current_Players (game_id, `count`) VALUES (?, ?)', stats)];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, connection.commit()];
                    case 7:
                        _a.sent();
                        return [4 /*yield*/, connection.release()];
                    case 8:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished inserting Current Players');
                        return [2 /*return*/];
                }
            });
        });
    };
    return Poller;
}());
exports.Poller = Poller;
//# sourceMappingURL=poller.js.map