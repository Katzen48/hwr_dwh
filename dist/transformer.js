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
exports.Transformer = void 0;
var Client = require("mariadb");
var moment = require("moment");
var Transformer = /** @class */ (function () {
    function Transformer() {
        this.originalDb = Client.createPool({ host: process.env.ORIGINAL_DB_HOST, user: process.env.ORIGINAL_DB_USER, password: process.env.ORIGINAL_DB_PASSWORD, database: process.env.ORIGINAL_DB_DATABASE, connectionLimit: 5 });
        this.targetDb = Client.createPool({ host: process.env.TARGET_DB_HOST, user: process.env.TARGET_DB_USER, password: process.env.TARGET_DB_PASSWORD, database: process.env.TARGET_DB_DATABASE, connectionLimit: 5 });
    }
    Transformer.prototype.transform = function () {
        return __awaiter(this, void 0, void 0, function () {
            var gamesPromise;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        gamesPromise = this.transformGames();
                        return [4 /*yield*/, this.transformStreams()];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, this.transformFacts()];
                    case 2:
                        _a.sent();
                        return [4 /*yield*/, gamesPromise];
                    case 3:
                        _a.sent();
                        process.exit(0);
                        return [2 /*return*/];
                }
            });
        });
    };
    Transformer.prototype.transformGames = function () {
        return __awaiter(this, void 0, void 0, function () {
            var originalConnection, targetConnection, originalGames, targetGames;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + " Starting transform Games!");
                        return [4 /*yield*/, this.originalDb.getConnection()];
                    case 1:
                        originalConnection = _a.sent();
                        return [4 /*yield*/, this.targetDb.getConnection()];
                    case 2:
                        targetConnection = _a.sent();
                        return [4 /*yield*/, originalConnection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 3:
                        _a.sent();
                        // 1. Games aus Orginal Tabelle laden
                        return [4 /*yield*/, originalConnection.beginTransaction()];
                    case 4:
                        // 1. Games aus Orginal Tabelle laden
                        _a.sent();
                        return [4 /*yield*/, originalConnection.query("SELECT * FROM games_transformed")];
                    case 5:
                        originalGames = _a.sent();
                        return [4 /*yield*/, originalConnection.commit()];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.release()];
                    case 7:
                        _a.sent();
                        targetGames = [];
                        originalGames.forEach(function (g) {
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
                        return [4 /*yield*/, targetConnection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 8:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.beginTransaction()];
                    case 9:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.batch('INSERT IGNORE INTO Game (id, `name`, steam_id, first_release_date, rating, genre_name) VALUES (?, ?, ?, ?, ?, ?)', targetGames)];
                    case 10:
                        _a.sent();
                        // 3. Commiten
                        return [4 /*yield*/, targetConnection.commit()];
                    case 11:
                        // 3. Commiten
                        _a.sent();
                        // 4. fertig
                        return [4 /*yield*/, targetConnection.release()];
                    case 12:
                        // 4. fertig
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + " Transform Games finished");
                        return [2 /*return*/];
                }
            });
        });
    };
    Transformer.prototype.transformStreams = function () {
        return __awaiter(this, void 0, void 0, function () {
            var originalConnection, targetConnection, synchronized, originalStreams, targetStreamIds, targetStreams, s, sqlIds;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + " Starting transform Streams!");
                        return [4 /*yield*/, this.originalDb.getConnection()];
                    case 1:
                        originalConnection = _a.sent();
                        return [4 /*yield*/, this.targetDb.getConnection()];
                    case 2:
                        targetConnection = _a.sent();
                        return [4 /*yield*/, originalConnection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 3:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.beginTransaction()];
                    case 4:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 5:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.beginTransaction()];
                    case 6:
                        _a.sent();
                        synchronized = 0;
                        _a.label = 7;
                    case 7: return [4 /*yield*/, originalConnection.query("SELECT * FROM streams_transformed LIMIT 100000")];
                    case 8:
                        originalStreams = _a.sent();
                        synchronized = originalStreams.length;
                        console.log(moment().format('HH:mm:ss') + " Got " + synchronized + " Streams");
                        targetStreamIds = [];
                        targetStreams = [];
                        s = void 0;
                        while ((s = originalStreams.shift())) {
                            targetStreamIds.push('\'' + s.id + '\'');
                            targetStreams.push([
                                s.id,
                                s.user_id,
                                s.started_at ? s.started_at : null,
                                s.ended_at ? s.ended_at : null,
                                s.tag ? s.tag : null,
                                s.user_name,
                                s.user_type,
                                s.user_broadcaster_type
                            ]);
                        }
                        if (!(synchronized > 1)) return [3 /*break*/, 13];
                        sqlIds = targetStreamIds.join(',');
                        return [4 /*yield*/, originalConnection.query("UPDATE Stream SET transformed_at = CURRENT_TIMESTAMP() WHERE id IN (" + sqlIds + ")")];
                    case 9:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.batch('INSERT INTO Stream (id, user_id, started_at, ended_at, tag, user_name, user_type, user_broadcaster_type) VALUES (?, ?, ?, ?, ?, ?, ?, ?) ON DUPLICATE KEY UPDATE ended_at = VALUES(ended_at)', targetStreams)];
                    case 10:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.commit()];
                    case 11:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.commit()];
                    case 12:
                        _a.sent();
                        _a.label = 13;
                    case 13:
                        if (synchronized > 0) return [3 /*break*/, 7];
                        _a.label = 14;
                    case 14: return [4 /*yield*/, targetConnection.release()];
                    case 15:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.release()];
                    case 16:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + " Transform Streams finished");
                        return [2 /*return*/];
                }
            });
        });
    };
    Transformer.prototype.transformFacts = function () {
        return __awaiter(this, void 0, void 0, function () {
            var originalConnection, targetConnection, result, maxDate, synchronized, limit, startIndex, targetStats, originalCurrentPlayers, originalStreamStats, synchronized, limit, startIndex, targetStats, originalCurrentPlayers, originalStreamStats;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + " Starting transform Facts!");
                        return [4 /*yield*/, this.originalDb.getConnection()];
                    case 1:
                        originalConnection = _a.sent();
                        return [4 /*yield*/, this.targetDb.getConnection()];
                    case 2:
                        targetConnection = _a.sent();
                        return [4 /*yield*/, targetConnection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 3:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.beginTransaction()];
                    case 4:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.query('SELECT MAX(created_at) as max_date FROM Facts')];
                    case 5:
                        result = _a.sent();
                        maxDate = result[0].max_date;
                        if (maxDate) {
                            maxDate = '\'' + maxDate.toISOString() + '\'';
                        }
                        return [4 /*yield*/, originalConnection.query('SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED')];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.beginTransaction()];
                    case 7:
                        _a.sent();
                        if (!maxDate) return [3 /*break*/, 18];
                        synchronized = 0;
                        limit = 1000000;
                        startIndex = 0;
                        _a.label = 8;
                    case 8:
                        targetStats = [];
                        console.log(moment().format('HH:mm:ss') + " Requesting stats created_at >" + maxDate + " LIMIT " + startIndex + "," + limit);
                        return [4 /*yield*/, originalConnection.query("SELECT * FROM Current_Players WHERE created_at > " + maxDate + " ORDER BY created_at ASC LIMIT " + startIndex + "," + limit)];
                    case 9:
                        originalCurrentPlayers = _a.sent();
                        synchronized = originalCurrentPlayers.length;
                        console.log(moment().format('HH:mm:ss') + " Got " + synchronized + " stats");
                        return [4 /*yield*/, this.pushPlayerStats(originalCurrentPlayers, targetStats)];
                    case 10:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.query("SELECT * FROM Stream_Stats WHERE created_at > " + maxDate + " ORDER BY created_at ASC LIMIT " + startIndex + "," + limit)];
                    case 11:
                        originalStreamStats = _a.sent();
                        synchronized += originalStreamStats.length;
                        console.log(moment().format('HH:mm:ss') + " Got " + synchronized + " stats");
                        return [4 /*yield*/, this.pushStreamStats(originalStreamStats, targetStats)];
                    case 12:
                        _a.sent();
                        if (!(targetStats.length > 0)) return [3 /*break*/, 14];
                        return [4 /*yield*/, this.saveFacts(targetConnection, targetStats)];
                    case 13:
                        _a.sent();
                        _a.label = 14;
                    case 14:
                        startIndex += limit;
                        _a.label = 15;
                    case 15:
                        if (synchronized > 0) return [3 /*break*/, 8];
                        _a.label = 16;
                    case 16: return [4 /*yield*/, originalConnection.release()];
                    case 17:
                        _a.sent();
                        return [3 /*break*/, 29];
                    case 18:
                        synchronized = 0;
                        limit = 1000000;
                        startIndex = 0;
                        _a.label = 19;
                    case 19:
                        targetStats = [];
                        console.log(moment().format('HH:mm:ss') + " Requesting stats startIndex=" + startIndex);
                        return [4 /*yield*/, originalConnection.query("SELECT * FROM Current_Players ORDER BY created_at ASC LIMIT " + startIndex + "," + limit)];
                    case 20:
                        originalCurrentPlayers = _a.sent();
                        synchronized = originalCurrentPlayers.length;
                        console.log(moment().format('HH:mm:ss') + " Got " + synchronized + " stats");
                        return [4 /*yield*/, this.pushPlayerStats(originalCurrentPlayers, targetStats)];
                    case 21:
                        _a.sent();
                        return [4 /*yield*/, originalConnection.query("SELECT * FROM Stream_Stats WHERE NOT game_id is NULL ORDER BY created_at ASC LIMIT " + startIndex + "," + limit)];
                    case 22:
                        originalStreamStats = _a.sent();
                        synchronized += originalStreamStats.length;
                        console.log(moment().format('HH:mm:ss') + " Got " + synchronized + " stats");
                        return [4 /*yield*/, this.pushStreamStats(originalStreamStats, targetStats)];
                    case 23:
                        _a.sent();
                        if (!(targetStats.length > 0)) return [3 /*break*/, 25];
                        return [4 /*yield*/, this.saveFacts(targetConnection, targetStats)];
                    case 24:
                        _a.sent();
                        _a.label = 25;
                    case 25:
                        startIndex += limit;
                        _a.label = 26;
                    case 26:
                        if (synchronized > 0) return [3 /*break*/, 19];
                        _a.label = 27;
                    case 27: return [4 /*yield*/, originalConnection.release()];
                    case 28:
                        _a.sent();
                        _a.label = 29;
                    case 29: return [4 /*yield*/, targetConnection.release()];
                    case 30:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + " Transform Facts finished");
                        return [2 /*return*/];
                }
            });
        });
    };
    Transformer.prototype.pushPlayerStats = function (originalCurrentPlayers, targetStats) {
        return __awaiter(this, void 0, void 0, function () {
            var p, analyticsDate;
            return __generator(this, function (_a) {
                while ((p = originalCurrentPlayers.shift())) {
                    analyticsDate = moment(p.created_at).seconds(0).milliseconds(0);
                    analyticsDate = analyticsDate.minutes(Math.floor(analyticsDate.minutes() / 30) * 30);
                    targetStats.push([
                        0,
                        p.count ? p.count : 0,
                        p.game_id,
                        null,
                        analyticsDate.toDate(),
                    ]);
                }
                return [2 /*return*/];
            });
        });
    };
    Transformer.prototype.pushStreamStats = function (originalStreamStats, targetStats) {
        return __awaiter(this, void 0, void 0, function () {
            var s, analyticsDate;
            return __generator(this, function (_a) {
                while ((s = originalStreamStats.shift())) {
                    analyticsDate = moment(s.created_at).seconds(0).milliseconds(0);
                    analyticsDate = analyticsDate.minutes(Math.floor(analyticsDate.minutes() / 30) * 30);
                    targetStats.push([
                        s.viewer_count ? s.viewer_count : 0,
                        0,
                        s.game_id,
                        s.stream_id,
                        analyticsDate.toDate(),
                    ]);
                }
                return [2 /*return*/];
            });
        });
    };
    Transformer.prototype.saveFacts = function (targetConnection, targetStats) {
        return __awaiter(this, void 0, void 0, function () {
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, targetConnection.batch('INSERT INTO Facts (viewer_count, players_count, game_id, stream_id, created_at) VALUES (?, ?, ?, ?, ?)', targetStats)];
                    case 1:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.commit()];
                    case 2:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    return Transformer;
}());
exports.Transformer = Transformer;
//# sourceMappingURL=transformer.js.map