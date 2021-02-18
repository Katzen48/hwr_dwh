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
var Client = require("mariadb");
var moment = require("moment");
var Poller = /** @class */ (function () {
    function Poller() {
        var authProvider = new twitch_auth_1.ClientCredentialsAuthProvider(process.env.TWITCH_CLIENT_ID, process.env.TWITCH_CLIENT_SECRET);
        this.apiClient = new twitch_1.ApiClient({ authProvider: authProvider });
        this.db = Client.createPool({ host: process.env.DB_HOST, user: process.env.DB_USER, password: process.env.DB_PASSWORD, database: process.env.DB_DATABASE, connectionLimit: 10 });
    }
    Poller.prototype.saveStreams = function () {
        return __awaiter(this, void 0, void 0, function () {
            var paginatedStreams, currentPage, page, _loop_1, this_1;
            var _this = this;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + ': Started polling');
                        return [4 /*yield*/, this.apiClient.helix.streams.getStreamsPaginated()];
                    case 1:
                        paginatedStreams = _a.sent();
                        currentPage = null;
                        _loop_1 = function () {
                            var self_1 = this_1;
                            currentPage = (function () { return __awaiter(_this, void 0, void 0, function () {
                                var users, streams, games, streamStats, connection;
                                return __generator(this, function (_a) {
                                    switch (_a.label) {
                                        case 0:
                                            users = [];
                                            streams = [];
                                            games = [];
                                            streamStats = [];
                                            page.forEach(function (stream) {
                                                users.push([stream.userId, stream.userDisplayName]);
                                                streams.push([stream.id, stream.userId, stream.startDate]);
                                                games.push([stream.gameId]);
                                                streamStats.push([stream.gameId, stream.id, stream.viewers, stream.language]);
                                            });
                                            return [4 /*yield*/, self_1.db.getConnection()];
                                        case 1:
                                            connection = _a.sent();
                                            return [4 /*yield*/, connection.beginTransaction()];
                                        case 2:
                                            _a.sent();
                                            return [4 /*yield*/, connection.batch("INSERT IGNORE INTO User (id, display_name) VALUES (?, ?)", users)];
                                        case 3:
                                            _a.sent();
                                            return [4 /*yield*/, connection.batch("INSERT INTO Stream (id, user_id, started_at, ended_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE ended_at=CURRENT_TIMESTAMP()", streams)];
                                        case 4:
                                            _a.sent();
                                            return [4 /*yield*/, connection.batch("INSERT IGNORE INTO Game (id) VALUES (?)", games)];
                                        case 5:
                                            _a.sent();
                                            return [4 /*yield*/, connection.batch("INSERT INTO Stream_Stats (game_id, stream_id, viewer_count, language) VALUES (?, ?, ?, ?)", streamStats)];
                                        case 6:
                                            _a.sent();
                                            return [4 /*yield*/, connection.commit()];
                                        case 7:
                                            _a.sent();
                                            return [4 /*yield*/, connection.release()];
                                        case 8:
                                            _a.sent();
                                            return [2 /*return*/];
                                    }
                                });
                            }); })();
                        };
                        this_1 = this;
                        _a.label = 2;
                    case 2: return [4 /*yield*/, paginatedStreams.getNext()];
                    case 3:
                        if (!(page = _a.sent()).length) return [3 /*break*/, 4];
                        _loop_1();
                        return [3 /*break*/, 2];
                    case 4: return [4 /*yield*/, currentPage];
                    case 5:
                        _a.sent();
                        return [2 /*return*/];
                }
            });
        });
    };
    Poller.prototype.saveStreamsComplete = function () {
        return __awaiter(this, void 0, void 0, function () {
            var paginatedStreams, streamsComplete, connection, users, streams, games, streamStats;
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0:
                        console.log(moment().format('HH:mm:ss') + ': Started polling');
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
                        console.log(moment().format('HH:mm:ss') + ': Finished Polling');
                        streamsComplete.forEach(function (stream) {
                            users.push([stream.userId, stream.userDisplayName]);
                            streams.push([stream.id, stream.userId, stream.startDate]);
                            if (stream.gameId != '' && stream.gameId != null) {
                                games.push([stream.gameId]);
                            }
                            streamStats.push([(stream.gameId == '' ? null : stream.gameId), stream.id, stream.viewers, stream.language]);
                        });
                        console.log(moment().format('HH:mm:ss') + ': Saving');
                        return [4 /*yield*/, connection.beginTransaction()];
                    case 4:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT IGNORE INTO User (id, display_name) VALUES (?, ?)", users)];
                    case 5:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT INTO Stream (id, user_id, started_at, ended_at) VALUES (?, ?, ?, CURRENT_TIMESTAMP()) ON DUPLICATE KEY UPDATE ended_at=CURRENT_TIMESTAMP()", streams)];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT IGNORE INTO Game (id) VALUES (?)", games)];
                    case 7:
                        _a.sent();
                        return [4 /*yield*/, connection.batch("INSERT INTO Stream_Stats (game_id, stream_id, viewer_count, language) VALUES (?, ?, ?, ?)", streamStats)];
                    case 8:
                        _a.sent();
                        return [4 /*yield*/, connection.commit()];
                    case 9:
                        _a.sent();
                        return [4 /*yield*/, connection.release()];
                    case 10:
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + ': Finished Saving');
                        return [2 /*return*/];
                }
            });
        });
    };
    return Poller;
}());
exports.Poller = Poller;
//# sourceMappingURL=poller.js.map