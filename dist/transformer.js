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
            return __generator(this, function (_a) {
                switch (_a.label) {
                    case 0: return [4 /*yield*/, this.transformGames()];
                    case 1:
                        _a.sent();
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
                        // 1. Games aus Orginal Tabelle laden
                        return [4 /*yield*/, originalConnection.beginTransaction()];
                    case 3:
                        // 1. Games aus Orginal Tabelle laden
                        _a.sent();
                        return [4 /*yield*/, originalConnection.query("SELECT * FROM games_transformed")];
                    case 4:
                        originalGames = _a.sent();
                        return [4 /*yield*/, originalConnection.release()];
                    case 5:
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
                        return [4 /*yield*/, targetConnection.beginTransaction()];
                    case 6:
                        _a.sent();
                        return [4 /*yield*/, targetConnection.batch('INSERT IGNORE INTO Game (id, `name`, steam_id, first_release_date, rating, genre_name) VALUES (?, ?, ?, ?, ?, ?)', targetGames)];
                    case 7:
                        _a.sent();
                        // 3. Commiten
                        return [4 /*yield*/, targetConnection.commit()];
                    case 8:
                        // 3. Commiten
                        _a.sent();
                        // 4. fertig
                        return [4 /*yield*/, targetConnection.release()];
                    case 9:
                        // 4. fertig
                        _a.sent();
                        console.log(moment().format('HH:mm:ss') + " Transform Games finished");
                        return [2 /*return*/];
                }
            });
        });
    };
    return Transformer;
}());
exports.Transformer = Transformer;
//# sourceMappingURL=transformer.js.map