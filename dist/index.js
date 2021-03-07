"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var transformer_1 = require("./transformer");
require('dotenv').config();
var transformer = new transformer_1.Transformer();
transformer.transform().then(function () { return process.exit(0); });
//# sourceMappingURL=index.js.map