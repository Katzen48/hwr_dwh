import {Transformer} from "./transformer";

require('dotenv').config();

let transformer = new Transformer();
transformer.transform().then(() => process.exit(0));