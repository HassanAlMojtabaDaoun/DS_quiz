// Copies index.html and styles.css next to the bundle in dist/.
import { cpSync, mkdirSync } from "node:fs";
mkdirSync("dist", { recursive: true });
cpSync("static", "dist", { recursive: true });
