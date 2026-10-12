import 'reflect-metadata';
import { NestFactory } from '@nestjs/core';
import { AppModule, configureApp } from './app.module.js';
import { openBroker } from './broker/broker.js';
import { loadConfig } from './config.js';

// ROLES=api,relay,processor,sink (default: all four), exactly as in the Go service.
const cfg = loadConfig();
const broker = cfg.roles.some((r) => r !== 'api') ? await openBroker() : null;
const app = configureApp(await NestFactory.create(AppModule.forRoot(cfg, broker), { bodyParser: false }));
await app.listen(Number(process.env.PORT ?? 3000));
console.log(`hookgate-nest listening on ${await app.getUrl()}`);
