import { Controller, DynamicModule, Get, INestApplication, Module, ValidationPipe } from '@nestjs/common';
import { APP_FILTER, APP_GUARD } from '@nestjs/core';
import express from 'express';
import { AdminController } from './api/admin.controller.js';
import { IngestController } from './api/ingest.controller.js';
import { BROKER, Broker } from './broker/broker.js';
import { API_KEY, ApiKeyGuard, Public } from './common/api-key.guard.js';
import { ErrorFilter, HookgateError } from './common/errors.js';
import { APP_CONFIG, AppConfig } from './config.js';
import { DatabaseModule } from './database/database.module.js';
import { DatabaseService } from './database/database.service.js';
import { StoreService } from './store/store.service.js';
import { ProcessorWorker, RelayWorker, SinkWorker, WorkerRunner } from './workers/workers.js';

@Controller('healthz')
class HealthController {
  constructor(private readonly db: DatabaseService) {}

  @Get()
  @Public()
  async check() {
    await this.db.query('SELECT 1');
    return 'ok';
  }
}

@Module({})
export class AppModule {
  /** `broker` is null when only the api role runs (it never talks to the broker). */
  static forRoot(cfg: AppConfig, broker: Broker | null): DynamicModule {
    const api = cfg.roles.includes('api');
    return {
      module: AppModule,
      imports: [DatabaseModule.forRoot({ url: cfg.databaseUrl, maxConnections: cfg.dbMaxConns })],
      controllers: api ? [HealthController, IngestController, AdminController] : [HealthController],
      providers: [
        StoreService,
        { provide: APP_CONFIG, useValue: cfg },
        { provide: API_KEY, useValue: cfg.apiKey },
        { provide: APP_GUARD, useClass: ApiKeyGuard },
        { provide: APP_FILTER, useClass: ErrorFilter },
        ...(broker ? [{ provide: BROKER, useValue: broker }, RelayWorker, ProcessorWorker, SinkWorker, WorkerRunner] : []),
      ],
    };
  }
}

/**
 * HTTP setup shared by main.ts and the tests. Ingest needs the exact bytes
 * the provider signed, so /v1/ingest gets a raw parser and everything else
 * a small JSON parser (create the app with bodyParser: false).
 */
export function configureApp(app: INestApplication) {
  app.use('/v1/ingest', express.raw({ type: () => true, limit: '1mb' }));
  app.use(express.json({ limit: '64kb' }));
  app.useGlobalPipes(
    new ValidationPipe({
      whitelist: true,
      forbidNonWhitelisted: true,
      exceptionFactory: (errors) =>
        HookgateError.invalid(errors.flatMap((e) => Object.values(e.constraints ?? {})).join('; ') || 'validation failed'),
    }),
  );
  app.enableShutdownHooks();
  return app;
}
