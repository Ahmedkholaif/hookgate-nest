import { DynamicModule, Global, Module } from '@nestjs/common';
import { DATABASE_OPTIONS, DatabaseOptions, DatabaseService } from './database.service.js';

@Global()
@Module({})
export class DatabaseModule {
  static forRoot(options: DatabaseOptions): DynamicModule {
    return {
      module: DatabaseModule,
      providers: [{ provide: DATABASE_OPTIONS, useValue: options }, DatabaseService],
      exports: [DatabaseService],
    };
  }
}
