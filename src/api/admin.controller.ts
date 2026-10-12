import { Body, Controller, Delete, Get, HttpCode, Inject, Param, Post, Query } from '@nestjs/common';
import { IsOptional, IsString, MaxLength } from 'class-validator';
import { StoreService } from '../store/store.service.js';
import { APP_CONFIG, type AppConfig } from '../config.js';

export class CreateSourceDto {
  @IsString() name: string;
  @IsOptional() @IsString() @MaxLength(500) secret?: string;
  @IsOptional() @IsString() @MaxLength(100) signature_header?: string;
  @IsOptional() @IsString() @MaxLength(100) id_header?: string;
  @IsOptional() @IsString() @MaxLength(200) type_field?: string;
}

export class CreateRouteDto {
  @IsString() source: string;
  @IsString() event_type: string;
  @IsString() topic: string;
}

/** Admin and status-tracking API; the same routes and shapes as the Go service. */
@Controller('v1')
export class AdminController {
  constructor(
    private readonly store: StoreService,
    @Inject(APP_CONFIG) private readonly cfg: AppConfig,
  ) {}

  @Get('stats')
  stats() {
    return this.store.stats();
  }

  /** The only response that includes the signing secret. */
  @Post('sources')
  createSource(@Body() dto: CreateSourceDto) {
    return this.store.createSource(dto);
  }

  @Get('sources')
  async listSources() {
    return { sources: (await this.store.listSources()).map(({ secret: _, ...s }) => s) };
  }

  @Get('sources/:name')
  async getSource(@Param('name') name: string) {
    const { secret: _, ...s } = await this.store.getSource(name);
    return s;
  }

  @Post('routes')
  createRoute(@Body() dto: CreateRouteDto) {
    return this.store.createRoute(dto.source, dto.event_type, dto.topic);
  }

  @Get('routes')
  async listRoutes(@Query('source') source?: string) {
    return { routes: await this.store.listRoutes(source ?? '') };
  }

  @Delete('routes/:id')
  @HttpCode(204)
  async deleteRoute(@Param('id') id: string) {
    await this.store.deleteRoute(id);
  }

  @Get('events')
  async listEvents(@Query() q: Record<string, string>) {
    return { events: await this.store.listEvents({ status: q.status, source: q.source, type: q.type, limit: Number(q.limit) || 0 }) };
  }

  @Get('events/:id')
  getEvent(@Param('id') id: string) {
    return this.store.getEvent(id);
  }

  @Post('events/:id/replay')
  @HttpCode(202)
  async replay(@Param('id') id: string) {
    await this.store.replay(id, this.cfg.eventsTopic, 'nest/api');
    return { id, status: 'retrying' };
  }

  @Get('messages/:id')
  getMessage(@Param('id') id: string) {
    return this.store.getMessage(id);
  }
}
