import { CanActivate, ExecutionContext, Inject, Injectable, SetMetadata, UnauthorizedException } from '@nestjs/common';
import { Reflector } from '@nestjs/core';
import { timingSafeEqual } from 'node:crypto';
import type { Request } from 'express';

export const API_KEY = Symbol('API_KEY');
const IS_PUBLIC = 'isPublic';

/** Marks a route (e.g. health checks) as reachable without the API key. */
export const Public = () => SetMetadata(IS_PUBLIC, true);

/**
 * Global guard: when an API key is configured, every non-@Public() route
 * requires "Authorization: Bearer <key>". With no key configured the API is
 * open, which is convenient for local development.
 */
@Injectable()
export class ApiKeyGuard implements CanActivate {
  constructor(
    private readonly reflector: Reflector,
    @Inject(API_KEY) private readonly apiKey: string | undefined,
  ) {}

  canActivate(ctx: ExecutionContext): boolean {
    if (!this.apiKey) return true;
    if (this.reflector.getAllAndOverride<boolean>(IS_PUBLIC, [ctx.getHandler(), ctx.getClass()])) return true;

    const header = ctx.switchToHttp().getRequest<Request>().header('Authorization') ?? '';
    const given = Buffer.from(header.replace(/^Bearer /, ''));
    const want = Buffer.from(this.apiKey);
    if (given.length === want.length && timingSafeEqual(given, want)) return true;
    throw new UnauthorizedException('missing or invalid API key');
  }
}
