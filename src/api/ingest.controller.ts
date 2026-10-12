import { Controller, HttpCode, Inject, Param, Post, Req, Res } from '@nestjs/common';
import { createHash } from 'node:crypto';
import type { Request, Response } from 'express';
import { Public } from '../common/api-key.guard.js';
import { HookgateError } from '../common/errors.js';
import { SignatureError, verify } from '../common/signature.js';
import { StoreService } from '../store/store.service.js';
import { APP_CONFIG, type AppConfig } from '../config.js';

const SIGNATURE_TOLERANCE_MS = 5 * 60_000;

/** The provider's event id when it sends one, else a hash of the body. */
export function dedupKey(headerId: string | undefined, body: Buffer): string {
  const id = (headerId ?? '').trim();
  if (id && id.length <= 200) return 'id:' + id;
  return 'sha256:' + createHash('sha256').update(body).digest('hex');
}

/** Reads a dotted path ("type", "data.object.type") from the payload, else "unknown". */
export function eventType(payload: Record<string, unknown>, path: string): string {
  let cur: unknown = payload;
  for (const p of path.split('.')) {
    if (!cur || typeof cur !== 'object' || Array.isArray(cur) || !(p in cur)) return 'unknown';
    cur = (cur as Record<string, unknown>)[p];
  }
  return typeof cur === 'string' && cur !== '' && cur.length <= 200 ? cur : 'unknown';
}

@Controller('v1/ingest')
export class IngestController {
  constructor(
    private readonly store: StoreService,
    @Inject(APP_CONFIG) private readonly cfg: AppConfig,
  ) {}

  /**
   * Verifies the signature, derives the dedup key and event type, then stores
   * the event and its outbox row atomically. It never calls the broker, so a
   * broker outage doesn't fail the provider's request.
   */
  @Post(':source')
  @Public()
  @HttpCode(202)
  async ingest(@Param('source') sourceName: string, @Req() req: Request, @Res({ passthrough: true }) res: Response) {
    const src = await this.store.getSource(sourceName);
    const body: Buffer = Buffer.isBuffer(req.body) ? req.body : Buffer.alloc(0); // raw parser, see configureApp
    try {
      verify(src.secret!, req.header(src.signature_header) ?? '', body, SIGNATURE_TOLERANCE_MS);
    } catch (err) {
      if (err instanceof SignatureError) {
        res.status(401);
        return { error: 'invalid signature: ' + err.message };
      }
      throw err;
    }
    let payload: unknown;
    try {
      payload = JSON.parse(body.toString('utf8'));
    } catch {
      payload = undefined;
    }
    if (!payload || typeof payload !== 'object' || Array.isArray(payload)) throw HookgateError.invalid('body must be a JSON object');

    const result = await this.store.ingest(
      src.name,
      dedupKey(req.header(src.id_header), body),
      eventType(payload as Record<string, unknown>, src.type_field),
      body,
      this.cfg.eventsTopic,
      'nest/api',
    );
    if (result.duplicate) res.status(200);
    return result;
  }
}
