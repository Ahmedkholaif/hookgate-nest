import { createHmac, timingSafeEqual } from 'node:crypto';

/**
 * Webhook signatures: "t=<unix seconds>,v1=<hex HMAC-SHA256 of "<t>.<body>">".
 * The same scheme as hookgate (Go) and hookline, so either service verifies
 * what the other (or hookline) signs.
 */
export const SIGNATURE_HEADER = 'Hookgate-Signature';

export class SignatureError extends Error {}

export function sign(body: Buffer | string, at: Date, ...secrets: string[]): string {
  const t = String(Math.floor(at.getTime() / 1000));
  return ['t=' + t, ...secrets.map((s) => 'v1=' + mac(s, t, body).toString('hex'))].join(',');
}

export function verify(secret: string, header: string, body: Buffer, toleranceMs: number, now = new Date()): void {
  let t = '';
  const sigs: Buffer[] = [];
  for (const part of header.split(',')) {
    const i = part.indexOf('=');
    if (i < 0) throw new SignatureError('malformed header');
    const k = part.slice(0, i).trim();
    const v = part.slice(i + 1).trim();
    if (k === 't') t = v;
    else if (k === 'v1') {
      if (!/^([0-9a-f]{2})+$/i.test(v)) throw new SignatureError('malformed header');
      sigs.push(Buffer.from(v, 'hex'));
    }
  }
  const unix = Number(t);
  if (!/^-?\d+$/.test(t) || !Number.isSafeInteger(unix) || sigs.length === 0) throw new SignatureError('malformed header');
  if (Math.abs(now.getTime() - unix * 1000) > toleranceMs) throw new SignatureError('timestamp outside tolerance');
  const want = mac(secret, t, body);
  if (!sigs.some((s) => s.length === want.length && timingSafeEqual(s, want))) {
    throw new SignatureError('no matching signature');
  }
}

function mac(secret: string, t: string, body: Buffer | string) {
  return createHmac('sha256', secret).update(t).update('.').update(body).digest();
}
