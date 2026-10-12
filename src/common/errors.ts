import { ArgumentsHost, Catch, ExceptionFilter, HttpException, HttpStatus, Logger } from '@nestjs/common';
import type { Response } from 'express';

export type ErrorCode = 'invalid' | 'not_found' | 'conflict';

const STATUS: Record<ErrorCode, number> = {
  invalid: HttpStatus.BAD_REQUEST,
  not_found: HttpStatus.NOT_FOUND,
  conflict: HttpStatus.CONFLICT,
};

export class HookgateError extends Error {
  constructor(
    readonly code: ErrorCode,
    message: string,
  ) {
    super(message);
  }
  static invalid = (msg: string) => new HookgateError('invalid', msg);
  static notFound = (msg = 'not found') => new HookgateError('not_found', msg);
  static conflict = (msg: string) => new HookgateError('conflict', msg);
}

/**
 * Renders every error in the Go service's shape, {"error": "<message>"}.
 * Unexpected errors become an opaque 500 and are logged.
 */
@Catch()
export class ErrorFilter implements ExceptionFilter {
  private readonly log = new Logger('ErrorFilter');

  catch(err: unknown, host: ArgumentsHost) {
    const res = host.switchToHttp().getResponse<Response>();
    let status = HttpStatus.INTERNAL_SERVER_ERROR;
    let message = 'internal error';
    if (err instanceof HookgateError) {
      status = STATUS[err.code];
      message = err.message;
    } else if (err instanceof HttpException) {
      status = err.getStatus();
      message = err.message;
      if (status === HttpStatus.PAYLOAD_TOO_LARGE) message = 'body exceeds 1 MiB';
    } else if ((err as { type?: string })?.type === 'entity.too.large') {
      status = HttpStatus.PAYLOAD_TOO_LARGE;
      message = 'body exceeds 1 MiB';
    } else {
      this.log.error(err instanceof Error ? (err.stack ?? err.message) : String(err));
    }
    res.status(status).json({ error: message });
  }
}
