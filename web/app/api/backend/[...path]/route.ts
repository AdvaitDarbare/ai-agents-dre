import { NextRequest } from 'next/server';

export const dynamic = 'force-dynamic';

function backendBase(): string {
  // Prefer explicit backend URL in env.
  return process.env.BACKEND_URL || process.env.NEXT_PUBLIC_BACKEND_URL || '';
}

async function forward(request: NextRequest, pathParts: string[]) {
  const suffix = pathParts.map((p) => encodeURIComponent(p)).join('/');
  const configuredBase = backendBase().replace(/\/+$/, '');
  const candidateBases = configuredBase
    ? [configuredBase]
    : [
        // Local fallback order: prefer the project default backend first.
        'http://127.0.0.1:8000',
        'http://127.0.0.1:8011',
      ];

  // Copy body for non-GET/HEAD requests. For GET/HEAD, some runtimes reject a body.
  const method = request.method.toUpperCase();
  const body = method === 'GET' || method === 'HEAD' ? undefined : await request.arrayBuffer();

  let upstream: Response | null = null;
  let lastError: unknown = null;
  for (const base of candidateBases) {
    const url = `${base}/${suffix}${request.nextUrl.search}`;
    try {
      const resp = await fetch(url, {
        method,
        // Keep content-type for JSON; drop host/referer-ish headers.
        headers: {
          'content-type': request.headers.get('content-type') || 'application/json',
        },
        body,
        cache: 'no-store',
      });
      upstream = resp;
      // In local fallback mode, try next candidate when endpoint is missing.
      if (!configuredBase && resp.status === 404) continue;
      break;
    } catch (error) {
      lastError = error;
    }
  }

  if (!upstream) {
    return new Response(
      JSON.stringify({
        detail: `Backend unavailable: ${lastError instanceof Error ? lastError.message : 'unknown error'}`,
      }),
      {
        status: 502,
        headers: {
          'content-type': 'application/json; charset=utf-8',
          'cache-control': 'no-cache, no-transform',
        },
      },
    );
  }

  // Pass through body and content-type. We intentionally do not pass upstream CORS headers.
  return new Response(upstream.body, {
    status: upstream.status,
    headers: {
      'content-type': upstream.headers.get('content-type') || 'application/json; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
    },
  });
}

export async function GET(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return forward(request, path);
}

export async function POST(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return forward(request, path);
}

export async function PATCH(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return forward(request, path);
}

export async function DELETE(request: NextRequest, context: { params: Promise<{ path: string[] }> }) {
  const { path } = await context.params;
  return forward(request, path);
}
