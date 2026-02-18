import { NextRequest } from 'next/server';

const BACKEND_URL = process.env.NEXT_PUBLIC_BACKEND_URL || 'http://127.0.0.1:8000';

export async function POST(request: NextRequest) {
  const body = await request.text();

  const upstream = await fetch(`${BACKEND_URL}/chat/stream`, {
    method: 'POST',
    headers: {
      'content-type': request.headers.get('content-type') || 'application/json',
    },
    body,
  });

  return new Response(upstream.body, {
    status: upstream.status,
    headers: {
      'content-type': upstream.headers.get('content-type') || 'text/plain; charset=utf-8',
      'cache-control': 'no-cache, no-transform',
    },
  });
}
