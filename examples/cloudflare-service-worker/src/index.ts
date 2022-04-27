// import { Redis } from "@upstash/redis/cloudflare";


addEventListener("fetch", (event) => {
  event.respondWith(handleRequest(event.request));
});

async function handleRequest(request: Request) {
  const url = new URL(request.url);
  
  if (url.pathname !== "/") {
    return new Response();
  }
  
  // const redis = Redis.fromEnv();
  const count = 111; //await redis.incr("cloudflare-workers-count");

  return new Response(html(count), {
    headers: { "content-type": "text/html;charset=UTF-8" },
  });
}

const html = (count: number) => `
  <h1>Cloudflare Workers with Upstash Redis</h1>
  <h2>Count: ${count}</h2>
`;
