import { UpstashError } from "./error";
import { Telemetry } from "./types";

type CacheSetting =
  | "default"
  | "force-cache"
  | "no-cache"
  | "no-store"
  | "only-if-cached"
  | "reload";

export type UpstashRequest = {
  path?: string[];
  /**
   * Request body will be serialized to json
   */
  body?: unknown;
};
export type UpstashResponse<TResult> = { result?: TResult; error?: string };

export interface Requester {
  request: <TResult = unknown>(req: UpstashRequest) => Promise<UpstashResponse<TResult>>;
}

type ResultError = {
  result?: string | number | null | (string | number | null)[];
  error?: string;
};
export type RetryConfig =
  | false
  | {
      /**
       * The number of retries to attempt before giving up.
       *
       * @default 5
       */
      retries?: number;
      /**
       * A backoff function receives the current retry cound and returns a number in milliseconds to wait before retrying.
       *
       * @default
       * ```ts
       * Math.exp(retryCount) * 50
       * ```
       */
      backoff?: (retryCount: number) => number;
    };

export type Options = {
  backend?: string;
};

export type RequesterConfig = {
  /**
   * Configure the retry behaviour in case of network errors
   */
  retry?: RetryConfig;

  /**
   * Due to the nature of dynamic and custom data, it is possible to write data to redis that is not
   * valid json and will therefore cause errors when deserializing. This used to happen very
   * frequently with non-utf8 data, such as emojis.
   *
   * By default we will therefore encode the data as base64 on the server, before sending it to the
   * client. The client will then decode the base64 data and parse it as utf8.
   *
   * For very large entries, this can add a few milliseconds, so if you are sure that your data is
   * valid utf8, you can disable this behaviour by setting this option to false.
   *
   * Here's what the response body looks like:
   *
   * ```json
   * {
   *  result?: "base64-encoded",
   *  error?: string
   * }
   * ```
   *
   * @default "base64"
   */
  responseEncoding?: false | "base64";

  /**
   * Configure the cache behaviour
   * @default "no-store"
   */
  cache?: CacheSetting;
};

export type HttpClientConfig = {
  headers?: Record<string, string>;
  baseUrl: string;
  options?: Options;
  retry?: RetryConfig;
  agent?: any;
  timeout?: number;
} & RequesterConfig;

export class HttpClient implements Requester {
  public baseUrl: string;
  public headers: Record<string, string>;
  public readonly options: {
    backend?: string;
    agent: any;
    timeout?: number;
    responseEncoding?: false | "base64";
    cache?: CacheSetting;
  };

  public readonly retry: {
    attempts: number;
    backoff: (retryCount: number) => number;
  };

  public constructor(config: HttpClientConfig) {
    this.options = {
      backend: config.options?.backend,
      agent: config.agent,
      responseEncoding: config.responseEncoding ?? "base64", // default to base64
      cache: config.cache,
      timeout: config.timeout,
    };

    this.baseUrl = config.baseUrl.replace(/\/$/, "");

    this.headers = {
      "Content-Type": "application/json",

      ...config.headers,
    };

    if (this.options.responseEncoding === "base64") {
      this.headers["Upstash-Encoding"] = "base64";
    }

    if (typeof config?.retry === "boolean" && config?.retry === false) {
      this.retry = {
        attempts: 1,
        backoff: () => 0,
      };
    } else {
      this.retry = {
        attempts: config?.retry?.retries ?? 5,
        backoff: config?.retry?.backoff ?? ((retryCount) => Math.exp(retryCount) * 50),
      };
    }
  }

  public mergeTelemetry(telemetry: Telemetry): void {
    function merge(
      obj: Record<string, string>,
      key: string,
      value?: string,
    ): Record<string, string> {
      if (!value) {
        return obj;
      }
      if (obj[key]) {
        obj[key] = [obj[key], value].join(",");
      } else {
        obj[key] = value;
      }
      return obj;
    }

    this.headers = merge(this.headers, "Upstash-Telemetry-Runtime", telemetry.runtime);
    this.headers = merge(this.headers, "Upstash-Telemetry-Platform", telemetry.platform);
    this.headers = merge(this.headers, "Upstash-Telemetry-Sdk", telemetry.sdk);
  }

  public async request<TResult>(req: UpstashRequest): Promise<UpstashResponse<TResult>> {
    const requestOptions: RequestInit & { backend?: string; agent?: any } = {
      cache: this.options.cache,
      method: "POST",
      headers: this.headers,
      body: JSON.stringify(req.body),
      keepalive: true,
      agent: this.options?.agent,
      signal:
        this.options.timeout && this.options.timeout > 0
          ? AbortSignal.timeout(this.options.timeout)
          : null,

      /**
       * Fastly specific
       */
      backend: this.options?.backend,
    };

    let res: Response | null = null;
    let error: Error | null = null;
    for (let i = 0; i <= this.retry.attempts; i++) {
      try {
        res = await fetch([this.baseUrl, ...(req.path ?? [])].join("/"), requestOptions);
        break;
      } catch (err) {
        if (requestOptions.signal?.aborted) {
          const myBlob = new Blob([
            JSON.stringify({ result: requestOptions.signal.reason ?? "Aborted" }),
          ]);
          const myOptions = {
            status: 200,
            statusText: requestOptions.signal.reason ?? "Aborted",
          };
          res = new Response(myBlob, myOptions);
          break;
        }
        error = err as Error;
        await new Promise((r) => setTimeout(r, this.retry.backoff(i)));
      }
    }
    if (!res) {
      throw error ?? new Error("Exhausted all retries");
    }

    const body = (await res.json()) as UpstashResponse<string>;
    if (!res.ok) {
      throw new UpstashError(`${body.error}, command was: ${JSON.stringify(req.body)}`);
    }

    if (this.options?.responseEncoding === "base64") {
      if (Array.isArray(body)) {
        return body.map(({ result, error }) => ({
          result: decode(result),
          error,
        })) as UpstashResponse<TResult>;
      }
      const result = decode(body.result) as any;
      return { result, error: body.error };
    }
    return body as UpstashResponse<TResult>;
  }
}

function base64decode(b64: string): string {
  let dec = "";
  try {
    /**
     * Using only atob() is not enough because it doesn't work with unicode characters
     */
    const binString = atob(b64);
    const size = binString.length;
    const bytes = new Uint8Array(size);
    for (let i = 0; i < size; i++) {
      bytes[i] = binString.charCodeAt(i);
    }
    dec = new TextDecoder().decode(bytes);
  } catch {
    dec = b64;
  }
  return dec;
  // try {
  //   return decodeURIComponent(dec);
  // } catch {
  //   return dec;
  // }
}

function decode(raw: ResultError["result"]): ResultError["result"] {
  let result: any = undefined;
  switch (typeof raw) {
    case "undefined":
      return raw;

    case "number": {
      result = raw;
      break;
    }
    case "object": {
      if (Array.isArray(raw)) {
        result = raw.map((v) =>
          typeof v === "string" ? base64decode(v) : Array.isArray(v) ? v.map(decode) : v,
        );
      } else {
        // If it's not an array it must be null
        // Apparently null is an object in javascript
        result = null;
      }
      break;
    }

    case "string": {
      result = raw === "OK" ? "OK" : base64decode(raw);
      break;
    }

    default:
      break;
  }

  return result;
}
