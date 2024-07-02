import { createAutoPipelineProxy } from "../pkg/auto-pipeline";
import {
  AppendCommand,
  BitCountCommand,
  BitFieldCommand,
  BitOpCommand,
  BitPosCommand,
  CommandOptions,
  CopyCommand,
  DBSizeCommand,
  DecrByCommand,
  DecrCommand,
  DelCommand,
  EchoCommand,
  EvalCommand,
  EvalshaCommand,
  ExistsCommand,
  ExpireAtCommand,
  ExpireCommand,
  FlushAllCommand,
  FlushDBCommand,
  GeoAddCommand,
  GeoDistCommand,
  GeoHashCommand,
  GeoPosCommand,
  GeoSearchCommand,
  GeoSearchStoreCommand,
  GetBitCommand,
  GetCommand,
  GetDelCommand,
  GetRangeCommand,
  GetSetCommand,
  HDelCommand,
  HExistsCommand,
  HGetAllCommand,
  HGetCommand,
  HIncrByCommand,
  HIncrByFloatCommand,
  HKeysCommand,
  HLenCommand,
  HMGetCommand,
  HMSetCommand,
  HRandFieldCommand,
  HScanCommand,
  HSetCommand,
  HSetNXCommand,
  HStrLenCommand,
  HValsCommand,
  IncrByCommand,
  IncrByFloatCommand,
  IncrCommand,
  JsonArrAppendCommand,
  JsonArrIndexCommand,
  JsonArrInsertCommand,
  JsonArrLenCommand,
  JsonArrPopCommand,
  JsonArrTrimCommand,
  JsonClearCommand,
  JsonDelCommand,
  JsonForgetCommand,
  JsonGetCommand,
  JsonMGetCommand,
  JsonNumIncrByCommand,
  JsonNumMultByCommand,
  JsonObjKeysCommand,
  JsonObjLenCommand,
  JsonRespCommand,
  JsonSetCommand,
  JsonStrAppendCommand,
  JsonStrLenCommand,
  JsonToggleCommand,
  JsonTypeCommand,
  KeysCommand,
  LIndexCommand,
  LInsertCommand,
  LLenCommand,
  LMoveCommand,
  LPopCommand,
  LPosCommand,
  LPushCommand,
  LPushXCommand,
  LRangeCommand,
  LRemCommand,
  LSetCommand,
  LTrimCommand,
  LmPopCommand,
  MGetCommand,
  MSetCommand,
  MSetNXCommand,
  PExpireAtCommand,
  PExpireCommand,
  PSetEXCommand,
  PTtlCommand,
  PersistCommand,
  PfAddCommand,
  PfCountCommand,
  PfMergeCommand,
  PingCommand,
  PublishCommand,
  RPopCommand,
  RPushCommand,
  RPushXCommand,
  RandomKeyCommand,
  RenameCommand,
  RenameNXCommand,
  SAddCommand,
  SCardCommand,
  SDiffCommand,
  SDiffStoreCommand,
  SInterCommand,
  SInterStoreCommand,
  SIsMemberCommand,
  SMIsMemberCommand,
  SMembersCommand,
  SMoveCommand,
  SPopCommand,
  SRandMemberCommand,
  SRemCommand,
  SScanCommand,
  SUnionCommand,
  SUnionStoreCommand,
  ScanCommand,
  ScoreMember,
  ScriptExistsCommand,
  ScriptFlushCommand,
  ScriptLoadCommand,
  SetBitCommand,
  SetCommand,
  SetCommandOptions,
  SetExCommand,
  SetNxCommand,
  SetRangeCommand,
  StrLenCommand,
  TimeCommand,
  TouchCommand,
  TtlCommand,
  TypeCommand,
  UnlinkCommand,
  XAckCommand,
  XAddCommand,
  XAutoClaim,
  XClaimCommand,
  XDelCommand,
  XGroupCommand,
  XInfoCommand,
  XLenCommand,
  XPendingCommand,
  XRangeCommand,
  XReadCommand,
  XReadGroupCommand,
  XRevRangeCommand,
  XTrimCommand,
  ZAddCommand,
  ZAddCommandOptions,
  ZCardCommand,
  ZCountCommand,
  ZIncrByCommand,
  ZInterStoreCommand,
  ZLexCountCommand,
  ZPopMaxCommand,
  ZPopMinCommand,
  ZRangeCommand,
  ZRangeCommandOptions,
  ZRankCommand,
  ZRemCommand,
  ZRemRangeByLexCommand,
  ZRemRangeByRankCommand,
  ZRemRangeByScoreCommand,
  ZRevRankCommand,
  ZScanCommand,
  ZScoreCommand,
  ZUnionCommand,
  ZUnionStoreCommand,
} from "./commands/mod";
import { ZDiffStoreCommand } from "./commands/zdiffstore";
import { ZMScoreCommand } from "./commands/zmscore";
import { Requester, UpstashRequest, UpstashResponse } from "./http";
import { Pipeline } from "./pipeline";
import { Script } from "./script";
import type { CommandArgs, RedisOptions, Telemetry } from "./types";

// See https://github.com/upstash/upstash-redis/issues/342
// why we need this export
export type { RedisOptions } from "./types";

/**
 * Serverless redis client for upstash.
 */
export class Redis {
  protected client: Requester;
  protected opts?: CommandOptions<any, any>;
  protected enableTelemetry: boolean;
  protected enableAutoPipelining: boolean;

  /**
   * Create a new redis client
   *
   * @example
   * ```typescript
   * const redis = new Redis({
   *  url: "<UPSTASH_REDIS_REST_URL>",
   *  token: "<UPSTASH_REDIS_REST_TOKEN>",
   * });
   * ```
   */
  constructor(client: Requester, opts?: RedisOptions) {
    this.client = client;
    this.opts = opts;
    this.enableTelemetry = opts?.enableTelemetry ?? true;
    this.enableAutoPipelining = opts?.enableAutoPipelining ?? false;
  }

  get json() {
    return {
      /**
       * @see https://redis.io/commands/json.arrappend
       */
      arrappend: (...args: CommandArgs<typeof JsonArrAppendCommand>) =>
        new JsonArrAppendCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.arrindex
       */
      arrindex: (...args: CommandArgs<typeof JsonArrIndexCommand>) =>
        new JsonArrIndexCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.arrinsert
       */
      arrinsert: (...args: CommandArgs<typeof JsonArrInsertCommand>) =>
        new JsonArrInsertCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.arrlen
       */
      arrlen: (...args: CommandArgs<typeof JsonArrLenCommand>) =>
        new JsonArrLenCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.arrpop
       */
      arrpop: (...args: CommandArgs<typeof JsonArrPopCommand>) =>
        new JsonArrPopCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.arrtrim
       */
      arrtrim: (...args: CommandArgs<typeof JsonArrTrimCommand>) =>
        new JsonArrTrimCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.clear
       */
      clear: (...args: CommandArgs<typeof JsonClearCommand>) =>
        new JsonClearCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.del
       */
      del: (...args: CommandArgs<typeof JsonDelCommand>) =>
        new JsonDelCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.forget
       */
      forget: (...args: CommandArgs<typeof JsonForgetCommand>) =>
        new JsonForgetCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.get
       */
      get: <TData>(...args: CommandArgs<typeof JsonGetCommand>) =>
        new JsonGetCommand<TData>(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.mget
       */
      mget: <TData>(...args: CommandArgs<typeof JsonMGetCommand>) =>
        new JsonMGetCommand<TData>(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.numincrby
       */
      numincrby: (...args: CommandArgs<typeof JsonNumIncrByCommand>) =>
        new JsonNumIncrByCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.nummultby
       */
      nummultby: (...args: CommandArgs<typeof JsonNumMultByCommand>) =>
        new JsonNumMultByCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.objkeys
       */
      objkeys: (...args: CommandArgs<typeof JsonObjKeysCommand>) =>
        new JsonObjKeysCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.objlen
       */
      objlen: (...args: CommandArgs<typeof JsonObjLenCommand>) =>
        new JsonObjLenCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.resp
       */
      resp: (...args: CommandArgs<typeof JsonRespCommand>) =>
        new JsonRespCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.set
       */
      set: (...args: CommandArgs<typeof JsonSetCommand>) =>
        new JsonSetCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.strappend
       */
      strappend: (...args: CommandArgs<typeof JsonStrAppendCommand>) =>
        new JsonStrAppendCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.strlen
       */
      strlen: (...args: CommandArgs<typeof JsonStrLenCommand>) =>
        new JsonStrLenCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.toggle
       */
      toggle: (...args: CommandArgs<typeof JsonToggleCommand>) =>
        new JsonToggleCommand(args, this.opts).exec(this.client),

      /**
       * @see https://redis.io/commands/json.type
       */
      type: (...args: CommandArgs<typeof JsonTypeCommand>) =>
        new JsonTypeCommand(args, this.opts).exec(this.client),
    };
  }
  /**
   * Wrap a new middleware around the HTTP client.
   */
  use = <TResult = unknown>(
    middleware: (
      r: UpstashRequest,
      next: <TResult = unknown>(req: UpstashRequest) => Promise<UpstashResponse<TResult>>,
    ) => Promise<UpstashResponse<TResult>>,
  ) => {
    const makeRequest = this.client.request.bind(this.client);
    this.client.request = (req: UpstashRequest) => middleware(req, makeRequest) as any;
  };

  /**
   * Technically this is not private, we can hide it from intellisense by doing this
   */
  protected addTelemetry = (telemetry: Telemetry) => {
    if (!this.enableTelemetry) {
      return;
    }
    try {
      // @ts-ignore - The `Requester` interface does not know about this method but it will be there
      // as long as the user uses the standard HttpClient
      this.client.mergeTelemetry(telemetry);
    } catch {
      // ignore
    }
  };

  createScript(script: string): Script {
    return new Script(this, script);
  }
  /**
   * Create a new pipeline that allows you to send requests in bulk.
   *
   * @see {@link Pipeline}
   */
  pipeline = () =>
    new Pipeline({
      client: this.client,
      commandOptions: this.opts,
      multiExec: false,
    });

  protected autoPipeline = () => {
    return createAutoPipelineProxy(this);
  };

  /**
   * Create a new transaction to allow executing multiple steps atomically.
   *
   * All the commands in a transaction are serialized and executed sequentially. A request sent by
   * another client will never be served in the middle of the execution of a Redis Transaction. This
   * guarantees that the commands are executed as a single isolated operation.
   *
   * @see {@link Pipeline}
   */
  multi = () =>
    new Pipeline({
      client: this.client,
      commandOptions: this.opts,
      multiExec: true,
    });

  /**
   * Returns an instance that can be used to execute `BITFIELD` commands on one key.
   *
   * @example
   * ```typescript
   * redis.set("mykey", 0);
   * const result = await redis.bitfield("mykey")
   *   .set("u4", 0, 16)
   *   .incr("u4", "#1", 1)
   *   .exec();
   * console.log(result); // [0, 1]
   * ```
   *
   * @see https://redis.io/commands/bitfield
   */
  bitfield = (...args: CommandArgs<typeof BitFieldCommand>) =>
    new BitFieldCommand(args, this.client, this.opts);

  /**
   * @see https://redis.io/commands/append
   */
  append = (...args: CommandArgs<typeof AppendCommand>) =>
    new AppendCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/bitcount
   */
  bitcount = (...args: CommandArgs<typeof BitCountCommand>) =>
    new BitCountCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/bitop
   */
  bitop: {
    (
      op: "and" | "or" | "xor",
      destinationKey: string,
      sourceKey: string,
      ...sourceKeys: string[]
    ): Promise<number>;
    (op: "not", destinationKey: string, sourceKey: string): Promise<number>;
  } = (
    op: "and" | "or" | "xor" | "not",
    destinationKey: string,
    sourceKey: string,
    ...sourceKeys: string[]
  ) =>
    new BitOpCommand([op as any, destinationKey, sourceKey, ...sourceKeys], this.opts).exec(
      this.client,
    );

  /**
   * @see https://redis.io/commands/bitpos
   */
  bitpos = (...args: CommandArgs<typeof BitPosCommand>) =>
    new BitPosCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/copy
   */
  copy = (...args: CommandArgs<typeof CopyCommand>) =>
    new CopyCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/dbsize
   */
  dbsize = () => new DBSizeCommand(this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/decr
   */
  decr = (...args: CommandArgs<typeof DecrCommand>) =>
    new DecrCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/decrby
   */
  decrby = (...args: CommandArgs<typeof DecrByCommand>) =>
    new DecrByCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/del
   */
  del = (...args: CommandArgs<typeof DelCommand>) =>
    new DelCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/echo
   */
  echo = (...args: CommandArgs<typeof EchoCommand>) =>
    new EchoCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/eval
   */
  eval = <TArgs extends unknown[], TData = unknown>(
    ...args: [script: string, keys: string[], args: TArgs]
  ) => new EvalCommand<TArgs, TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/evalsha
   */
  evalsha = <TArgs extends unknown[], TData = unknown>(
    ...args: [sha1: string, keys: string[], args: TArgs]
  ) => new EvalshaCommand<TArgs, TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/exists
   */
  exists = (...args: CommandArgs<typeof ExistsCommand>) =>
    new ExistsCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/expire
   */
  expire = (...args: CommandArgs<typeof ExpireCommand>) =>
    new ExpireCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/expireat
   */
  expireat = (...args: CommandArgs<typeof ExpireAtCommand>) =>
    new ExpireAtCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/flushall
   */
  flushall = (args?: CommandArgs<typeof FlushAllCommand>) =>
    new FlushAllCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/flushdb
   */
  flushdb = (...args: CommandArgs<typeof FlushDBCommand>) =>
    new FlushDBCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/geoadd
   */
  geoadd = <TData>(...args: CommandArgs<typeof GeoAddCommand<TData>>) =>
    new GeoAddCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/geopos
   */
  geopos = <TData>(...args: CommandArgs<typeof GeoPosCommand<TData>>) =>
    new GeoPosCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/geodist
   */
  geodist = <TData>(...args: CommandArgs<typeof GeoDistCommand<TData>>) =>
    new GeoDistCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/geohash
   */
  geohash = <TData>(...args: CommandArgs<typeof GeoHashCommand<TData>>) =>
    new GeoHashCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/geosearch
   */
  geosearch = <TData>(...args: CommandArgs<typeof GeoSearchCommand<TData>>) =>
    new GeoSearchCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/geosearchstore
   */
  geosearchstore = <TData>(...args: CommandArgs<typeof GeoSearchStoreCommand<TData>>) =>
    new GeoSearchStoreCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/get
   */
  get = <TData>(...args: CommandArgs<typeof GetCommand>) =>
    new GetCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/getbit
   */
  getbit = (...args: CommandArgs<typeof GetBitCommand>) =>
    new GetBitCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/getdel
   */
  getdel = <TData>(...args: CommandArgs<typeof GetDelCommand>) =>
    new GetDelCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/getrange
   */
  getrange = (...args: CommandArgs<typeof GetRangeCommand>) =>
    new GetRangeCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/getset
   */
  getset = <TData>(key: string, value: TData) =>
    new GetSetCommand<TData>([key, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hdel
   */
  hdel = (...args: CommandArgs<typeof HDelCommand>) =>
    new HDelCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hexists
   */
  hexists = (...args: CommandArgs<typeof HExistsCommand>) =>
    new HExistsCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hget
   */
  hget = <TData>(...args: CommandArgs<typeof HGetCommand>) =>
    new HGetCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hgetall
   */
  hgetall = <TData extends Record<string, unknown>>(...args: CommandArgs<typeof HGetAllCommand>) =>
    new HGetAllCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hincrby
   */
  hincrby = (...args: CommandArgs<typeof HIncrByCommand>) =>
    new HIncrByCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hincrbyfloat
   */
  hincrbyfloat = (...args: CommandArgs<typeof HIncrByFloatCommand>) =>
    new HIncrByFloatCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hkeys
   */
  hkeys = (...args: CommandArgs<typeof HKeysCommand>) =>
    new HKeysCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hlen
   */
  hlen = (...args: CommandArgs<typeof HLenCommand>) =>
    new HLenCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hmget
   */
  hmget = <TData extends Record<string, unknown>>(...args: CommandArgs<typeof HMGetCommand>) =>
    new HMGetCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hmset
   */
  hmset = <TData>(key: string, kv: { [field: string]: TData }) =>
    new HMSetCommand([key, kv], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hrandfield
   */
  hrandfield: {
    (key: string): Promise<string | null>;
    (key: string, count: number): Promise<string[]>;
    <TData extends Record<string, unknown>>(
      key: string,
      count: number,
      withValues: boolean,
    ): Promise<Partial<TData>>;
  } = <TData extends string | string[] | Record<string, unknown>>(
    key: string,
    count?: number,
    withValues?: boolean,
  ) => new HRandFieldCommand<TData>([key, count, withValues] as any, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hscan
   */
  hscan = (...args: CommandArgs<typeof HScanCommand>) =>
    new HScanCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hset
   */
  hset = <TData>(key: string, kv: { [field: string]: TData }) =>
    new HSetCommand<TData>([key, kv], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hsetnx
   */
  hsetnx = <TData>(key: string, field: string, value: TData) =>
    new HSetNXCommand<TData>([key, field, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hstrlen
   */
  hstrlen = (...args: CommandArgs<typeof HStrLenCommand>) =>
    new HStrLenCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/hvals
   */
  hvals = (...args: CommandArgs<typeof HValsCommand>) =>
    new HValsCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/incr
   */
  incr = (...args: CommandArgs<typeof IncrCommand>) =>
    new IncrCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/incrby
   */
  incrby = (...args: CommandArgs<typeof IncrByCommand>) =>
    new IncrByCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/incrbyfloat
   */
  incrbyfloat = (...args: CommandArgs<typeof IncrByFloatCommand>) =>
    new IncrByFloatCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/keys
   */
  keys = (...args: CommandArgs<typeof KeysCommand>) =>
    new KeysCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lindex
   */
  lindex = (...args: CommandArgs<typeof LIndexCommand>) =>
    new LIndexCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/linsert
   */
  linsert = <TData>(key: string, direction: "before" | "after", pivot: TData, value: TData) =>
    new LInsertCommand<TData>([key, direction, pivot, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/llen
   */
  llen = (...args: CommandArgs<typeof LLenCommand>) =>
    new LLenCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lmove
   */
  lmove = <TData = string>(...args: CommandArgs<typeof LMoveCommand>) =>
    new LMoveCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lpop
   */
  lpop = <TData>(...args: CommandArgs<typeof LPopCommand>) =>
    new LPopCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lmpop
   */
  lmpop = <TData>(...args: CommandArgs<typeof LmPopCommand>) =>
    new LmPopCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lpos
   */
  lpos = <TData = number>(...args: CommandArgs<typeof LPosCommand>) =>
    new LPosCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lpush
   */
  lpush = <TData>(key: string, ...elements: TData[]) =>
    new LPushCommand<TData>([key, ...elements], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lpushx
   */
  lpushx = <TData>(key: string, ...elements: TData[]) =>
    new LPushXCommand<TData>([key, ...elements], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lrange
   */
  lrange = <TResult = string>(...args: CommandArgs<typeof LRangeCommand>) =>
    new LRangeCommand<TResult>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lrem
   */
  lrem = <TData>(key: string, count: number, value: TData) =>
    new LRemCommand([key, count, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/lset
   */
  lset = <TData>(key: string, index: number, value: TData) =>
    new LSetCommand([key, index, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/ltrim
   */
  ltrim = (...args: CommandArgs<typeof LTrimCommand>) =>
    new LTrimCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/mget
   */
  mget = <TData extends unknown[]>(...args: CommandArgs<typeof MGetCommand>) =>
    new MGetCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/mset
   */
  mset = <TData>(kv: { [key: string]: TData }) =>
    new MSetCommand<TData>([kv], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/msetnx
   */
  msetnx = <TData>(kv: { [key: string]: TData }) =>
    new MSetNXCommand<TData>([kv], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/persist
   */
  persist = (...args: CommandArgs<typeof PersistCommand>) =>
    new PersistCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/pexpire
   */
  pexpire = (...args: CommandArgs<typeof PExpireCommand>) =>
    new PExpireCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/pexpireat
   */
  pexpireat = (...args: CommandArgs<typeof PExpireAtCommand>) =>
    new PExpireAtCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/pfadd
   */
  pfadd = (...args: CommandArgs<typeof PfAddCommand>) =>
    new PfAddCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/pfcount
   */
  pfcount = (...args: CommandArgs<typeof PfCountCommand>) =>
    new PfCountCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/pfmerge
   */
  pfmerge = (...args: CommandArgs<typeof PfMergeCommand>) =>
    new PfMergeCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/ping
   */
  ping = (args?: CommandArgs<typeof PingCommand>) =>
    new PingCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/psetex
   */
  psetex = <TData>(key: string, ttl: number, value: TData) =>
    new PSetEXCommand<TData>([key, ttl, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/pttl
   */
  pttl = (...args: CommandArgs<typeof PTtlCommand>) =>
    new PTtlCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/publish
   */
  publish = (...args: CommandArgs<typeof PublishCommand>) =>
    new PublishCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/randomkey
   */
  randomkey = () => new RandomKeyCommand().exec(this.client);

  /**
   * @see https://redis.io/commands/rename
   */
  rename = (...args: CommandArgs<typeof RenameCommand>) =>
    new RenameCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/renamenx
   */
  renamenx = (...args: CommandArgs<typeof RenameNXCommand>) =>
    new RenameNXCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/rpop
   */
  rpop = <TData = string>(...args: CommandArgs<typeof RPopCommand>) =>
    new RPopCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/rpush
   */
  rpush = <TData>(key: string, ...elements: TData[]) =>
    new RPushCommand([key, ...elements], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/rpushx
   */
  rpushx = <TData>(key: string, ...elements: TData[]) =>
    new RPushXCommand([key, ...elements], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sadd
   */
  sadd = <TData>(key: string, ...members: TData[]) =>
    new SAddCommand<TData>([key, ...members], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/scan
   */
  scan = (...args: CommandArgs<typeof ScanCommand>) =>
    new ScanCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/scard
   */
  scard = (...args: CommandArgs<typeof SCardCommand>) =>
    new SCardCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/script-exists
   */
  scriptExists = (...args: CommandArgs<typeof ScriptExistsCommand>) =>
    new ScriptExistsCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/script-flush
   */
  scriptFlush = (...args: CommandArgs<typeof ScriptFlushCommand>) =>
    new ScriptFlushCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/script-load
   */
  scriptLoad = (...args: CommandArgs<typeof ScriptLoadCommand>) =>
    new ScriptLoadCommand(args, this.opts).exec(this.client);
  /**
   * @see https://redis.io/commands/sdiff
   */
  sdiff = (...args: CommandArgs<typeof SDiffCommand>) =>
    new SDiffCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sdiffstore
   */
  sdiffstore = (...args: CommandArgs<typeof SDiffStoreCommand>) =>
    new SDiffStoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/set
   */
  set = <TData>(key: string, value: TData, opts?: SetCommandOptions) =>
    new SetCommand<TData>([key, value, opts], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/setbit
   */
  setbit = (...args: CommandArgs<typeof SetBitCommand>) =>
    new SetBitCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/setex
   */
  setex = <TData>(key: string, ttl: number, value: TData) =>
    new SetExCommand<TData>([key, ttl, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/setnx
   */
  setnx = <TData>(key: string, value: TData) =>
    new SetNxCommand<TData>([key, value], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/setrange
   */
  setrange = (...args: CommandArgs<typeof SetRangeCommand>) =>
    new SetRangeCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sinter
   */
  sinter = (...args: CommandArgs<typeof SInterCommand>) =>
    new SInterCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sinterstore
   */
  sinterstore = (...args: CommandArgs<typeof SInterStoreCommand>) =>
    new SInterStoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sismember
   */
  sismember = <TData>(key: string, member: TData) =>
    new SIsMemberCommand<TData>([key, member], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/smismember
   */
  smismember = <TMembers extends unknown[]>(key: string, members: TMembers) =>
    new SMIsMemberCommand<TMembers>([key, members], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/smembers
   */
  smembers = <TData extends unknown[] = string[]>(...args: CommandArgs<typeof SMembersCommand>) =>
    new SMembersCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/smove
   */
  smove = <TData>(source: string, destination: string, member: TData) =>
    new SMoveCommand<TData>([source, destination, member], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/spop
   */
  spop = <TData>(...args: CommandArgs<typeof SPopCommand>) =>
    new SPopCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/srandmember
   */
  srandmember = <TData>(...args: CommandArgs<typeof SRandMemberCommand>) =>
    new SRandMemberCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/srem
   */
  srem = <TData>(key: string, ...members: TData[]) =>
    new SRemCommand<TData>([key, ...members], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sscan
   */
  sscan = (...args: CommandArgs<typeof SScanCommand>) =>
    new SScanCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/strlen
   */
  strlen = (...args: CommandArgs<typeof StrLenCommand>) =>
    new StrLenCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sunion
   */
  sunion = (...args: CommandArgs<typeof SUnionCommand>) =>
    new SUnionCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/sunionstore
   */
  sunionstore = (...args: CommandArgs<typeof SUnionStoreCommand>) =>
    new SUnionStoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/time
   */
  time = () => new TimeCommand().exec(this.client);

  /**
   * @see https://redis.io/commands/touch
   */
  touch = (...args: CommandArgs<typeof TouchCommand>) =>
    new TouchCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/ttl
   */
  ttl = (...args: CommandArgs<typeof TtlCommand>) =>
    new TtlCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/type
   */
  type = (...args: CommandArgs<typeof TypeCommand>) =>
    new TypeCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/unlink
   */
  unlink = (...args: CommandArgs<typeof UnlinkCommand>) =>
    new UnlinkCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xadd
   */
  xadd = (...args: CommandArgs<typeof XAddCommand>) =>
    new XAddCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xack
   */
  xack = (...args: CommandArgs<typeof XAckCommand>) =>
    new XAckCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xdel
   */
  xdel = (...args: CommandArgs<typeof XDelCommand>) =>
    new XDelCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xgroup
   */
  xgroup = (...args: CommandArgs<typeof XGroupCommand>) =>
    new XGroupCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xread
   */
  xread = (...args: CommandArgs<typeof XReadCommand>) =>
    new XReadCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xreadgroup
   */
  xreadgroup = (...args: CommandArgs<typeof XReadGroupCommand>) =>
    new XReadGroupCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xinfo
   */
  xinfo = (...args: CommandArgs<typeof XInfoCommand>) =>
    new XInfoCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xlen
   */
  xlen = (...args: CommandArgs<typeof XLenCommand>) =>
    new XLenCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xpending
   */
  xpending = (...args: CommandArgs<typeof XPendingCommand>) =>
    new XPendingCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xclaim
   */
  xclaim = (...args: CommandArgs<typeof XClaimCommand>) =>
    new XClaimCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xautoclaim
   */
  xautoclaim = (...args: CommandArgs<typeof XAutoClaim>) =>
    new XAutoClaim(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xtrim
   */
  xtrim = (...args: CommandArgs<typeof XTrimCommand>) =>
    new XTrimCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xrange
   */
  xrange = (...args: CommandArgs<typeof XRangeCommand>) =>
    new XRangeCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/xrevrange
   */
  xrevrange = (...args: CommandArgs<typeof XRevRangeCommand>) =>
    new XRevRangeCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zadd
   */
  zadd = <TData>(
    ...args:
      | [key: string, scoreMember: ScoreMember<TData>, ...scoreMemberPairs: ScoreMember<TData>[]]
      | [
          key: string,
          opts: ZAddCommandOptions,
          ...scoreMemberPairs: [ScoreMember<TData>, ...ScoreMember<TData>[]],
        ]
  ) => {
    if ("score" in args[1]) {
      return new ZAddCommand<TData>(
        [args[0], args[1] as ScoreMember<TData>, ...(args.slice(2) as any)],
        this.opts,
      ).exec(this.client);
    }

    return new ZAddCommand<TData>(
      [args[0], args[1] as any, ...(args.slice(2) as any)],
      this.opts,
    ).exec(this.client);
  };
  /**
   * @see https://redis.io/commands/zcard
   */
  zcard = (...args: CommandArgs<typeof ZCardCommand>) =>
    new ZCardCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zcount
   */
  zcount = (...args: CommandArgs<typeof ZCountCommand>) =>
    new ZCountCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zdiffstore
   */
  zdiffstore = (...args: CommandArgs<typeof ZDiffStoreCommand>) =>
    new ZDiffStoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zincrby
   */
  zincrby = <TData>(key: string, increment: number, member: TData) =>
    new ZIncrByCommand<TData>([key, increment, member], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zinterstore
   */
  zinterstore = (...args: CommandArgs<typeof ZInterStoreCommand>) =>
    new ZInterStoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zlexcount
   */
  zlexcount = (...args: CommandArgs<typeof ZLexCountCommand>) =>
    new ZLexCountCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zmscore
   */
  zmscore = (...args: CommandArgs<typeof ZMScoreCommand>) =>
    new ZMScoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zpopmax
   */
  zpopmax = <TData>(...args: CommandArgs<typeof ZPopMaxCommand>) =>
    new ZPopMaxCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zpopmin
   */
  zpopmin = <TData>(...args: CommandArgs<typeof ZPopMinCommand>) =>
    new ZPopMinCommand<TData>(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zrange
   */
  zrange = <TData extends unknown[]>(
    ...args:
      | [key: string, min: number, max: number, opts?: ZRangeCommandOptions]
      | [
          key: string,
          min: `(${string}` | `[${string}` | "-" | "+",
          max: `(${string}` | `[${string}` | "-" | "+",
          opts: { byLex: true } & ZRangeCommandOptions,
        ]
      | [
          key: string,
          min: number | `(${number}` | "-inf" | "+inf",
          max: number | `(${number}` | "-inf" | "+inf",
          opts: { byScore: true } & ZRangeCommandOptions,
        ]
  ) => new ZRangeCommand<TData>(args as any, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zrank
   */
  zrank = <TData>(key: string, member: TData) =>
    new ZRankCommand<TData>([key, member], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zrem
   */
  zrem = <TData>(key: string, ...members: TData[]) =>
    new ZRemCommand<TData>([key, ...members], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zremrangebylex
   */
  zremrangebylex = (...args: CommandArgs<typeof ZRemRangeByLexCommand>) =>
    new ZRemRangeByLexCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zremrangebyrank
   */
  zremrangebyrank = (...args: CommandArgs<typeof ZRemRangeByRankCommand>) =>
    new ZRemRangeByRankCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zremrangebyscore
   */
  zremrangebyscore = (...args: CommandArgs<typeof ZRemRangeByScoreCommand>) =>
    new ZRemRangeByScoreCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zrevrank
   */
  zrevrank = <TData>(key: string, member: TData) =>
    new ZRevRankCommand<TData>([key, member], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zscan
   */
  zscan = (...args: CommandArgs<typeof ZScanCommand>) =>
    new ZScanCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zscore
   */
  zscore = <TData>(key: string, member: TData) =>
    new ZScoreCommand<TData>([key, member], this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zunion
   */
  zunion = (...args: CommandArgs<typeof ZUnionCommand>) =>
    new ZUnionCommand(args, this.opts).exec(this.client);

  /**
   * @see https://redis.io/commands/zunionstore
   */
  zunionstore = (...args: CommandArgs<typeof ZUnionStoreCommand>) =>
    new ZUnionStoreCommand(args, this.opts).exec(this.client);
}
