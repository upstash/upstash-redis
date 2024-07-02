import { Command, CommandOptions } from "./commands/command";
import { HRandFieldCommand } from "./commands/hrandfield";
import {
  AppendCommand,
  BitCountCommand,
  BitFieldCommand,
  BitOpCommand,
  BitPosCommand,
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
import { UpstashError } from "./error";
import { Requester, UpstashResponse } from "./http";
import { CommandArgs } from "./types";

// Given a tuple of commands, returns a tuple of the response data of each command
type InferResponseData<T extends unknown[]> = {
  [K in keyof T]: T[K] extends Command<any, infer TData> ? TData : unknown;
};

/**
 * Upstash REST API supports command pipelining to send multiple commands in
 * batch, instead of sending each command one by one and waiting for a response.
 * When using pipelines, several commands are sent using a single HTTP request,
 * and a single JSON array response is returned. Each item in the response array
 * corresponds to the command in the same order within the pipeline.
 *
 * **NOTE:**
 *
 * Execution of the pipeline is not atomic. Even though each command in
 * the pipeline will be executed in order, commands sent by other clients can
 * interleave with the pipeline.
 *
 * **Examples:**
 *
 * ```ts
 *  const p = redis.pipeline() // or redis.multi()
 * p.set("key","value")
 * p.get("key")
 * const res = await p.exec()
 * ```
 *
 * You can also chain commands together
 * ```ts
 * const p = redis.pipeline()
 * const res = await p.set("key","value").get("key").exec()
 * ```
 *
 * Return types are inferred if all commands are chained, but you can still
 * override the response type manually:
 * ```ts
 *  redis.pipeline()
 *   .set("key", { greeting: "hello"})
 *   .get("key")
 *   .exec<["OK", { greeting: string } ]>()
 *
 * ```
 */
export class Pipeline<TCommands extends Command<any, any>[] = []> {
  private client: Requester;
  private commands: TCommands;
  private commandOptions?: CommandOptions<any, any>;
  private multiExec: boolean;

  constructor(opts: {
    client: Requester;
    commandOptions?: CommandOptions<any, any>;
    multiExec?: boolean;
  }) {
    this.client = opts.client;

    this.commands = [] as unknown as TCommands; // the TCommands generic in the class definition is only used for carrying through chained command types and should never be explicitly set when instantiating the class
    this.commandOptions = opts.commandOptions;
    this.multiExec = opts.multiExec ?? false;

    if (this.commandOptions?.latencyLogging) {
      const originalExec = this.exec.bind(this);
      this.exec = async <
        TCommandResults extends unknown[] = [] extends TCommands
          ? unknown[]
          : InferResponseData<TCommands>,
      >(): Promise<TCommandResults> => {
        const start = performance.now();
        const result = await originalExec();
        const end = performance.now();
        const loggerResult = (end - start).toFixed(2);
        console.log(
          `Latency for \x1b[38;2;19;185;39m${
            this.multiExec ? ["MULTI-EXEC"] : ["PIPELINE"].toString().toUpperCase()
          }\x1b[0m: \x1b[38;2;0;255;255m${loggerResult} ms\x1b[0m`,
        );
        return result as TCommandResults;
      };
    }
  }

  /**
   * Send the pipeline request to upstash.
   *
   * Returns an array with the results of all pipelined commands.
   *
   * If all commands are statically chained from start to finish, types are inferred. You can still define a return type manually if necessary though:
   * ```ts
   * const p = redis.pipeline()
   * p.get("key")
   * const result = p.exec<[{ greeting: string }]>()
   * ```
   */
  exec = async <
    TCommandResults extends unknown[] = [] extends TCommands
      ? unknown[]
      : InferResponseData<TCommands>,
  >(): Promise<TCommandResults> => {
    if (this.commands.length === 0) {
      throw new Error("Pipeline is empty");
    }
    const path = this.multiExec ? ["multi-exec"] : ["pipeline"];
    const res = (await this.client.request({
      path,
      body: Object.values(this.commands).map((c) => c.command),
    })) as UpstashResponse<any>[];

    return res.map(({ error, result }, i) => {
      if (error) {
        throw new UpstashError(
          `Command ${i + 1} [ ${this.commands[i].command[0]} ] failed: ${error}`,
        );
      }

      return this.commands[i].deserialize(result);
    }) as TCommandResults;
  };

  /**
   * Returns the length of pipeline before the execution
   */
  length(): number {
    return this.commands.length;
  }

  /**
   * Pushes a command into the pipeline and returns a chainable instance of the
   * pipeline
   */
  private chain<T>(command: Command<any, T>): Pipeline<[...TCommands, Command<any, T>]> {
    this.commands.push(command);
    return this as any; // TS thinks we're returning Pipeline<[]> here, because we're not creating a new instance of the class, hence the cast
  }

  /**
   * @see https://redis.io/commands/append
   */
  append = (...args: CommandArgs<typeof AppendCommand>) =>
    this.chain(new AppendCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/bitcount
   */
  bitcount = (...args: CommandArgs<typeof BitCountCommand>) =>
    this.chain(new BitCountCommand(args, this.commandOptions));

  /**
   * Returns an instance that can be used to execute `BITFIELD` commands on one key.
   *
   * @example
   * ```typescript
   * redis.set("mykey", 0);
   * const result = await redis.pipeline()
   *   .bitfield("mykey")
   *   .set("u4", 0, 16)
   *   .incr("u4", "#1", 1)
   *   .exec();
   * console.log(result); // [[0, 1]]
   * ```
   *
   * @see https://redis.io/commands/bitfield
   */
  bitfield = (...args: CommandArgs<typeof BitFieldCommand>) =>
    new BitFieldCommand(args, this.client, this.commandOptions, this.chain.bind(this));

  /**
   * @see https://redis.io/commands/bitop
   */
  bitop: {
    (
      op: "and" | "or" | "xor",
      destinationKey: string,
      sourceKey: string,
      ...sourceKeys: string[]
    ): Pipeline<[...TCommands, BitOpCommand]>;
    (op: "not", destinationKey: string, sourceKey: string): Pipeline<[...TCommands, BitOpCommand]>;
  } = (
    op: "and" | "or" | "xor" | "not",
    destinationKey: string,
    sourceKey: string,
    ...sourceKeys: string[]
  ) =>
    this.chain(
      new BitOpCommand([op as any, destinationKey, sourceKey, ...sourceKeys], this.commandOptions),
    );

  /**
   * @see https://redis.io/commands/bitpos
   */
  bitpos = (...args: CommandArgs<typeof BitPosCommand>) =>
    this.chain(new BitPosCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/copy
   */
  copy = (...args: CommandArgs<typeof CopyCommand>) =>
    this.chain(new CopyCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zdiffstore
   */
  zdiffstore = (...args: CommandArgs<typeof ZDiffStoreCommand>) =>
    this.chain(new ZDiffStoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/dbsize
   */
  dbsize = () => this.chain(new DBSizeCommand(this.commandOptions));

  /**
   * @see https://redis.io/commands/decr
   */
  decr = (...args: CommandArgs<typeof DecrCommand>) =>
    this.chain(new DecrCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/decrby
   */
  decrby = (...args: CommandArgs<typeof DecrByCommand>) =>
    this.chain(new DecrByCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/del
   */
  del = (...args: CommandArgs<typeof DelCommand>) =>
    this.chain(new DelCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/echo
   */
  echo = (...args: CommandArgs<typeof EchoCommand>) =>
    this.chain(new EchoCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/eval
   */
  eval = <TArgs extends unknown[], TData = unknown>(
    ...args: [script: string, keys: string[], args: TArgs]
  ) => this.chain(new EvalCommand<TArgs, TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/evalsha
   */
  evalsha = <TArgs extends unknown[], TData = unknown>(
    ...args: [sha1: string, keys: string[], args: TArgs]
  ) => this.chain(new EvalshaCommand<TArgs, TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/exists
   */
  exists = (...args: CommandArgs<typeof ExistsCommand>) =>
    this.chain(new ExistsCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/expire
   */
  expire = (...args: CommandArgs<typeof ExpireCommand>) =>
    this.chain(new ExpireCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/expireat
   */
  expireat = (...args: CommandArgs<typeof ExpireAtCommand>) =>
    this.chain(new ExpireAtCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/flushall
   */
  flushall = (args?: CommandArgs<typeof FlushAllCommand>) =>
    this.chain(new FlushAllCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/flushdb
   */
  flushdb = (...args: CommandArgs<typeof FlushDBCommand>) =>
    this.chain(new FlushDBCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/geoadd
   */
  geoadd = <TData>(...args: CommandArgs<typeof GeoAddCommand<TData>>) =>
    this.chain(new GeoAddCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/geodist
   */
  geodist = <TData>(...args: CommandArgs<typeof GeoDistCommand<TData>>) =>
    this.chain(new GeoDistCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/geopos
   */
  geopos = <TData>(...args: CommandArgs<typeof GeoPosCommand<TData>>) =>
    this.chain(new GeoPosCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/geohash
   */
  geohash = <TData>(...args: CommandArgs<typeof GeoHashCommand<TData>>) =>
    this.chain(new GeoHashCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/geosearch
   */
  geosearch = <TData>(...args: CommandArgs<typeof GeoSearchCommand<TData>>) =>
    this.chain(new GeoSearchCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/geosearchstore
   */
  geosearchstore = <TData>(...args: CommandArgs<typeof GeoSearchStoreCommand<TData>>) =>
    this.chain(new GeoSearchStoreCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/get
   */
  get = <TData>(...args: CommandArgs<typeof GetCommand>) =>
    this.chain(new GetCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/getbit
   */
  getbit = (...args: CommandArgs<typeof GetBitCommand>) =>
    this.chain(new GetBitCommand(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getdel
   */
  getdel = <TData>(...args: CommandArgs<typeof GetDelCommand>) =>
    this.chain(new GetDelCommand<TData>(args, this.commandOptions));
  /**
   * @see https://redis.io/commands/getrange
   */
  getrange = (...args: CommandArgs<typeof GetRangeCommand>) =>
    this.chain(new GetRangeCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/getset
   */
  getset = <TData>(key: string, value: TData) =>
    this.chain(new GetSetCommand<TData>([key, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/hdel
   */
  hdel = (...args: CommandArgs<typeof HDelCommand>) =>
    this.chain(new HDelCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hexists
   */
  hexists = (...args: CommandArgs<typeof HExistsCommand>) =>
    this.chain(new HExistsCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hget
   */
  hget = <TData>(...args: CommandArgs<typeof HGetCommand>) =>
    this.chain(new HGetCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hgetall
   */
  hgetall = <TData extends Record<string, unknown>>(...args: CommandArgs<typeof HGetAllCommand>) =>
    this.chain(new HGetAllCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hincrby
   */
  hincrby = (...args: CommandArgs<typeof HIncrByCommand>) =>
    this.chain(new HIncrByCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hincrbyfloat
   */
  hincrbyfloat = (...args: CommandArgs<typeof HIncrByFloatCommand>) =>
    this.chain(new HIncrByFloatCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hkeys
   */
  hkeys = (...args: CommandArgs<typeof HKeysCommand>) =>
    this.chain(new HKeysCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hlen
   */
  hlen = (...args: CommandArgs<typeof HLenCommand>) =>
    this.chain(new HLenCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hmget
   */
  hmget = <TData extends Record<string, unknown>>(...args: CommandArgs<typeof HMGetCommand>) =>
    this.chain(new HMGetCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hmset
   */
  hmset = <TData>(key: string, kv: { [field: string]: TData }) =>
    this.chain(new HMSetCommand([key, kv], this.commandOptions));

  /**
   * @see https://redis.io/commands/hrandfield
   */
  hrandfield = <TData extends string | string[] | Record<string, unknown>>(
    key: string,
    count?: number,
    withValues?: boolean,
  ) =>
    this.chain(new HRandFieldCommand<TData>([key, count, withValues] as any, this.commandOptions));

  /**
   * @see https://redis.io/commands/hscan
   */
  hscan = (...args: CommandArgs<typeof HScanCommand>) =>
    this.chain(new HScanCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hset
   */
  hset = <TData>(key: string, kv: { [field: string]: TData }) =>
    this.chain(new HSetCommand<TData>([key, kv], this.commandOptions));

  /**
   * @see https://redis.io/commands/hsetnx
   */
  hsetnx = <TData>(key: string, field: string, value: TData) =>
    this.chain(new HSetNXCommand<TData>([key, field, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/hstrlen
   */
  hstrlen = (...args: CommandArgs<typeof HStrLenCommand>) =>
    this.chain(new HStrLenCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/hvals
   */
  hvals = (...args: CommandArgs<typeof HValsCommand>) =>
    this.chain(new HValsCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/incr
   */
  incr = (...args: CommandArgs<typeof IncrCommand>) =>
    this.chain(new IncrCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/incrby
   */
  incrby = (...args: CommandArgs<typeof IncrByCommand>) =>
    this.chain(new IncrByCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/incrbyfloat
   */
  incrbyfloat = (...args: CommandArgs<typeof IncrByFloatCommand>) =>
    this.chain(new IncrByFloatCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/keys
   */
  keys = (...args: CommandArgs<typeof KeysCommand>) =>
    this.chain(new KeysCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lindex
   */
  lindex = (...args: CommandArgs<typeof LIndexCommand>) =>
    this.chain(new LIndexCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/linsert
   */
  linsert = <TData>(key: string, direction: "before" | "after", pivot: TData, value: TData) =>
    this.chain(new LInsertCommand<TData>([key, direction, pivot, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/llen
   */
  llen = (...args: CommandArgs<typeof LLenCommand>) =>
    this.chain(new LLenCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lmove
   */
  lmove = <TData = string>(...args: CommandArgs<typeof LMoveCommand>) =>
    this.chain(new LMoveCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lpop
   */
  lpop = <TData>(...args: CommandArgs<typeof LPopCommand>) =>
    this.chain(new LPopCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lmpop
   */
  lmpop = <TData>(...args: CommandArgs<typeof LmPopCommand>) =>
    this.chain(new LmPopCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lpos
   */
  lpos = <TData>(...args: CommandArgs<typeof LPosCommand>) =>
    this.chain(new LPosCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lpush
   */
  lpush = <TData>(key: string, ...elements: TData[]) =>
    this.chain(new LPushCommand<TData>([key, ...elements], this.commandOptions));

  /**
   * @see https://redis.io/commands/lpushx
   */
  lpushx = <TData>(key: string, ...elements: TData[]) =>
    this.chain(new LPushXCommand<TData>([key, ...elements], this.commandOptions));

  /**
   * @see https://redis.io/commands/lrange
   */
  lrange = <TResult = string>(...args: CommandArgs<typeof LRangeCommand>) =>
    this.chain(new LRangeCommand<TResult>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/lrem
   */
  lrem = <TData>(key: string, count: number, value: TData) =>
    this.chain(new LRemCommand([key, count, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/lset
   */
  lset = <TData>(key: string, index: number, value: TData) =>
    this.chain(new LSetCommand([key, index, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/ltrim
   */
  ltrim = (...args: CommandArgs<typeof LTrimCommand>) =>
    this.chain(new LTrimCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/mget
   */
  mget = <TData extends unknown[]>(...args: CommandArgs<typeof MGetCommand>) =>
    this.chain(new MGetCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/mset
   */
  mset = <TData>(kv: { [key: string]: TData }) =>
    this.chain(new MSetCommand<TData>([kv], this.commandOptions));

  /**
   * @see https://redis.io/commands/msetnx
   */
  msetnx = <TData>(kv: { [key: string]: TData }) =>
    this.chain(new MSetNXCommand<TData>([kv], this.commandOptions));

  /**
   * @see https://redis.io/commands/persist
   */
  persist = (...args: CommandArgs<typeof PersistCommand>) =>
    this.chain(new PersistCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/pexpire
   */
  pexpire = (...args: CommandArgs<typeof PExpireCommand>) =>
    this.chain(new PExpireCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/pexpireat
   */
  pexpireat = (...args: CommandArgs<typeof PExpireAtCommand>) =>
    this.chain(new PExpireAtCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/pfadd
   */
  pfadd = (...args: CommandArgs<typeof PfAddCommand>) =>
    this.chain(new PfAddCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/pfcount
   */
  pfcount = (...args: CommandArgs<typeof PfCountCommand>) =>
    this.chain(new PfCountCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/pfmerge
   */
  pfmerge = (...args: CommandArgs<typeof PfMergeCommand>) =>
    this.chain(new PfMergeCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/ping
   */
  ping = (args?: CommandArgs<typeof PingCommand>) =>
    this.chain(new PingCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/psetex
   */
  psetex = <TData>(key: string, ttl: number, value: TData) =>
    this.chain(new PSetEXCommand<TData>([key, ttl, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/pttl
   */
  pttl = (...args: CommandArgs<typeof PTtlCommand>) =>
    this.chain(new PTtlCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/publish
   */
  publish = (...args: CommandArgs<typeof PublishCommand>) =>
    this.chain(new PublishCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/randomkey
   */
  randomkey = () => this.chain(new RandomKeyCommand(this.commandOptions));

  /**
   * @see https://redis.io/commands/rename
   */
  rename = (...args: CommandArgs<typeof RenameCommand>) =>
    this.chain(new RenameCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/renamenx
   */
  renamenx = (...args: CommandArgs<typeof RenameNXCommand>) =>
    this.chain(new RenameNXCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/rpop
   */
  rpop = <TData = string>(...args: CommandArgs<typeof RPopCommand>) =>
    this.chain(new RPopCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/rpush
   */
  rpush = <TData>(key: string, ...elements: TData[]) =>
    this.chain(new RPushCommand([key, ...elements], this.commandOptions));

  /**
   * @see https://redis.io/commands/rpushx
   */
  rpushx = <TData>(key: string, ...elements: TData[]) =>
    this.chain(new RPushXCommand([key, ...elements], this.commandOptions));

  /**
   * @see https://redis.io/commands/sadd
   */
  sadd = <TData>(key: string, ...members: TData[]) =>
    this.chain(new SAddCommand<TData>([key, ...members], this.commandOptions));

  /**
   * @see https://redis.io/commands/scan
   */
  scan = (...args: CommandArgs<typeof ScanCommand>) =>
    this.chain(new ScanCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/scard
   */
  scard = (...args: CommandArgs<typeof SCardCommand>) =>
    this.chain(new SCardCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/script-exists
   */
  scriptExists = (...args: CommandArgs<typeof ScriptExistsCommand>) =>
    this.chain(new ScriptExistsCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/script-flush
   */
  scriptFlush = (...args: CommandArgs<typeof ScriptFlushCommand>) =>
    this.chain(new ScriptFlushCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/script-load
   */
  scriptLoad = (...args: CommandArgs<typeof ScriptLoadCommand>) =>
    this.chain(new ScriptLoadCommand(args, this.commandOptions));
  /*)*
   * @see https://redis.io/commands/sdiff
   */
  sdiff = (...args: CommandArgs<typeof SDiffCommand>) =>
    this.chain(new SDiffCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/sdiffstore
   */
  sdiffstore = (...args: CommandArgs<typeof SDiffStoreCommand>) =>
    this.chain(new SDiffStoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/set
   */
  set = <TData>(key: string, value: TData, opts?: SetCommandOptions) =>
    this.chain(new SetCommand<TData>([key, value, opts], this.commandOptions));

  /**
   * @see https://redis.io/commands/setbit
   */
  setbit = (...args: CommandArgs<typeof SetBitCommand>) =>
    this.chain(new SetBitCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/setex
   */
  setex = <TData>(key: string, ttl: number, value: TData) =>
    this.chain(new SetExCommand<TData>([key, ttl, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/setnx
   */
  setnx = <TData>(key: string, value: TData) =>
    this.chain(new SetNxCommand<TData>([key, value], this.commandOptions));

  /**
   * @see https://redis.io/commands/setrange
   */
  setrange = (...args: CommandArgs<typeof SetRangeCommand>) =>
    this.chain(new SetRangeCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/sinter
   */
  sinter = (...args: CommandArgs<typeof SInterCommand>) =>
    this.chain(new SInterCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/sinterstore
   */
  sinterstore = (...args: CommandArgs<typeof SInterStoreCommand>) =>
    this.chain(new SInterStoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/sismember
   */
  sismember = <TData>(key: string, member: TData) =>
    this.chain(new SIsMemberCommand<TData>([key, member], this.commandOptions));

  /**
   * @see https://redis.io/commands/smembers
   */
  smembers = <TData extends unknown[] = string[]>(...args: CommandArgs<typeof SMembersCommand>) =>
    this.chain(new SMembersCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/smismember
   */
  smismember = <TMembers extends unknown[]>(key: string, members: TMembers) =>
    this.chain(new SMIsMemberCommand<TMembers>([key, members], this.commandOptions));

  /**
   * @see https://redis.io/commands/smove
   */
  smove = <TData>(source: string, destination: string, member: TData) =>
    this.chain(new SMoveCommand<TData>([source, destination, member], this.commandOptions));

  /**
   * @see https://redis.io/commands/spop
   */
  spop = <TData>(...args: CommandArgs<typeof SPopCommand>) =>
    this.chain(new SPopCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/srandmember
   */
  srandmember = <TData>(...args: CommandArgs<typeof SRandMemberCommand>) =>
    this.chain(new SRandMemberCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/srem
   */
  srem = <TData>(key: string, ...members: TData[]) =>
    this.chain(new SRemCommand<TData>([key, ...members], this.commandOptions));

  /**
   * @see https://redis.io/commands/sscan
   */
  sscan = (...args: CommandArgs<typeof SScanCommand>) =>
    this.chain(new SScanCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/strlen
   */
  strlen = (...args: CommandArgs<typeof StrLenCommand>) =>
    this.chain(new StrLenCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/sunion
   */
  sunion = (...args: CommandArgs<typeof SUnionCommand>) =>
    this.chain(new SUnionCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/sunionstore
   */
  sunionstore = (...args: CommandArgs<typeof SUnionStoreCommand>) =>
    this.chain(new SUnionStoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/time
   */
  time = () => this.chain(new TimeCommand(this.commandOptions));

  /**
   * @see https://redis.io/commands/touch
   */
  touch = (...args: CommandArgs<typeof TouchCommand>) =>
    this.chain(new TouchCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/ttl
   */
  ttl = (...args: CommandArgs<typeof TtlCommand>) =>
    this.chain(new TtlCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/type
   */
  type = (...args: CommandArgs<typeof TypeCommand>) =>
    this.chain(new TypeCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/unlink
   */
  unlink = (...args: CommandArgs<typeof UnlinkCommand>) =>
    this.chain(new UnlinkCommand(args, this.commandOptions));

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
      return this.chain(
        new ZAddCommand<TData>(
          [args[0], args[1] as ScoreMember<TData>, ...(args.slice(2) as any)],
          this.commandOptions,
        ),
      );
    }

    return this.chain(
      new ZAddCommand<TData>(
        [args[0], args[1] as any, ...(args.slice(2) as any)],
        this.commandOptions,
      ),
    );
  };

  /**
   * @see https://redis.io/commands/xadd
   */
  xadd = (...args: CommandArgs<typeof XAddCommand>) =>
    this.chain(new XAddCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xack
   */
  xack = (...args: CommandArgs<typeof XAckCommand>) =>
    this.chain(new XAckCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xdel
   */
  xdel = (...args: CommandArgs<typeof XDelCommand>) =>
    this.chain(new XDelCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xgroup
   */
  xgroup = (...args: CommandArgs<typeof XGroupCommand>) =>
    this.chain(new XGroupCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xread
   */
  xread = (...args: CommandArgs<typeof XReadCommand>) =>
    this.chain(new XReadCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xreadgroup
   */
  xreadgroup = (...args: CommandArgs<typeof XReadGroupCommand>) =>
    this.chain(new XReadGroupCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xinfo
   */
  xinfo = (...args: CommandArgs<typeof XInfoCommand>) =>
    this.chain(new XInfoCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xlen
   */
  xlen = (...args: CommandArgs<typeof XLenCommand>) =>
    this.chain(new XLenCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xpending
   */
  xpending = (...args: CommandArgs<typeof XPendingCommand>) =>
    this.chain(new XPendingCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xclaim
   */
  xclaim = (...args: CommandArgs<typeof XClaimCommand>) =>
    this.chain(new XClaimCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xautoclaim
   */
  xautoclaim = (...args: CommandArgs<typeof XAutoClaim>) =>
    this.chain(new XAutoClaim(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xtrim
   */
  xtrim = (...args: CommandArgs<typeof XTrimCommand>) =>
    this.chain(new XTrimCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xrange
   */
  xrange = (...args: CommandArgs<typeof XRangeCommand>) =>
    this.chain(new XRangeCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/xrevrange
   */
  xrevrange = (...args: CommandArgs<typeof XRevRangeCommand>) =>
    this.chain(new XRevRangeCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zcard
   */
  zcard = (...args: CommandArgs<typeof ZCardCommand>) =>
    this.chain(new ZCardCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zcount
   */
  zcount = (...args: CommandArgs<typeof ZCountCommand>) =>
    this.chain(new ZCountCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zincrby
   */
  zincrby = <TData>(key: string, increment: number, member: TData) =>
    this.chain(new ZIncrByCommand<TData>([key, increment, member], this.commandOptions));

  /**
   * @see https://redis.io/commands/zinterstore
   */
  zinterstore = (...args: CommandArgs<typeof ZInterStoreCommand>) =>
    this.chain(new ZInterStoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zlexcount
   */
  zlexcount = (...args: CommandArgs<typeof ZLexCountCommand>) =>
    this.chain(new ZLexCountCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zmscore
   */
  zmscore = (...args: CommandArgs<typeof ZMScoreCommand>) =>
    this.chain(new ZMScoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zpopmax
   */
  zpopmax = <TData>(...args: CommandArgs<typeof ZPopMaxCommand>) =>
    this.chain(new ZPopMaxCommand<TData>(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zpopmin
   */
  zpopmin = <TData>(...args: CommandArgs<typeof ZPopMinCommand>) =>
    this.chain(new ZPopMinCommand<TData>(args, this.commandOptions));

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
  ) => this.chain(new ZRangeCommand<TData>(args as any, this.commandOptions));

  /**
   * @see https://redis.io/commands/zrank
   */
  zrank = <TData>(key: string, member: TData) =>
    this.chain(new ZRankCommand<TData>([key, member], this.commandOptions));

  /**
   * @see https://redis.io/commands/zrem
   */
  zrem = <TData>(key: string, ...members: TData[]) =>
    this.chain(new ZRemCommand<TData>([key, ...members], this.commandOptions));

  /**
   * @see https://redis.io/commands/zremrangebylex
   */
  zremrangebylex = (...args: CommandArgs<typeof ZRemRangeByLexCommand>) =>
    this.chain(new ZRemRangeByLexCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zremrangebyrank
   */
  zremrangebyrank = (...args: CommandArgs<typeof ZRemRangeByRankCommand>) =>
    this.chain(new ZRemRangeByRankCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zremrangebyscore
   */
  zremrangebyscore = (...args: CommandArgs<typeof ZRemRangeByScoreCommand>) =>
    this.chain(new ZRemRangeByScoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zrevrank
   */
  zrevrank = <TData>(key: string, member: TData) =>
    this.chain(new ZRevRankCommand<TData>([key, member], this.commandOptions));

  /**
   * @see https://redis.io/commands/zscan
   */
  zscan = (...args: CommandArgs<typeof ZScanCommand>) =>
    this.chain(new ZScanCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zscore
   */
  zscore = <TData>(key: string, member: TData) =>
    this.chain(new ZScoreCommand<TData>([key, member], this.commandOptions));

  /**
   * @see https://redis.io/commands/zunionstore
   */
  zunionstore = (...args: CommandArgs<typeof ZUnionStoreCommand>) =>
    this.chain(new ZUnionStoreCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/zunion
   */
  zunion = (...args: CommandArgs<typeof ZUnionCommand>) =>
    this.chain(new ZUnionCommand(args, this.commandOptions));

  /**
   * @see https://redis.io/commands/?group=json
   */
  get json() {
    return {
      /**
       * @see https://redis.io/commands/json.arrappend
       */
      arrappend: (...args: CommandArgs<typeof JsonArrAppendCommand>) =>
        this.chain(new JsonArrAppendCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.arrindex
       */
      arrindex: (...args: CommandArgs<typeof JsonArrIndexCommand>) =>
        this.chain(new JsonArrIndexCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.arrinsert
       */
      arrinsert: (...args: CommandArgs<typeof JsonArrInsertCommand>) =>
        this.chain(new JsonArrInsertCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.arrlen
       */
      arrlen: (...args: CommandArgs<typeof JsonArrLenCommand>) =>
        this.chain(new JsonArrLenCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.arrpop
       */
      arrpop: (...args: CommandArgs<typeof JsonArrPopCommand>) =>
        this.chain(new JsonArrPopCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.arrtrim
       */
      arrtrim: (...args: CommandArgs<typeof JsonArrTrimCommand>) =>
        this.chain(new JsonArrTrimCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.clear
       */
      clear: (...args: CommandArgs<typeof JsonClearCommand>) =>
        this.chain(new JsonClearCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.del
       */
      del: (...args: CommandArgs<typeof JsonDelCommand>) =>
        this.chain(new JsonDelCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.forget
       */
      forget: (...args: CommandArgs<typeof JsonForgetCommand>) =>
        this.chain(new JsonForgetCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.get
       */
      get: (...args: CommandArgs<typeof JsonGetCommand>) =>
        this.chain(new JsonGetCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.mget
       */
      mget: (...args: CommandArgs<typeof JsonMGetCommand>) =>
        this.chain(new JsonMGetCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.numincrby
       */
      numincrby: (...args: CommandArgs<typeof JsonNumIncrByCommand>) =>
        this.chain(new JsonNumIncrByCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.nummultby
       */
      nummultby: (...args: CommandArgs<typeof JsonNumMultByCommand>) =>
        this.chain(new JsonNumMultByCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.objkeys
       */
      objkeys: (...args: CommandArgs<typeof JsonObjKeysCommand>) =>
        this.chain(new JsonObjKeysCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.objlen
       */
      objlen: (...args: CommandArgs<typeof JsonObjLenCommand>) =>
        this.chain(new JsonObjLenCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.resp
       */
      resp: (...args: CommandArgs<typeof JsonRespCommand>) =>
        this.chain(new JsonRespCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.set
       */
      set: (...args: CommandArgs<typeof JsonSetCommand>) =>
        this.chain(new JsonSetCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.strappend
       */
      strappend: (...args: CommandArgs<typeof JsonStrAppendCommand>) =>
        this.chain(new JsonStrAppendCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.strlen
       */
      strlen: (...args: CommandArgs<typeof JsonStrLenCommand>) =>
        this.chain(new JsonStrLenCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.toggle
       */
      toggle: (...args: CommandArgs<typeof JsonToggleCommand>) =>
        this.chain(new JsonToggleCommand(args, this.commandOptions)),

      /**
       * @see https://redis.io/commands/json.type
       */
      type: (...args: CommandArgs<typeof JsonTypeCommand>) =>
        this.chain(new JsonTypeCommand(args, this.commandOptions)),
    };
  }
}
