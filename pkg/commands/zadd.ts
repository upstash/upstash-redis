import { Command, CommandOptions } from "./command.ts";

export type ZAddCommandOptions =
  & (
    | { nx: true; xx?: never }
    | { nx?: never; xx: true }
    | { nx?: never; xx?: never }
  )
  & { ch?: true };

export type ZAddCommandOptionsWithIncr = ZAddCommandOptions & { incr: true };

export type ScoreMember<TData> = { score: number; member: TData };
/**
 * @see https://redis.io/commands/zadd
 */
export class ZAddCommand<TData = string> extends Command<
  number | null,
  number | null
> {
  constructor(
    cmd: [
      key: string,
      scoreMember: ScoreMember<TData>,
      ...scoreMemberPairs: ScoreMember<TData>[],
    ],
    opts?: CommandOptions<number | null, number | null>,
  );
  constructor(
    cmd: [
      key: string,
      opts: ZAddCommandOptions | ZAddCommandOptionsWithIncr,
      ...scoreMemberPairs: [ScoreMember<TData>, ...ScoreMember<TData>[]],
    ],
    opts?: CommandOptions<number | null, number | null>,
  );
  constructor(
    [key, arg1, ...arg2]: [
      key: string,
      arg1:
        | ScoreMember<TData>
        | ZAddCommandOptions
        | ZAddCommandOptionsWithIncr,
      ...arg2: ScoreMember<TData>[]
    ],
    opts?: CommandOptions<number | null, number | null>,
  ) {
    const command: unknown[] = ["zadd", key];

    if ("nx" in arg1 && arg1.nx) {
      command.push("nx");
    } else if ("xx" in arg1 && arg1.xx) {
      command.push("xx");
    }
    if ("ch" in arg1 && arg1.ch) {
      command.push("ch");
    }
    if ("incr" in arg1 && arg1.incr) {
      command.push("incr");
    }

    if ("score" in arg1 && "member" in arg1) {
      command.push(arg1.score, arg1.member);
    }

    command.push(...arg2.flatMap(({ score, member }) => [score, member]));

    super(command, opts);
  }
}
