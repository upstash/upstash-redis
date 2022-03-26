import { Command } from "./command"

export type ZRangeCommandOptions = {
  withScores?: boolean
  byScore?: boolean
}

/**
 * @see https://redis.io/commands/zrange
 */
export class ZRangeCommand<TData extends unknown[]> extends Command<TData, string[]> {
  constructor(
    key: string,
    min: number | `(${number}`,
    max: number | `(${number}`,
    opts?: ZRangeCommandOptions,
  ) {
    const command: unknown[] = ["zrange", key, min, max]

    // Either byScore or byLex is allowed
    if (opts?.byScore) {
      command.push("byscore")
    }
    if (opts?.withScores) {
      command.push("withscores")
    }
    super(command)
  }
}
