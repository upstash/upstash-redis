import { ScanCommandOptions } from "."
import { Command } from "../command"

/**
 * @see https://redis.io/commands/sscan
 */
export class SScanCommand extends Command<
  [number, (string | number)[]],
  [number, (string | number)[]]
> {
  constructor(key: string, cursor: number, opts?: ScanCommandOptions) {
    const command = ["sscan", key, cursor]
    if (opts?.match) {
      command.push("match", opts.match)
    }
    if (typeof opts?.count === "number") {
      command.push("count", opts.count)
    }

    super(command)
  }
}