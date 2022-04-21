import { Command } from "./command";

/**
 * @see https://redis.io/commands/pttl
 */
export class PTtlCommand extends Command<number, number> {
  constructor(cmd: [key: string]) {
    super(["pttl", ...cmd])
  }
}
