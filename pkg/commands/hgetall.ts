import { Command } from "../command"

/**
 * @see https://redis.io/commands/hgetall
 */
export class HGetAllCommand<TFields extends unknown[]> extends Command<TFields | null> {
  constructor(key: string) {
    super(["hgetall", key])
  }
}
