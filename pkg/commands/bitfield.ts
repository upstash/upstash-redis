import { type Requester } from "../http";
import { Command, type CommandOptions } from "./command";

type SubCommandArgs<TRest extends unknown[] = []> = [
  encoding: string, // u1 - u63 | i1 - i64
  offset: number | string, // <int> | #<int>
  ...TRest,
];

/**
 * @see https://redis.io/commands/bitfield
 */
export class BitFieldCommand<T = Promise<number[]>> {
  private command: (string | number)[];

  constructor(
    args: [key: string],
    private client: Requester,
    private opts?: CommandOptions<number[], number[]>,
    private execOperation = (command: Command<number[], number[]>) =>
      command.exec(this.client) as T,
  ) {
    this.command = ["bitfield", ...args];
  }

  private chain(...args: typeof this.command) {
    this.command.push(...args);
    return this;
  }

  get(...args: SubCommandArgs) {
    return this.chain("get", ...args);
  }

  set(...args: SubCommandArgs<[value: number]>) {
    return this.chain("set", ...args);
  }

  incrby(...args: SubCommandArgs<[increment: number]>) {
    return this.chain("incrby", ...args);
  }

  overflow(overflow: "WRAP" | "SAT" | "FAIL") {
    return this.chain("overflow", overflow);
  }

  exec() {
    const command = new Command(this.command, this.opts);
    return this.execOperation(command);
  }
}
