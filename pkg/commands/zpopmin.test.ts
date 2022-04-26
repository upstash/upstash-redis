import { keygen, newHttpClient } from "../test-utils.ts";

import {
  afterAll,
  describe,
  it,
} from "https://deno.land/std@0.136.0/testing/bdd.ts";
import { ZAddCommand } from "./zadd.ts";
import { ZPopMinCommand } from "./zpopmin.ts";
import { assertEquals } from "https://deno.land/std@0.136.0/testing/asserts.ts";

const client = newHttpClient();

const { newKey, cleanup } = keygen();
afterAll(cleanup);

describe(
  "without options",
  () => {
    it(
      "returns the popped elements",
      async () => {
        const key = newKey();
        const score1 = 1;
        const member1 = Math.random().toString();
        const score2 = 2;
        const member2 = Math.random().toString();
        const score3 = 3;
        const member3 = Math.random().toString();
        await new ZAddCommand(
          key,
          { score: score1, member: member1 },
          { score: score2, member: member2 },
          { score: score3, member: member3 },
        ).exec(client);
        const res = await new ZPopMinCommand(key).exec(client);
        assertEquals(res, [member1, score1]);
      },
    );
  },
);

describe(
  "with count",
  () => {
    it(
      "returns the popped elements",
      async () => {
        const key = newKey();
        const score1 = 1;
        const member1 = Math.random().toString();
        const score2 = 2;
        const member2 = Math.random().toString();
        const score3 = 3;
        const member3 = Math.random().toString();
        await new ZAddCommand(
          key,
          { score: score1, member: member1 },
          { score: score2, member: member2 },
          { score: score3, member: member3 },
        ).exec(client);
        const res = await new ZPopMinCommand(key, 2).exec(client);
        assertEquals(res, [member1, score1, member2, score2]);
      },
    );
  },
);
