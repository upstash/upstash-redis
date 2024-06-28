import { afterAll, describe, expect, test } from "bun:test";
import { keygen, newHttpClient } from "../test-utils";
import { BitFieldCommand } from "./bitfield";

const client = newHttpClient();

const { newKey, cleanup } = keygen();
afterAll(cleanup);

describe("when key is not set", () => {
  test("returns 0", async () => {
    const key = newKey();
    const res = await new BitFieldCommand([key]).get("u4", "#0").exec(client);
    expect(res).toEqual([0]);
  });
});

describe("when key is set", () => {
  test("sets / gets value", async () => {
    const key = newKey();
    const value = 42;
    const res = await new BitFieldCommand([key])
      .set("u8", "#0", value)
      .get("u8", "#0")
      .exec(client);
    expect(res).toEqual([0, value]);
  });

  test("increments value", async () => {
    const key = newKey();
    const value = 42;
    const increment = 10;
    const res = await new BitFieldCommand([key])
      .set("u8", "#0", value)
      .incrby("u8", "#0", increment)
      .exec(client);
    expect(res).toEqual([0, value + increment]);
  });

  test("overflows", async () => {
    const key = newKey();
    const value = 255;
    const res = await new BitFieldCommand([key])
      .set("u8", "#0", value)
      .incrby("u8", "#0", 10)
      .overflow("WRAP")
      .exec(client);
    expect(res).toEqual([0, value]);
  });
});
