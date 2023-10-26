import { newHttpClient, randomID } from "../test-utils";
import { ScriptLoadCommand } from "./script_load";

import { ScriptExistsCommand } from "./script_exists";
import { ScriptFlushCommand } from "./script_flush";
const client = newHttpClient();

test("sync", () => {
  test("flushes all scripts", async () => {
    const script = `return "${randomID()}"`;
    const sha1 = await new ScriptLoadCommand([script]).exec(client);
    expect(await new ScriptExistsCommand([sha1]).exec(client), [1]);

    const res = await new ScriptFlushCommand([{ sync: true }]).exec(client);
    expect(res).toEqual("OK");
    expect(await new ScriptExistsCommand([sha1]).exec(client), [0]);
  });
});

test("async", () => {
  test("flushes all scripts", async () => {
    const script = `return "${randomID()}"`;
    const sha1 = await new ScriptLoadCommand([script]).exec(client);
    expect(await new ScriptExistsCommand([sha1]).exec(client), [1]);

    const res = await new ScriptFlushCommand([{ async: true }]).exec(client);

    expect(res).toEqual("OK");

    await new Promise((res) => setTimeout(res, 5000));
    expect(await new ScriptExistsCommand([sha1]).exec(client), [0]);
  });
});
