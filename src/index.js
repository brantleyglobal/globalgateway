const methods = {
  // 🔹 TransactionHistory
  createTransaction: async (params, env) => {
   const keys = [
      "txhash", "contractaddress", "calldata", "signature", "sender", "smartwallet", "poolamount",
      "token", "amount", "status", "chainstatus", "queuedat", "quarter",
      "processedat", "priority", "retrycount", "receipthash", "notes", "timestamp"
    ];

    const placeholders = keys.map(() => "?").join(", ");
    const values = keys.map(key => params[key]);

    await env.DB_TRANSACTIONHISTORY.prepare(`
      INSERT INTO transactionhistory (${keys.join(", ")})
      VALUES (${placeholders})
    `).bind(...values).run();

    return { success: true };
  },

  getTransactionHistory: async (params, env) => {
    const { quarter, page = 1, pageSize = 10, sortBy = "timestamp", sortOrder = "desc" } = params;

    let query = "";
    let value;

    if (quarter) {
      query = `SELECT * FROM transactionhistory WHERE quarter = ?`;
    } else {
      return { error: "Missing query parameter" };
    }

    const rows = await env.DB_TRANSACTIONHISTORY.prepare(query).bind(value).all();
    return { transactionhistory: rows.results };
  },

  // 🔹 Transfers
  createTransfer: async (params, env) => {
    const keys = [
      "txhash", "contractaddress", "calldata", "signature", "sender", "smartwallet",
      "recipient", "token", "amount", "status", "chainstatus", "queuedat",
      "processedat", "priority", "retrycount", "receipthash", "notes", "timestamp"
    ];

    const placeholders = keys.map(() => "?").join(", ");
    const values = keys.map(key => params[key]);

    await env.DB_TRANSFERS.prepare(`
      INSERT INTO transfers (${keys.join(", ")})
      VALUES (${placeholders})
    `).bind(...values).run();

    return { success: true };
  },

  getTransfer: async (params, env) => {
    const { useraddress, chainstatus, page = 1, pageSize = 10, sortBy = "timestamp", sortOrder = "desc" } = params;

    if (!useraddress && !chainstatus) {
      return { error: "Missing query parameter" };
    }

    let query = `SELECT * FROM transfers`;
    let filters = [];
    let values = [];

    if (useraddress) {
      filters.push("(sender = ? OR recipient = ?)");
      values.push(useraddress, useraddress);
    }

    if (chainstatus) {
      filters.push("chainstatus = ?");
      values.push(chainstatus);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${sortBy} ${sortOrder}`;
    query += ` LIMIT ? OFFSET ?`;
    values.push(pageSize, (page - 1) * pageSize);

    const rows = await env.DB_TRANSFERS.prepare(query).bind(...values).all();
    return { transfers: rows.results };
  },

  // 🔹 Vault
  getVault: async (params, env) => {
    const { useraddress, depositstarttime, chainstatus, committedquarters, page = 1, pageSize = 10, sortBy = "timestamp", sortOrder = "desc" } = params;

    // Enforce only one filter
    if (useraddress && depositstarttime) {
      return { error: "Please provide only one filter: useraddress or depositstarttime" };
    }

    let query = "";
    let value;

    if (useraddress) {
      query = `SELECT * FROM vault WHERE useraddress = ?`;
      value = useraddress;
    } else if (depositstarttime) {
      query = `SELECT * FROM vault WHERE depositstarttime = ?`;
      value = depositstarttime;
    } else if (committedquarters) {
      query = `SELECT * FROM vault WHERE committedquarters = ?`;
      value = committedquarters;
    } else if (chainstatus) {
      query = `SELECT * FROM vault WHERE chainstatus = ?`;
      value = chainstatus;
    } else {
      return { error: "Missing query parameter" };
    }

    const rows = await env.DB_VAULT.prepare(query).bind(value).all();
    return { vault: rows.results };
  },


  // 🔹 Commit
  vaultCommit: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "depositamount", "paymentmethod",
      "ispending", "isclosed", "txhash", "signature", "depositstarttime",
      "calldata", "status", "chainstatus", "timestamp", "queuedat", "processedat",
      "retrycount", "notes", "receipthash", "smartwallet", "committedquarters"
    ];
    await env.DB_VAULT.prepare(`
      INSERT INTO vault (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { recorded: true };
  },


  // 🔹 Purchase
  recordPurchase: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "asset", "amount", "quantity", "paymentmethod",
      "timestamp", "txhash", "signature", "calldata", "status", "chainstatus",
      "queuedat", "processedat", "priority", "retrycount", "notes",
      "receipthash", "smartwallet"
    ];
    await env.DB_PURCHASE.prepare(`
      INSERT INTO purchases (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { recorded: true };
  },

  // 🔹 Purchase
  getPurchase: async (params, env) => {
    const { useraddress, chainstatus, page = 1, pageSize = 10, sortBy = "timestamp", sortOrder = "desc" } = params;

    let query = `SELECT * FROM purchases`;
    let filters = [];
    let values = [];

    if (useraddress) {
      filters.push("useraddress = ?");
      values.push(useraddress);
    } else if (chainstatus) {
      filters.push("chainstatus = ?");
      values.push(chainstatus);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${sortBy} ${sortOrder}`;
    query += ` LIMIT ? OFFSET ?`;
    values.push(pageSize, (page - 1) * pageSize);

    const rows = await env.DB_PURCHASE.prepare(query).bind(...values).all();
    return { purchases: rows.results };
  },



  // 🔹 Swap
  executeSwap: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "direction", "amountin", "amountout", "exchangerate",
      "txhash", "signature", "calldata", "status", "chainstatus", "timestamp",
      "queuedat", "processedat", "priority", "retrycount", "notes", "smartwallet"
    ];
    const placeholders = keys.map(() => "?").join(", ");
    const values = keys.map(k => params[k]);

    await env.DB_SWAP.prepare(`
      INSERT INTO swaps (${keys.join(", ")})
      VALUES (${placeholders})
    `).bind(...values).run();

    return { swapped: true };
  },

  getSwap: async (params, env) => {
    const { useraddress, chainstatus, page = 1, pageSize = 10, sortBy = "timestamp", sortOrder = "desc" } = params;

    // Enforce only one filter
    if (useraddress && chainstatus) {
      return { error: "Please provide only one filter: useraddress or chainstatus" };
    }

    let query = "";
    let value;

    if (useraddress) {
      query = `SELECT * FROM purchases WHERE useraddress = ?`;
      value = useraddress;
    } else if (chainstatus) {
      query = `SELECT * FROM purchases WHERE chainstatus = ?`;
      value = chainstatus;
    } else {
      return { error: "Missing query parameter" };
    }

    const rows = await env.DB_SWAP.prepare(query).bind(value).all();
    return { swaps: rows.results };
  },


  // 🔹 Redemptions
  redeemToken: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "vaultid", "amount", "paymentmethod",
      "timestamp", "txhash", "signature", "calldata", "status", "chainstatus",
      "queuedat", "processedat", "priority", "retrycount", "notes",
      "receipthash", "smartwallet"
    ];
    await env.DB_REDEMPTIONS.prepare(`
      INSERT INTO redemptions (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { redeemed: true };
  },

  getRedemption: async (params, env) => {
    const { useraddress, chainstatus, page = 1, pageSize = 10, sortBy = "timestamp", sortOrder = "desc" } = params;

    // Enforce only one filter
    if (useraddress && chainstatus) {
      return { error: "Please provide only one filter: useraddress or chainstatus" };
    }

    let query = "";
    let value;

    if (useraddress) {
      query = `SELECT * FROM purchases WHERE useraddress = ?`;
      value = useraddress;
    } else if (chainstatus) {
      query = `SELECT * FROM purchases WHERE chainstatus = ?`;
      value = chainstatus;
    } else {
      return { error: "Missing query parameter" };
    }

    const rows = await env.DB_REDEMPTIONS.prepare(query).bind(value).all();
    return { redemptions: rows.results };
  },

};

export default {
  async fetch(request, env) {
    // Step 1: Handle CORS preflight
    if (request.method === "OPTIONS") {
      return new Response(null, {
        status: 204,
        headers: {
          "Access-Control-Allow-Origin": "https://brantley-global.com",
          "Access-Control-Allow-Methods": "POST, OPTIONS",
          "Access-Control-Allow-Headers": "Content-Type, x-api-key",
          "Access-Control-Max-Age": "86400"
        }
      });
    }

    try {
      // Step 2: API key validation
      const apiKey = request.headers.get("x-api-key");
      if (apiKey !== env.API_SECRET) {
        return new Response("Unauthorized", {
          status: 401,
          headers: {
            "Access-Control-Allow-Origin": "https://brantley-global.com"
          }
        });
      }

      // Step 3: Parse request
      const body = await request.json();
      const { method, params, id } = body;

      if (!method || !methods[method]) {
        return Response.json({
          jsonrpc: "2.0",
          id,
          error: { code: -32601, message: "Method not found" }
        }, {
          headers: {
            "Access-Control-Allow-Origin": "https://brantley-global.com"
          }
        });
      }

      // 🚀 Step 4: Call method
      const result = await methods[method](params, env);
      return Response.json({
        jsonrpc: "2.0",
        id,
        result
      }, {
        headers: {
          "Access-Control-Allow-Origin": "https://brantley-global.com"
        }
      });

    } catch (err) {
      return Response.json({
        jsonrpc: "2.0",
        id: null,
        error: { code: -32000, message: err.message }
      }, {
        headers: {
          "Access-Control-Allow-Origin": "https://brantley-global.com"
        }
      });
    }
  }
};
