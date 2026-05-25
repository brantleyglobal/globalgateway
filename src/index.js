const methods = {
  // 🔹 TransactionHistory
  createTransaction: async (params, env) => {
   const keys = [
      "txhash", "txtype", "", "sender", "token", "amount", "status", "chainstatus",
      "quarter", "processedat", "retrycount", "receipthash", "notes", "timestamp"
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
      "txhash", "contractaddress", "sender", "timestamp",
      "recipient", "token", "amount", "status", "chainstatus", "queuedat",
      "processedat", "priority", "retrycount", "receipthash", "notes"
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
    const {
      useraddress,
      chainstatus,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // require at least one filter
    if (typeof useraddress === "undefined" && typeof chainstatus === "undefined") {
      return { error: "Missing query parameter" };
    }

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "txhash", "chainstatus", "amount", "sender", "recipient"]);
    const allowedOrder = new Set(["asc", "desc"]);
    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM transfers`;
    const filters = [];
    const values = [];

    // useraddress: case-insensitive match against sender or recipient
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("(LOWER(sender) = ? OR LOWER(recipient) = ?)");
      const ua = String(useraddress).toLowerCase();
      values.push(ua, ua);
    }

    // chainstatus may be boolean false; check explicitly for undefined
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_TRANSFERS.prepare(query).bind(...values).all();
    return { transfers: rows.results };
  },

  updateTransfer: async (params, env) => {
    // 1. Load existing swap
    const row = await env.DB_TRANSFERS
      .prepare(`SELECT txhash FROM transfers WHERE chainstatus = ?`)
      .bind(params.chainstatus)
      .first();

    if (!row) return { error: "Tranfer not found" };
    const txhash = params.txhash ?? row.txhash;

    // 3. Build UPDATE only for mutable fields
    await env.DB_TRANSFERS
      .prepare(`
        UPDATE transfers
        SET txhash = ?, timestamp = ?
        WHERE chainstatus = ?
      `)
      .bind(
        txhash,
        params.timestamp,
        params.chainstatus ? 1 : 0,
      )
      .run();

    return { updated: true };
  },

  // 🔹 Vault
  getVault: async (params, env) => {
    const {
      useraddress,
      depositstarttime,
      chainstatus,
      committedquarters,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "useraddress", "depositstarttime", "committedquarters", "chainstatus"]);
    const allowedOrder = new Set(["asc", "desc"]);
    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM vault`;
    const filters = [];
    const values = [];

    // useraddress: case-insensitive match
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("LOWER(useraddress) = ?");
      values.push(String(useraddress).toLowerCase());
    }

    // depositstarttime exact match if provided
    if (typeof depositstarttime !== "undefined" && depositstarttime !== null && String(depositstarttime).trim() !== "") {
      filters.push("depositstarttime = ?");
      values.push(depositstarttime);
    }

    // committedquarters exact match if provided
    if (typeof committedquarters !== "undefined" && committedquarters !== null && String(committedquarters).trim() !== "") {
      filters.push("committedquarters = ?");
      values.push(committedquarters);
    }

    // chainstatus may be boolean false; check explicitly for undefined
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    if (filters.length === 0) {
      return { error: "Provide at least one filter: useraddress, depositstarttime, committedquarters, or chainstatus" };
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_VAULT.prepare(query).bind(...values).all();
    return { vault: rows.results };
  },

  // 🔹 Commit
  vaultCommit: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "depositamount", "paymentmethod",
      "ispending", "isclosed", "txhash", "depositstarttime", "exchangerate",
      "status", "chainstatus", "timestamp", "queuedat", "processedat", "key",
      "retrycount", "notes", "receipthash", "committedquarters","venture"
    ];
    await env.DB_VAULT.prepare(`
      INSERT INTO vault (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { recorded: true };
  },

  updateVault: async (params, env) => {
    // 1. Load existing swap
    const row = await env.DB_VAULT
      .prepare(`SELECT txhash FROM vault WHERE chainstatus = ?`)
      .bind(params.chainstatus)
      .first();

    if (!row) return { error: "Vault Deposit not found" };

    const txhash = params.txhash ?? row.txhash;

    // 3. Build UPDATE only for mutable fields
    await env.DB_VAULT
      .prepare(`
        UPDATE vault
        SET txhash = ?, timestamp = ?
        WHERE chainstatus = ?
      `)
      .bind(
        txhash,
        params.timestamp,
        params.chainstatus ? 1 : 0,
      )
      .run();

    return { updated: true };
  },

  // 🔹 Region
  getRegion: async (params, env) => {
    const {
      useraddress,
      depositstarttime,
      chainstatus,
      committedquarters,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "region", "depositstarttime", "committedquarters", "chainstatus"]);
    const allowedOrder = new Set(["asc", "desc"]);
    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM infra`;
    const filters = [];
    const values = [];

    // useraddress: case-insensitive match
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("LOWER(useraddress) = ?");
      values.push(String(useraddress).toLowerCase());
    }

    // depositstarttime exact match if provided
    if (typeof depositstarttime !== "undefined" && depositstarttime !== null && String(depositstarttime).trim() !== "") {
      filters.push("depositstarttime = ?");
      values.push(depositstarttime);
    }

    // committedquarters exact match if provided
    if (typeof committedquarters !== "undefined" && committedquarters !== null && String(committedquarters).trim() !== "") {
      filters.push("committedquarters = ?");
      values.push(committedquarters);
    }

    // chainstatus may be boolean false; check explicitly for undefined
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    if (filters.length === 0) {
      return { error: "Provide at least one filter: useraddress, depositstarttime, committedquarters, or chainstatus" };
    }

    query += ` WHERE ${filters.join(" AND ")}`;
    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_INFRA.prepare(query).bind(...values).all();
    return { infra: rows.results };
  },

  // 🔹 Commit
  regionCommit: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "depositamount", "paymentmethod", "exchangerate",
      "ispending", "isclosed", "txhash", "depositstarttime", "venture", "status",
      "chainstatus", "timestamp", "queuedat", "processedat", "retrycount",
      "notes", "receipthash", "committedquarters"
    ];
    await env.DB_INFRA.prepare(`
      INSERT INTO infra (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { recorded: true };
  },

  updateRegion: async (params, env) => {
    // 1. Load existing swap
    const row = await env.DB_INFRA
      .prepare(`SELECT txhash FROM infra WHERE chainstatus = ?`)
      .bind(params.chainstatus)
      .first();

    if (!row) return { error: "Venture Deposit not found" };

    const txhash = params.txhash ?? row.txhash;

    // 3. Build UPDATE only for mutable fields
    await env.DB_INFRA
      .prepare(`
        UPDATE infra
        SET txhash = ?, timestamp = ?
        WHERE chainstatus = ?
      `)
      .bind(
        txhash,
        params.timestamp,
        params.chainstatus ? 1 : 0,
      )
      .run();

    return { updated: true };
  },


  // 🔹 Purchase
  recordPurchase: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "asset", "amount", "quantity", "payout", "refund",
      "timestamp", "txhash", "paymentmethod", "status", "chainstatus", "affiliate", "refundhash",
      "queuedat", "processedat", "priority", "commission", "retrycount", "notes",
      "receipthash", "configs", "region", "exchangerate", "shippingamount"
    ];
    await env.DB_PURCHASE.prepare(`
      INSERT INTO purchases (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { recorded: true };
  },

  // 🔹 Purchase
  getPurchase: async (params, env) => {
    const {
      useraddress,
      affiliate,
      chainstatus,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "txhash", "chainstatus", "payout"]);
    const allowedOrder = new Set(["asc", "desc"]);

    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM purchases`;
    const filters = [];
    const values = [];

    // Add filters independently (no else-if)
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("LOWER(useraddress) = ?");
      values.push(String(useraddress).toLowerCase());
    }

    // chainstatus may be boolean false; check for undefined explicitly
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    if (typeof affiliate !== "undefined" && affiliate !== null && String(affiliate).trim() !== "") {
      filters.push("affiliate = ?");
      values.push(affiliate);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    // Use whitelisted sort column and direction
    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    // Ensure page and pageSize are numbers
    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_PURCHASE.prepare(query).bind(...values).all();
    return { purchases: rows.results };
  },

  updatePurchase: async (params, env) => { 
    const selector = params.contractaddress ?? params.id;
    if (!selector) return { error: "Missing selector (contractaddress or id)" };

    const row = await env.DB_PURCHASE
      .prepare(
        `SELECT id, contractaddress, txhash, amount, chainstatus, payout, timestamp, refund, refundhash
        FROM purchases
        WHERE contractaddress = ?`
      )
      .bind(selector)
      .first();

    if (!row) return { error: "Purchase not found" };

    // Optional lightweight validation for payout string
    const isValidPayout = v => v === null || typeof v === "string";
    if (typeof params.payout !== "undefined" && !isValidPayout(params.payout)) {
      return { error: "Invalid payout format; expected string or null" };
    }

    // 3. Determine delta or absolute update
    let delta = toBigIntSafe(params.deltaAmount);
    let newAbsolute = toBigIntSafe(params.newAmount);

    if (newAbsolute !== null) {
      const current = BigInt(row.amount ?? "0");
      delta = newAbsolute - current;
    }

    if (delta === null) {
      // If no amount change is intended, allow hash-only updates
      delta = 0n;
    }

    const sign = params.isRefund ? -1n : 1n;
    const appliedDelta = sign * delta;

    const set = [];
    const binds = [];

    if (typeof params.txhash !== "undefined") {
      set.push("txhash = ?");
      binds.push(params.txhash ?? null);
    }
    if (typeof params.chainstatus !== "undefined") {
      set.push("chainstatus = ?");
      binds.push(params.chainstatus);
    }
    if (typeof params.payout !== "undefined") {
      set.push("payout = ?");
      binds.push(params.payout ?? null); // keep as string
    }
    if (typeof params.refund !== "undefined") {
      set.push("refund = ?");
      binds.push(params.refund ?? null); // keep as string
    }
    if (params.refundhash !== undefined) {
      set.push("refundhash = ?");
      binds.push(params.refundhash ?? null);
    }
    if (typeof params.timestamp !== "undefined") {
      set.push("timestamp = ?");
      binds.push(params.timestamp);
    }
    if (appliedDelta !== 0n) {
      set.push("amount = amount + ?");
      binds.push(appliedDelta.toString());
    }

    if (set.length === 0) return { error: "No updatable fields provided" };

    const sql = `UPDATE purchases SET ${set.join(", ")} WHERE contractaddress = ?`;
    binds.push(selector);

    await env.DB_PURCHASE.prepare(sql).bind(...binds).run();

    // Insert audit/history row storing payout as string
    await env.DB_PURCHASE
      .prepare(
        `INSERT INTO purchase_history
        (contractaddress, txhash, amount, chainstatus, payout, timestamp, notes, idempotency_key, refund, refundhash)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .bind(
        selector,
        typeof params.txhash !== "undefined" ? params.txhash : row.txhash,
        appliedDelta.toString(),
        typeof params.chainstatus !== "undefined" ? params.chainstatus : row.chainstatus,
        typeof params.payout !== "undefined" ? params.payout : row.payout, // string
        params.timestamp ?? Date.now(),
        typeof params.notes === "string" ? params.notes : JSON.stringify(params.notes ?? ""),
        params.idempotencyKey ?? null,
        params.isRefund ? 1 : 0,
        params.refundhash ?? null,
      )
      .run();

    const updated = await env.DB_PURCHASE
      .prepare(
        `SELECT id, contractaddress, txhash, amount, chainstatus, payout, timestamp
        FROM purchases
        WHERE contractaddress = ?`
      )
      .bind(selector)
      .first();

    return { updated: true, purchase: updated };
  },

  // 🔹 Swap
  executeSwap: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "initiator", "tokena", "tokenb", "counterparty", "amounta", "amountb",
      "depositahash", "depositbhash", "txhash","paymentmethod", "status", "chainstatus", "timestamp", "refund",
      "newcontract", "queuedat", "processedat", "priority", "retrycount", "notes"
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
    const {
      useraddress,
      chainstatus,
      contractaddress,
      notes,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "contractaddress", "chainstatus", "amounta", "amountb"]);
    const allowedOrder = new Set(["asc", "desc"]);

    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM swaps`;
    const filters = [];
    const values = [];

    // useraddress: match useraddress OR initiator OR counterparty (case-insensitive)
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("(LOWER(useraddress) = ? OR LOWER(initiator) = ? OR LOWER(counterparty) = ?)");
      const ua = String(useraddress).toLowerCase();
      values.push(ua, ua, ua);
    }

    // chainstatus may be boolean false; check explicitly for undefined
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    // contractaddress: case-insensitive exact match
    if (typeof contractaddress !== "undefined" && contractaddress !== null && String(contractaddress).trim() !== "") {
      filters.push("LOWER(contractaddress) = ?");
      values.push(String(contractaddress).toLowerCase());
    }

    // notes: partial match (LIKE) if provided
    if (typeof notes !== "undefined" && notes !== null && String(notes).trim() !== "") {
      filters.push("notes LIKE ?");
      values.push(`%${String(notes)}%`);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_SWAP.prepare(query).bind(...values).all();
    return { swaps: rows.results };
  },

  updateSwap: async (params, env) => {
    // Helper validators
    const isHexHash = v =>
      typeof v === "string" && /^0x[0-9a-fA-F]{6,}$/i.test(v); // adjust length as needed

    const isAddress = v =>
      typeof v === "string" && /^0x[a-fA-F0-9]{40}$/.test(v);

    const toBigIntSafe = v => {
      if (typeof v === "bigint") return v;
      if (typeof v === "number") return BigInt(Math.trunc(v));
      if (typeof v === "string" && v !== "") return BigInt(v);
      return null;
    };

    // 1. Load existing swap
    const row = await env.DB_SWAP
      .prepare(
        `SELECT initiator, counterparty, amounta, amountb, depositahash, depositbhash
        FROM swaps
        WHERE contractaddress = ?`
      )
      .bind(params.contractaddress)
      .first();

    if (!row) return { error: "Swap not found" };

    // 2. Normalize signer and side
    const signer = (params.signer ?? "").toLowerCase();
    if (!isAddress(signer)) return { error: "Invalid signer address" };
    const initiator = (row.initiator ?? "").toLowerCase();
    const counterparty = (row.counterparty ?? "").toLowerCase();
    const isInitiator = signer === initiator;
    const isCounterparty = signer === counterparty;
    if (!isInitiator && !isCounterparty) return { error: "Signer is neither initiator nor counterparty" };

    // 3. Determine delta or absolute update
    let delta = toBigIntSafe(params.deltaAmount);
    let newAbsolute = toBigIntSafe(params.newAmount);

    if (newAbsolute !== null) {
      const current = isInitiator
      ? BigInt(row.amounta ?? "0")
      : BigInt(row.amountb ?? "0");

      delta = newAbsolute - current;
    }

    if (delta === null) {
      // If no amount change is intended, allow hash-only updates
      delta = 0n;
    }

    const sign = params.isRefund ? -1n : 1n;
    const appliedDelta = sign * delta;

    // 4. Compute new amounts and guard negatives
    const currentA = BigInt(row.amounta ?? "0");
    const currentB = BigInt(row.amountb ?? "0");
    const newAmountA = isInitiator ? currentA + appliedDelta : currentA;
    const newAmountB = isCounterparty ? currentB + appliedDelta : currentB;
    if (newAmountA < 0n || newAmountB < 0n) return { error: "Resulting amount would be negative" };

    // 5. Build dynamic UPDATE clause
    const setClauses = [];
    const bindValues = [];

    // Always update amounts (we computed them)
    setClauses.push("amounta = ?");
    bindValues.push(newAmountA.toString());
    setClauses.push("amountb = ?");
    bindValues.push(newAmountB.toString());

    // Optional fields only if provided
    if (typeof params.isRefund === "boolean") {
      setClauses.push("refund = ?");
      bindValues.push(params.isRefund ? 1 : 0);
    }
    if (typeof params.timestamp !== "undefined") {
      setClauses.push("timestamp = ?");
      bindValues.push(params.timestamp);
    }
    if (typeof params.newcontract !== "undefined") {
      setClauses.push("newcontract = ?");
      bindValues.push(params.newcontract);
    }
    if (typeof params.notes !== "undefined") {
      setClauses.push("notes = ?");
      bindValues.push(
        typeof params.notes === "string"
          ? params.notes
          : JSON.stringify(params.notes ?? "")
      );

    }

    // depositahash and depositbhash only if provided and valid
    if (typeof params.depositahash !== "undefined") {
      if (params.depositahash !== null && !isHexHash(params.depositahash)) {
        return { error: "Invalid depositahash format" };
      }
      setClauses.push("depositahash = ?");
      bindValues.push(params.depositahash ?? null);
    }
    if (typeof params.depositbhash !== "undefined") {
      if (params.depositbhash !== null && !isHexHash(params.depositbhash)) {
        return { error: "Invalid depositbhash format" };
      }
      setClauses.push("depositbhash = ?");
      bindValues.push(params.depositbhash ?? null);
    }

    if (setClauses.length === 0) return { error: "No updatable fields provided" };

    const sql = `UPDATE swaps SET ${setClauses.join(", ")} WHERE contractaddress = ?`;
    bindValues.push(params.contractaddress);

    // 6. Execute update
    await env.DB_SWAP.prepare(sql).bind(...bindValues).run();

    // 7. Persist history so there is a link between the request and the change
    await env.DB_SWAP
      .prepare(
        `INSERT INTO swap_history
        (contractaddress, signer, delta, isRefund, timestamp, notes, depositahash, depositbhash, idempotency_key)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`
      )
      .bind(
        params.contractaddress,
        params.signer,
        appliedDelta.toString(),
        params.isRefund ? 1 : 0,
        params.timestamp ?? Date.now(),
        typeof params.notes === "string" ? params.notes : JSON.stringify(params.notes ?? ""),
        typeof params.depositahash !== "undefined" ? params.depositahash : null,
        typeof params.depositbhash !== "undefined" ? params.depositbhash : null,
        params.idempotencyKey ?? null
      )
      .run();

    // 8. Return updated row
    const updated = await env.DB_SWAP
      .prepare(
        `SELECT initiator, counterparty, amounta, amountb, depositahash, depositbhash, notes, newcontract, refund, timestamp
        FROM swaps WHERE contractaddress = ?`
      )
      .bind(params.contractaddress)
      .first();

    return { updated: true, swap: updated };
  },

  // 🔹 Acquisitions
  acquisitionCommit: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "amountout", "amountin", "exchangerate",
      "txhash", "paymentmethod", "status", "receipthash", "chainstatus",
      "processedat", "notes",  "timestamp"
    ];
    
    const placeholders = keys.map(() => "?").join(", ");
    const values = keys.map(k => params[k]);

    await env.DB_ACQUISITION.prepare(`
      INSERT INTO acquisitions (${keys.join(", ")})
      VALUES (${placeholders})
    `).bind(...values).run();

    return { acquisitioned: true };
  },

  getAcquisition: async (params, env) => {
    const {
      useraddress,
      chainstatus,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "useraddress", "chainstatus", "amount"]);
    const allowedOrder = new Set(["asc", "desc"]);

    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM acquisitions`;
    const filters = [];
    const values = [];

    // useraddress: case-insensitive match
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("LOWER(useraddress) = ?");
      values.push(String(useraddress).toLowerCase());
    }

    // chainstatus may be boolean false; check explicitly for undefined
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_ACQUISITION.prepare(query).bind(...values).all();
    return { acquisitions: rows.results };
  },
  
  updateAcquisition: async (params, env) => {
    // 1. Load existing swap
    const row = await env.DB_ACQUISITION
      .prepare(`SELECT txhash FROM acquisitions WHERE chainstatus = ?`)
      .bind(params.chainstatus)
      .first();

    if (!row) return { error: "Acquisition not found" };

    const txhash = params.txhash ?? row.txhash;

    // 3. Build UPDATE only for mutable fields
    await env.DB_ACQUISITION
      .prepare(`
        UPDATE acquisitions
        SET txhash = ?, timestamp = ?
        WHERE chainstatus = ?
      `)
      .bind(
        txhash,
        params.timestamp,
        params.chainstatus ? 1 : 0,
      )
      .run();

    return { updated: true };
  },

  // 🔹 Redemptions
  redeemToken: async (params, env) => {
    const keys = [
      "contractaddress", "useraddress", "amount", "chainstatus",
      "timestamp", "txhash", "asset", "status", "modification",
      "queuedat", "processedat", "priority", "retrycount", "notes",
      "receipthash"
    ];

    await env.DB_REDEMPTIONS.prepare(`
      INSERT INTO redemptions (${keys.join(", ")})
      VALUES (${keys.map(() => "?").join(", ")})
    `).bind(...keys.map(k => params[k])).run();
    return { redeemed: true };
  },

  getRedemption: async (params, env) => {
    const {
      useraddress,
      chainstatus,
      page = 1,
      pageSize = 10,
      sortBy = "timestamp",
      sortOrder = "desc",
    } = params;

    // Whitelist sortable columns and order to avoid SQL injection
    const allowedSortBy = new Set(["timestamp", "id", "txhash", "chainstatus", "amount"]);
    const allowedOrder = new Set(["asc", "desc"]);

    const orderCol = allowedSortBy.has(sortBy) ? sortBy : "timestamp";
    const orderDir = allowedOrder.has(String(sortOrder).toLowerCase()) ? String(sortOrder).toLowerCase() : "desc";

    let query = `SELECT * FROM redemptions`;
    const filters = [];
    const values = [];

    // useraddress: case-insensitive match
    if (typeof useraddress !== "undefined" && useraddress !== null && String(useraddress).trim() !== "") {
      filters.push("LOWER(useraddress) = ?");
      values.push(String(useraddress).toLowerCase());
    }

    // chainstatus may be boolean false; check explicitly for undefined
    if (typeof chainstatus !== "undefined" && chainstatus !== null) {
      filters.push("chainstatus = ?");
      values.push(chainstatus === true ? 1 : chainstatus === false ? 0 : chainstatus);
    }

    if (filters.length > 0) {
      query += ` WHERE ${filters.join(" AND ")}`;
    }

    query += ` ORDER BY ${orderCol} ${orderDir}`;
    query += ` LIMIT ? OFFSET ?`;

    const limit = Number(pageSize) || 10;
    const offset = (Math.max(Number(page) || 1, 1) - 1) * limit;
    values.push(limit, offset);

    const rows = await env.DB_REDEMPTIONS.prepare(query).bind(...values).all();
    return { redemptions: rows.results };
  },

  updateRedemption: async (params, env) => {
    // 1. Load existing swap
    const row = await env.DB_REDEMPTIONS
      .prepare(`SELECT txhash FROM acquisitions WHERE chainstatus = ?`)
      .bind(params.chainstatus)
      .first();

    if (!row) return { error: "Redemption not found" };

    // 3. Build UPDATE only for mutable fields
    await env.DB_REDEMPTIONS
      .prepare(`
        UPDATE redemptions
        SET txhash = ?, timestamp = ?
        WHERE chainstatus = ?
      `)
      .bind(
        txhash,
        params.chainstatus ? 1 : 0,
        params.timestamp,
      )
      .run();

    return { updated: true };
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

      //Step 4: Call method
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
