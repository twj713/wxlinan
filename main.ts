// 仿微信 · AI 酒馆 · WebSocket 后端（修复与加固版）
// 修复内容：
//  1. 修复 sendMessage 中的语法错误（原第 256 行 "recipie } else {" 导致无法编译）
//  2. 新增身份令牌（token）鉴权，防止任何人冒充任意 ID / 管理员
//  3. sendMessage 增加群成员校验、图片字段、消息 id（断线补发去重）
//  4. 新增 deleteMessage：服务端删除消息并广播 messageDeleted（修复前端"删除后复活"）
//  5. rejectFriend 改为通知申请方（原来只通知拒绝方自己）
//  6. acceptFriend 校验申请确实存在
//  7. createGroup 去重成员、校验成员已注册
//  8. getMessages 增加会话权限校验
//  9. 每连接限流，防刷消息
// 10. 新增钱包（["wallets", id] 存时间余额，单位小时，初始 100）
// 11. 新增转账（sendTransfer / acceptTransfer / refundTransfer，收款制，实时到账）
// 12. 普通用户添加管理员好友 / 拉管理员进群：统一提示"对方为管理员您暂时无权添加"
const kv = await Deno.openKv();
const online = new Map<string, WebSocket>();
const ADMIN_IDS = ['1', '2'];
// 管理员密钥（与前端一致）：输入密钥即为管理员身份的引导凭证，服务端会重签新令牌
const ADMIN_KEYS: Record<string, string> = {guanliyuanlinmi: "1", guanliyuanlinan: "2"};
const MSG_LIMIT = 500;                 // 每个会话最多保留的消息数
const RATE_MSGS = 30;                  // 滑动窗口内允许的操作次数
const RATE_WINDOW = 30 * 1000;         // 滑动窗口时长（毫秒）
const IMAGE_MAX = 300000;              // 图片 base64 字符串上限
const INITIAL_BALANCE = 100;           // 新用户初始时间余额（小时）
const rateMap = new Map<WebSocket, number[]>();

// 服务端心跳：定期 ping 所有在线连接，清理假死（半开）连接
setInterval(() => {
  for (const [id, s] of online) {
    if (s.readyState !== WebSocket.OPEN) { online.delete(id); continue; }
    try { s.ping(); } catch { online.delete(id); }
  }
}, 25000);

function isAdmin(id: string): boolean {
  return ADMIN_IDS.includes(id);
}
function chatIdOf(a: string, b: string): string {
  return a < b ? `f_${a}_${b}` : `f_${b}_${a}`;
}
function send(ws: WebSocket | undefined, obj: any) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    try { ws.send(JSON.stringify(obj)); } catch(_e) {}
  }
}
function rateLimit(ws: WebSocket): boolean {
  const now = Date.now();
  const arr = (rateMap.get(ws) || []).filter((t) => now - t < RATE_WINDOW);
  if (arr.length >= RATE_MSGS) { rateMap.set(ws, arr); return false; }
  arr.push(now);
  rateMap.set(ws, arr);
  return true;
}
async function authed(id: string, token: unknown): Promise<boolean> {
  if (!id || !token) return false;
  const stored = (await kv.get(["tokens", id])).value;
  return !!stored && stored === String(token);
}

Deno.serve((req) => {
  const url = new URL(req.url);
  if (url.pathname === "/ws") {
    const { socket, response } = Deno.upgradeWebSocket(req);
    let userId = "";
    socket.onmessage = async (e) => {
      let msg: any;
      try { msg = JSON.parse(e.data); } catch { return; }
      try { await handle(socket, msg, (id) => { userId = id; }); }
      catch(err) { send(socket, {type: "error", msg: String(err)}); }
    };
    socket.onclose = () => {
      rateMap.delete(socket);
      if (userId && online.get(userId) === socket) online.delete(userId);
    };
    return response;
  }
  return new Response("ok");
});

async function handle(ws: WebSocket, msg: any, setId: (id: string) => void) {
  const t = msg.type;

  // ---------- 心跳保活 ----------
  if (t === "ping") { send(ws, {type: "pong"}); return; }

  // ---------- 注册（签发/校验身份令牌） ----------
  if (t === "register") {
    const id = String(msg.id || "").trim();
    if (!id) return;
    const nick = String(msg.nick || "用户").slice(0, 20);
    const avatar = String(msg.avatar || "");
    const token = String(msg.token || "");
    // 管理员密钥作为管理员身份的引导凭证，验证通过后重签新令牌
    const adminKeyMatch = isAdmin(id) && ADMIN_KEYS[token] === id;
    const existing = (await kv.get(["tokens", id])).value;
    // 该 ID 已有令牌：必须匹配，否则拒绝（防止冒充已注册账号）
    if (existing && token !== existing && !adminKeyMatch) {
      send(ws, {type: "error", msg: "身份令牌不匹配，无法登录该账号"});
      return;
    }
    let finalToken = existing;
    if (adminKeyMatch) finalToken = crypto.randomUUID();
    if (!finalToken) finalToken = token || crypto.randomUUID();
    await kv.set(["tokens", id], finalToken);
    await kv.set(["users", id], {id, nick, avatar, lastSeen: Date.now()});
    const oldWs = online.get(id);
    if (oldWs && oldWs !== ws) {
      try { oldWs.close(); } catch(_e) {}
    }
    online.set(id, ws);
    setId(id);
    send(ws, {type: "registered", id, token: finalToken});
    const reqs = (await kv.get(["requests", id])).value || [];
    send(ws, {type: "requests", list: reqs});
    return;
  }

  // ---------- 资料更新 ----------
  if (t === "updateProfile") {
    const id = String(msg.id || "");
    if (!id || !(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    const nick = String(msg.nick || "用户").slice(0, 20);
    const avatar = String(msg.avatar || "");
    await kv.set(["users", id], {id, nick, avatar, lastSeen: Date.now()});
    const friends = (await kv.get(["friends", id])).value || [];
    friends.forEach((fid: string) => send(online.get(fid), {type: "friendUpdate", friendId: id, nick, avatar}));
    return;
  }

  // ---------- 好友列表 ----------
  if (t === "listFriends") {
    const id = String(msg.id || "");
    if (!(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    const friendIds = (await kv.get(["friends", id])).value || [];
    const friends = [];
    for (const fid of friendIds) {
      const u = (await kv.get(["users", fid])).value;
      if (u) friends.push(u);
    }
    send(ws, {type: "friends", list: friends});
    return;
  }

  // ---------- 添加好友 ----------
  if (t === "addFriend") {
    const fromId = String(msg.fromId || "");
    const targetId = String(msg.targetId || "").trim();
    if (!fromId || !targetId) return;
    if (!(await authed(fromId, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    if (fromId === targetId) { send(ws, {type: "addFriendResult", ok: false, reason: "不能加自己"}); return; }
    if (isAdmin(targetId) && !isAdmin(fromId)) {
      send(ws, {type: "addFriendResult", ok: false, reason: "对方为管理员您暂时无权添加"});
      return;
    }
    const from = (await kv.get(["users", fromId])).value;
    const target = (await kv.get(["users", targetId])).value;
    if (!from) { send(ws, {type: "addFriendResult", ok: false, reason: "请先注册"}); return; }
    if (!target) { send(ws, {type: "addFriendResult", ok: false, reason: "用户不存在"}); return; }
    const myFriends = (await kv.get(["friends", fromId])).value || [];
    if (myFriends.includes(targetId)) { send(ws, {type: "addFriendResult", ok: false, reason: "已经是好友"}); return; }
    const targetReqs = (await kv.get(["requests", targetId])).value || [];
    if (!targetReqs.find((r: any) => r.fromId === fromId)) {
      targetReqs.push({fromId, fromNick: from.nick, fromAvatar: from.avatar, time: Date.now()});
      await kv.set(["requests", targetId], targetReqs);
    }
    send(ws, {type: "addFriendResult", ok: true});
    send(online.get(targetId), {type: "friendRequest", fromId, fromNick: from.nick, fromAvatar: from.avatar});
    return;
  }

  // ---------- 同意好友 ----------
  if (t === "acceptFriend") {
    const myId = String(msg.myId || "");
    const fromId = String(msg.fromId || "");
    if (!myId || !fromId) return;
    if (!(await authed(myId, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const reqs = (await kv.get(["requests", myId])).value || [];
    if (!reqs.find((r: any) => r.fromId === fromId)) { send(ws, {type: "error", msg: "没有该好友申请"}); return; }
    const myFriends = (await kv.get(["friends", myId])).value || [];
    const fromFriends = (await kv.get(["friends", fromId])).value || [];
    if (!myFriends.includes(fromId)) myFriends.push(fromId);
    if (!fromFriends.includes(myId)) fromFriends.push(myId);
    await kv.set(["friends", myId], myFriends);
    await kv.set(["friends", fromId], fromFriends);
    await kv.set(["requests", myId], reqs.filter((r: any) => r.fromId !== fromId));
    send(ws, {type: "friendAdded", friendId: fromId});
    send(online.get(fromId), {type: "friendAdded", friendId: myId});
    return;
  }

  // ---------- 拒绝好友（通知申请方） ----------
  if (t === "rejectFriend") {
    const myId = String(msg.myId || "");
    const fromId = String(msg.fromId || "");
    if (!myId || !fromId) return;
    if (!(await authed(myId, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const reqs = (await kv.get(["requests", myId])).value || [];
    await kv.set(["requests", myId], reqs.filter((r: any) => r.fromId !== fromId));
    send(online.get(fromId), {type: "friendRejected", friendId: myId});
    return;
  }

  // ---------- 删除好友 ----------
  if (t === "deleteFriend") {
    const myId = String(msg.myId || "");
    const targetId = String(msg.targetId || "");
    if (!myId || !targetId) return;
    if (!(await authed(myId, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    if (isAdmin(targetId) && !isAdmin(myId)) {
      send(ws, {type: "error", msg: "不能删除管理员"});
      return;
    }
    const myFriends = (await kv.get(["friends", myId])).value || [];
    const targetFriends = (await kv.get(["friends", targetId])).value || [];
    await kv.set(["friends", myId], myFriends.filter((x: string) => x !== targetId));
    await kv.set(["friends", targetId], targetFriends.filter((x: string) => x !== myId));
    send(ws, {type: "friendDeleted", friendId: targetId});
    send(online.get(targetId), {type: "friendDeleted", friendId: myId});
    return;
  }

  // ---------- 踢出群成员 ----------
  if (t === "kickMember") {
    const gid = String(msg.gid || "");
    const operatorId = String(msg.operatorId || "");
    const targetId = String(msg.targetId || "");
    if (!gid || !operatorId || !targetId) return;
    if (!(await authed(operatorId, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const g = (await kv.get(["groups", gid])).value;
    if (!g) { send(ws, {type: "error", msg: "群不存在"}); return; }
    if (g.ownerId !== operatorId && !isAdmin(operatorId)) {
      send(ws, {type: "error", msg: "只有群主或管理员才能踢人"});
      return;
    }
    if (isAdmin(targetId) && !isAdmin(operatorId)) {
      send(ws, {type: "error", msg: "不能踢出管理员"});
      return;
    }
    if (targetId === g.ownerId) {
      send(ws, {type: "error", msg: "不能踢出群主"});
      return;
    }
    g.members = g.members.filter((m: string) => m !== targetId);
    await kv.set(["groups", gid], g);
    const gl = (await kv.get(["userGroups", targetId])).value || [];
    await kv.set(["userGroups", targetId], gl.filter((x: string) => x !== gid));
    g.members.forEach((mid: string) => send(online.get(mid), {type: "groupUpdate", group: g}));
    send(online.get(targetId), {type: "kickedFromGroup", group: g});
    return;
  }

  // ---------- 创建群聊 ----------
  if (t === "createGroup") {
    const ownerId = String(msg.ownerId || "");
    const name = String(msg.name || "群聊").slice(0, 20);
    let memberIds = Array.isArray(msg.members) ? msg.members.map((m: any) => String(m)) : [];
    if (!ownerId) return;
    if (!(await authed(ownerId, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    memberIds = Array.from(new Set(memberIds));
    if (!memberIds.includes(ownerId)) memberIds.push(ownerId);
    if (!isAdmin(ownerId)) {
      const adminInGroup = memberIds.some((mid: string) => isAdmin(mid));
      if (adminInGroup) {
        send(ws, {type: "error", msg: "对方为管理员您暂时无权添加"});
        return;
      }
    }
    for (const mid of memberIds) {
      if (mid === ownerId) continue;
      const u = (await kv.get(["users", mid])).value;
      if (!u) { send(ws, {type: "error", msg: "成员 " + mid + " 不存在"}); return; }
    }
    const gid = "g_" + Date.now() + "_" + Math.random().toString(36).slice(2, 6);
    const g = {id: gid, name, ownerId, members: memberIds, createdAt: Date.now()};
    await kv.set(["groups", gid], g);
    for (const mid of memberIds) {
      const gl = (await kv.get(["userGroups", mid])).value || [];
      if (!gl.includes(gid)) gl.push(gid);
      await kv.set(["userGroups", mid], gl);
    }
    send(ws, {type: "groupCreated", group: g});
    memberIds.forEach((mid: string) => {
      if (mid !== ownerId) send(online.get(mid), {type: "groupInvited", group: g});
    });
    return;
  }

  // ---------- 群列表 ----------
  if (t === "listGroups") {
    const id = String(msg.id || "");
    if (!(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    const gids = (await kv.get(["userGroups", id])).value || [];
    const groups = [];
    for (const gid of gids) {
      const g = (await kv.get(["groups", gid])).value;
      if (g) groups.push(g);
    }
    send(ws, {type: "groups", list: groups});
    return;
  }

  // ---------- 退群 ----------
  if (t === "leaveGroup") {
    const id = String(msg.id || "");
    const gid = String(msg.gid || "");
    if (!id || !gid) return;
    if (!(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    const g = (await kv.get(["groups", gid])).value;
    if (!g) return;
    if (!g.members.includes(id)) { send(ws, {type: "error", msg: "你不是群成员"}); return; }
    g.members = g.members.filter((m: string) => m !== id);
    await kv.set(["groups", gid], g);
    const gl = (await kv.get(["userGroups", id])).value || [];
    await kv.set(["userGroups", id], gl.filter((x: string) => x !== gid));
    g.members.forEach((mid: string) => send(online.get(mid), {type: "groupUpdate", group: g}));
    send(ws, {type: "leftGroup", gid});
    return;
  }

  // ---------- 拉取历史消息（带权限） ----------
  if (t === "getMessages") {
    const id = String(msg.id || "");
    const chatId = String(msg.chatId || "");
    if (!id || !chatId) return;
    if (!(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (chatId.startsWith("g_")) {
      const g = (await kv.get(["groups", chatId])).value;
      if (!g || !g.members.includes(id)) { send(ws, {type: "error", msg: "无权查看该会话"}); return; }
    } else {
      const parts = chatId.split("_");
      if (parts.length < 3 || !parts.includes(id)) { send(ws, {type: "error", msg: "无权查看该会话"}); return; }
    }
    const limit = Math.min(parseInt(String(msg.limit)) || 50, 200);
    const list = (await kv.get(["messages", chatId])).value || [];
    send(ws, {type: "messages", chatId, list: list.slice(-limit)});
    return;
  }

  // ---------- 发送消息 ----------
  if (t === "sendMessage") {
    const from = String(msg.from || "");
    const to = String(msg.to || "");
    const content = String(msg.content || "").slice(0, 4000);
    const image = String(msg.image || "").slice(0, IMAGE_MAX);
    const mode = msg.mode === "novel" ? "novel" : "normal";
    if (!from || !to || (!content && !image)) return;
    if (!(await authed(from, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "发送过于频繁，请稍后再试"}); return; }
    let chatId = "";
    let recipients: string[] = [];
    if (to.startsWith("g_")) {
      const g = (await kv.get(["groups", to])).value;
      if (!g) { send(ws, {type: "error", msg: "群不存在"}); return; }
      if (!g.members.includes(from)) { send(ws, {type: "error", msg: "你不是群成员，无法发言"}); return; }
      chatId = to;
      recipients = g.members.slice();
    } else {
      chatId = chatIdOf(from, to);
      recipients = [from, to];
      // 私聊必须仍是好友：删除好友后不能再发消息
      const f1 = (await kv.get(["friends", from])).value || [];
      const f2 = (await kv.get(["friends", to])).value || [];
      if (!f1.includes(to) || !f2.includes(from)) {
        send(ws, {type: "error", msg: "你们已不是好友，无法发送消息"});
        return;
      }
    }
    const entry: any = {
      id: String(msg.id || crypto.randomUUID()),
      from, content, mode,
      time: Number(msg.time) || Date.now(),
    };
    if (image) entry.image = image;
    const list = (await kv.get(["messages", chatId])).value || [];
    // 按 id 去重：断线重连后的补发不会产生重复消息
    if (!list.find((m: any) => m.id === entry.id)) {
      list.push(entry);
      if (list.length > MSG_LIMIT) list.splice(0, list.length - MSG_LIMIT);
      await kv.set(["messages", chatId], list);
    }
    recipients.forEach((rid: string) => send(online.get(rid), {type: "newMessage", chatId, entry}));
    return;
  }

  // ---------- 删除消息（服务端删除 + 广播） ----------
  if (t === "deleteMessage") {
    const from = String(msg.from || "");
    const chatId = String(msg.chatId || "");
    const time = Number(msg.time) || 0;
    if (!from || !chatId || !time) return;
    if (!(await authed(from, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const list = (await kv.get(["messages", chatId])).value || [];
    const idx = list.findIndex((m: any) => m.time === time);
    if (idx < 0) { send(ws, {type: "error", msg: "消息不存在或已删除"}); return; }
    if (list[idx].from !== from && !isAdmin(from)) { send(ws, {type: "error", msg: "只能删除自己的消息"}); return; }
    list.splice(idx, 1);
    await kv.set(["messages", chatId], list);
    let recips: string[] = [];
    if (chatId.startsWith("g_")) {
      const g = (await kv.get(["groups", chatId])).value;
      recips = g ? g.members : [];
    } else {
      const p = chatId.split("_");
      recips = [p[1], p[2]];
    }
    recips.forEach((rid: string) => send(online.get(rid), {type: "messageDeleted", chatId, time}));
    return;
  }

  // ---------- 管理页：所有用户（在线状态 + 余额，仅管理员） ----------
  if (t === "adminListUsers") {
    const id = String(msg.id || "");
    if (!id || !isAdmin(id) || !(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const list: any[] = [];
    const iter = kv.list({prefix: ["users"]});
    for await (const e of iter) {
      const u = e.value as any;
      if (!u || !u.id) continue;
      list.push({
        id: String(u.id),
        nick: String(u.nick || "用户").slice(0, 20),
        avatar: String(u.avatar || ""),
        online: !!online.get(String(u.id)),
        balance: await getBalance(String(u.id)),
        isAdmin: isAdmin(String(u.id)),
      });
    }
    list.sort((a, b) => Number(b.online) - Number(a.online) || String(a.id).localeCompare(String(b.id)));
    send(ws, {type: "adminUsers", list});
    return;
  }

  // ---------- 钱包（时间余额，单位小时） ----------
  if (t === "getWallet") {
    const id = String(msg.id || "");
    if (!id || !(await authed(id, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    const balance = await getBalance(id);
    send(ws, {type: "wallet", balance});
    return;
  }

  // ---------- 转账（时间，单位小时；收款制） ----------
  if (t === "sendTransfer") {
    const from = String(msg.from || "");
    const to = String(msg.to || "").trim();
    const amount = round2(Number(msg.amount));
    if (!from || !to || !(amount > 0) || from === to) return;
    if (!(await authed(from, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    if (to.startsWith("g_")) { send(ws, {type: "error", msg: "群聊不支持转账"}); return; }
    const tu = (await kv.get(["users", to])).value;
    if (!tu) { send(ws, {type: "error", msg: "对方不存在"}); return; }
    const myFs = (await kv.get(["friends", from])).value || [];
    const toFs = (await kv.get(["friends", to])).value || [];
    if (!myFs.includes(to) || !toFs.includes(from)) { send(ws, {type: "error", msg: "仅好友之间可转账"}); return; }
    const fromBal = await getBalance(from);
    // 管理员余额无限：转账不扣管理员余额
    if (!isAdmin(from)) {
      if (fromBal < amount) { send(ws, {type: "error", msg: "余额不足"}); return; }
      await setBalance(from, fromBal - amount);
    }
    const transferId = String(msg.id || crypto.randomUUID());
    const chatId = chatIdOf(from, to);
    const entry: any = {
      id: "tr_" + Date.now() + "_" + Math.random().toString(36).slice(2, 8),
      from, to, content: "", mode: "normal",
      time: Number(msg.time) || Date.now(),
      transfer: {id: transferId, amount, status: "pending"},
    };
    const list = (await kv.get(["messages", chatId])).value || [];
    list.push(entry);
    if (list.length > MSG_LIMIT) list.splice(0, list.length - MSG_LIMIT);
    await kv.set(["messages", chatId], list);
    [from, to].forEach((rid) => send(online.get(rid), {type: "newMessage", chatId, entry}));
    return;
  }

  // ---------- 收款 ----------
  if (t === "acceptTransfer") {
    const operator = String(msg.from || "");
    const chatId = String(msg.chatId || "");
    const transferId = String(msg.transferId || "");
    if (!operator || !chatId || !transferId) return;
    if (!(await authed(operator, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const list = (await kv.get(["messages", chatId])).value || [];
    const entry = list.find((m: any) => m.transfer && m.transfer.id === transferId);
    if (!entry || !entry.transfer) { send(ws, {type: "error", msg: "转账不存在"}); return; }
    if (entry.transfer.status !== "pending") { send(ws, {type: "error", msg: "该转账已处理"}); return; }
    if (entry.to !== operator) { send(ws, {type: "error", msg: "只有收款人才能收款"}); return; }
    const bal = await getBalance(operator);
    await setBalance(operator, bal + entry.transfer.amount);
    entry.transfer.status = "done";
    await kv.set(["messages", chatId], list);
    [entry.from, entry.to].forEach((rid) => send(online.get(rid), {type: "transferUpdate", chatId, transferId, status: "done"}));
    send(ws, {type: "transferAccepted", amount: entry.transfer.amount});
    return;
  }

  // ---------- 撤回 / 退回 ----------
  if (t === "refundTransfer") {
    const operator = String(msg.from || "");
    const chatId = String(msg.chatId || "");
    const transferId = String(msg.transferId || "");
    if (!operator || !chatId || !transferId) return;
    if (!(await authed(operator, msg.token))) { send(ws, {type: "error", msg: "未授权"}); return; }
    if (!rateLimit(ws)) { send(ws, {type: "error", msg: "操作过于频繁"}); return; }
    const list = (await kv.get(["messages", chatId])).value || [];
    const entry = list.find((m: any) => m.transfer && m.transfer.id === transferId);
    if (!entry || !entry.transfer) { send(ws, {type: "error", msg: "转账不存在"}); return; }
    if (entry.transfer.status !== "pending") { send(ws, {type: "error", msg: "该转账已处理"}); return; }
    if (operator !== entry.from && operator !== entry.to) { send(ws, {type: "error", msg: "无权操作该转账"}); return; }
    // 管理员发出的转账未扣款，退回时不加回（管理员余额无限）
    if (!isAdmin(entry.from)) {
      const bal = await getBalance(entry.from);
      await setBalance(entry.from, bal + entry.transfer.amount);
    }
    entry.transfer.status = "refunded";
    await kv.set(["messages", chatId], list);
    [entry.from, entry.to].forEach((rid) => send(online.get(rid), {type: "transferUpdate", chatId, transferId, status: "refunded"}));
    return;
  }
}

// ---------- 钱包工具 ----------
function round2(n: number): number {
  return Math.round(n * 100) / 100;
}
async function getBalance(id: string): Promise<number> {
  const v = (await kv.get(["wallets", id])).value;
  if (typeof v === "number") return v;
  await kv.set(["wallets", id], INITIAL_BALANCE);
  return INITIAL_BALANCE;
}
async function setBalance(id: string, b: number) {
  await kv.set(["wallets", id], round2(b));
}
