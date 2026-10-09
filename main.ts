const kv = await Deno.openKv();
const online = new Map<string, WebSocket>();
const ADMIN_IDS = ['1', '2'];

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
      if (userId && online.get(userId) === socket) online.delete(userId);
    };
    return response;
  }
  return new Response("ok");
});

async function handle(ws: WebSocket, msg: any, setId: (id: string) => void) {
  const t = msg.type;

  if (t === "register") {
    const id = String(msg.id || "").trim();
    if (!id) return;
    const nick = String(msg.nick || "用户").slice(0, 20);
    const avatar = String(msg.avatar || "");
    await kv.set(["users", id], {id, nick, avatar, lastSeen: Date.now()});
    const oldWs = online.get(id);
    if (oldWs && oldWs !== ws) {
      try { oldWs.close(); } catch(_e) {}
    }
    online.set(id, ws);
    setId(id);
    send(ws, {type: "registered", id});
    const reqs = (await kv.get(["requests", id])).value || [];
    send(ws, {type: "requests", list: reqs});
    return;
  }

  if (t === "updateProfile") {
    const id = String(msg.id || "");
    if (!id) return;
    const nick = String(msg.nick || "用户").slice(0, 20);
    const avatar = String(msg.avatar || "");
    await kv.set(["users", id], {id, nick, avatar, lastSeen: Date.now()});
    const friends = (await kv.get(["friends", id])).value || [];
    friends.forEach((fid: string) => send(online.get(fid), {type: "friendUpdate", friendId: id, nick, avatar}));
    return;
  }

  if (t === "listFriends") {
    const id = String(msg.id || "");
    const friendIds = (await kv.get(["friends", id])).value || [];
    const friends = [];
    for (const fid of friendIds) {
      const u = (await kv.get(["users", fid])).value;
      if (u) friends.push(u);
    }
    send(ws, {type: "friends", list: friends});
    return;
  }

  if (t === "addFriend") {
    const fromId = String(msg.fromId || "");
    const targetId = String(msg.targetId || "").trim();
    if (!fromId || !targetId) return;
    if (fromId === targetId) { send(ws, {type: "addFriendResult", ok: false, reason: "不能加自己"}); return; }
    if (isAdmin(targetId) && !isAdmin(fromId)) {
      send(ws, {type: "addFriendResult", ok: false, reason: "该用户为管理员，不能主动添加"});
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

  if (t === "acceptFriend") {
    const myId = String(msg.myId || "");
    const fromId = String(msg.fromId || "");
    if (!myId || !fromId) return;
    const myFriends = (await kv.get(["friends", myId])).value || [];
    const fromFriends = (await kv.get(["friends", fromId])).value || [];
    if (!myFriends.includes(fromId)) myFriends.push(fromId);
    if (!fromFriends.includes(myId)) fromFriends.push(myId);
    await kv.set(["friends", myId], myFriends);
    await kv.set(["friends", fromId], fromFriends);
    const reqs = (await kv.get(["requests", myId])).value || [];
    await kv.set(["requests", myId], reqs.filter((r: any) => r.fromId !== fromId));
    send(ws, {type: "friendAdded", friendId: fromId});
    send(online.get(fromId), {type: "friendAdded", friendId: myId});
    return;
  }

  if (t === "rejectFriend") {
    const myId = String(msg.myId || "");
    const fromId = String(msg.fromId || "");
    if (!myId || !fromId) return;
    const reqs = (await kv.get(["requests", myId])).value || [];
        await kv.set(["requests", myId], reqs.filter((r: any) => r.fromId !== fromId));
    send(ws, {type: "friendRejected", friendId: fromId});
    return;
  }

  if (t === "deleteFriend") {
    const myId = String(msg.myId || "");
    const targetId = String(msg.targetId || "");
    if (!myId || !targetId) return;
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

  if (t === "kickMember") {
    const gid = String(msg.gid || "");
    const operatorId = String(msg.operatorId || "");
    const targetId = String(msg.targetId || "");
    if (!gid || !operatorId || !targetId) return;
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
    send(ws, {type: "groupUpdate", group: g});
    return;
  }

  if (t === "createGroup") {
    const ownerId = String(msg.ownerId || "");
    const name = String(msg.name || "群聊").slice(0, 20);
    const memberIds = Array.isArray(msg.members) ? msg.members.map(String) : [];
    if (!ownerId) return;
    if (!memberIds.includes(ownerId)) memberIds.push(ownerId);
    if (!isAdmin(ownerId)) {
      const adminInGroup = memberIds.some((mid: string) => isAdmin(mid));
      if (adminInGroup) {
        send(ws, {type: "error", msg: "不能将管理员拉入群聊"});
        return;
      }
    }
    const gid = "g_" + Date.now() + "_" + Math.random().toString(36).slice(2,6);
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

  if (t === "listGroups") {
    const id = String(msg.id || "");
    const gids = (await kv.get(["userGroups", id])).value || [];
    const groups = [];
    for (const gid of gids) {
      const g = (await kv.get(["groups", gid])).value;
      if (g) groups.push(g);
    }
    send(ws, {type: "groups", list: groups});
    return;
  }

  if (t === "leaveGroup") {
    const id = String(msg.id || "");
    const gid = String(msg.gid || "");
    if (!id || !gid) return;
    const g = (await kv.get(["groups", gid])).value;
    if (!g) return;
    g.members = g.members.filter((m: string) => m !== id);
    await kv.set(["groups", gid], g);
    const gl = (await kv.get(["userGroups", id])).value || [];
    await kv.set(["userGroups", id], gl.filter((x: string) => x !== gid));
    g.members.forEach((mid: string) => send(online.get(mid), {type: "groupUpdate", group: g}));
    send(ws, {type: "leftGroup", gid});
    return;
  }

  if (t === "getMessages") {
    const chatId = String(msg.chatId || "");
    const limit = Math.min(parseInt(msg.limit) || 50, 200);
    const list = (await kv.get(["messages", chatId])).value || [];
    send(ws, {type: "messages", chatId, list: list.slice(-limit)});
    return;
  }

  if (t === "sendMessage") {
    const from = String(msg.from || "");
    const to = String(msg.to || "");
    const content = String(msg.content || "").slice(0, 4000);
    const mode = msg.mode === "novel" ? "novel" : "normal";
    if (!from || !to || !content) return;
    let chatId = "";
    let recipients: string[] = [];
    if (to.startsWith("g_")) {
      const g = (await kv.get(["groups", to])).value;
      if (!g) { send(ws, {type: "error", msg: "群不存在"}); return; }
      chatId = to;
      recipients = g.members.slice();
    } else {
      chatId = chatIdOf(from, to);
      recipie    } else {
      chatId = chatIdOf(from, to);
      recipients = [from, to];
    }
    const entry = {from, content, mode, time: Date.now()};
    const list = (await kv.get(["messages", chatId])).value || [];
    list.push(entry);
    if (list.length > 500) list.splice(0, list.length - 500);
    await kv.set(["messages", chatId], list);
    recipients.forEach((rid: string) => send(online.get(rid), {type: "newMessage", chatId, entry}));
    return;
  }
}
