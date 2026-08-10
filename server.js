require('dotenv').config();
const express = require('express');
const { MongoClient } = require('mongodb');
const { createServer } = require('http');
const WebSocket = require('ws');
const crypto = require('crypto');
const cors = require('cors');

const app = express();
const server = createServer(app);
const wss = new WebSocket.Server({ noServer: true });

const MONGO_URI = process.env.MONGODB_URI || 'mongodb://localhost:27017';
const PORT = process.env.PORT || 3000;
const ALLOWED_ORIGINS = (process.env.ALLOWED_ORIGINS || 'http://localhost:3000').split(',');

let db;

app.use(cors({ origin: ALLOWED_ORIGINS, credentials: true }));
app.use(express.json());
app.use(express.static('public'));

async function authMiddleware(req, res, next) {
  const teamKey = req.headers['x-team-key'];
  const clientId = req.headers['x-client-id'];
  const actorName = req.headers['x-actor-name'];

  if (!teamKey || !clientId || !actorName) {
    return res.status(400).json({ error: 'Missing auth headers' });
  }

  req.teamKeyHash = sha256(Buffer.from(teamKey).toString('base64'));
  req.clientId = clientId;
  req.actorName = actorName;
  req.ip = req.ip || 'unknown';

  await db.collection('teams').updateOne(
    { teamKeyHash: req.teamKeyHash },
    {
      $setOnInsert: {
        teamKeyHash: req.teamKeyHash,
        name: `Team-${req.teamKeyHash.slice(0, 8)}`,
        createdAt: new Date()
      },
      $set: { lastSeenAt: new Date() }
    },
    { upsert: true }
  );

  next();
}

app.use(authMiddleware);

app.get('/api/team/quizzes', async (req, res) => {
  try {
    const quizzes = await db.collection('quizzes')
      .find({ teamKeyHash: req.teamKeyHash })
      .sort({ updatedAt: -1 })
      .project({
        _id: 1, title: 1, desc: 1, version: 1, updatedAt: -1,
        lockedBySession: 1, timePerQuestion: 1,
        updatedBy: 1,
        questionsCount: { $size: '$questions' }
      })
      .toArray();
    res.json({ quizzes });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/team/quizzes/:id', async (req, res) => {
  try {
    const quiz = await db.collection('quizzes').findOne({
      _id: req.params.id,
      teamKeyHash: req.teamKeyHash
    });
    if (!quiz) return res.status(404).json({ error: 'Not found' });
    res.json({ quiz });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/team/quizzes', async (req, res) => {
  try {
    const { title, desc, timePerQuestion, questions } = req.body;

    if (!title || !Array.isArray(questions) || questions.length < 1) {
      return res.status(400).json({ error: 'Invalid quiz data' });
    }

    for (const q of questions) {
      if (!q.id || !q.text || !Array.isArray(q.answers) || q.answers.length !== 4 ||
          typeof q.correct !== 'number' || q.correct < 0 || q.correct > 3) {
        return res.status(400).json({ error: 'Invalid question' });
      }
    }

    const quiz = {
      _id: `qb_${generateRandomId()}`,
      teamKeyHash: req.teamKeyHash,
      title,
      desc: desc || '',
      timePerQuestion: Math.min(Math.max(timePerQuestion || 12, 5), 60),
      questions,
      version: 1,
      lockedBySession: null,
      updatedBy: { name: req.actorName, clientId: req.clientId },
      lastModified: {},
      createdAt: new Date(),
      updatedAt: new Date()
    };

    await db.collection('quizzes').insertOne(quiz);

    await db.collection('quiz_audit').insertOne({
      teamKeyHash: req.teamKeyHash,
      quizId: quiz._id,
      ts: new Date(),
      actor: { name: req.actorName, clientId: req.clientId },
      action: 'quiz.create',
      ip: req.ip,
      newVersion: 1
    });

    broadcastToTeam(req.teamKeyHash, { type: 'team:quizzesUpdated', data: {} });

    res.status(201).json({ quiz });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.delete('/api/team/quizzes/:id', async (req, res) => {
  try {
    const quiz = await db.collection('quizzes').findOne({
      _id: req.params.id,
      teamKeyHash: req.teamKeyHash
    });

    if (!quiz) return res.status(404).json({ error: 'Not found' });

    if (quiz.lockedBySession) {
      const session = await db.collection('sessions').findOne({
        teamKeyHash: req.teamKeyHash,
        code: quiz.lockedBySession,
        status: 'live'
      });
      if (session && session.host.clientId !== req.clientId) {
        return res.status(423).json({ error: 'Quiz locked (live session)' });
      }
    }

    await db.collection('quizzes').deleteOne({ _id: req.params.id });

    await db.collection('quiz_audit').insertOne({
      teamKeyHash: req.teamKeyHash,
      quizId: req.params.id,
      ts: new Date(),
      actor: { name: req.actorName, clientId: req.clientId },
      action: 'quiz.delete',
      ip: req.ip
    });

    broadcastToTeam(req.teamKeyHash, { type: 'team:quizzesUpdated', data: {} });

    res.json({ deleted: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/team/sessions', async (req, res) => {
  try {
    const { quizId } = req.body;

    const quiz = await db.collection('quizzes').findOne({
      _id: quizId,
      teamKeyHash: req.teamKeyHash
    });

    if (!quiz) return res.status(404).json({ error: 'Quiz not found' });

    const code = generateSessionCode();
    const session = {
      teamKeyHash: req.teamKeyHash,
      code,
      quizId,
      status: 'lobby',
      host: { name: req.actorName, clientId: req.clientId },
      playersCount: 1,
      createdAt: new Date()
    };

    await db.collection('sessions').insertOne(session);
    broadcastToTeam(req.teamKeyHash, { type: 'session:created', data: { code, quizId } });

    res.status(201).json({ session });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/team/sessions/:code/start', async (req, res) => {
  try {
    const session = await db.collection('sessions').findOne({
      teamKeyHash: req.teamKeyHash,
      code: req.params.code
    });

    if (!session) return res.status(404).json({ error: 'Session not found' });
    if (session.host.clientId !== req.clientId) return res.status(403).json({ error: 'Only host' });

    await db.collection('sessions').updateOne(
      { _id: session._id },
      { $set: { status: 'live', startedAt: new Date() } }
    );

    await db.collection('quizzes').updateOne(
      { _id: session.quizId, teamKeyHash: req.teamKeyHash },
      { $set: { lockedBySession: req.params.code } }
    );

    await db.collection('quiz_audit').insertOne({
      teamKeyHash: req.teamKeyHash,
      quizId: session.quizId,
      ts: new Date(),
      actor: { name: req.actorName, clientId: req.clientId },
      action: 'quiz.lock',
      ip: req.ip
    });

    broadcastToTeam(req.teamKeyHash, {
      type: 'quiz:locked',
      data: { quizId: session.quizId, sessionCode: req.params.code, hostName: req.actorName }
    });

    res.json({ session });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/team/sessions/:code/end', async (req, res) => {
  try {
    const session = await db.collection('sessions').findOne({
      teamKeyHash: req.teamKeyHash,
      code: req.params.code
    });

    if (!session) return res.status(404).json({ error: 'Session not found' });
    if (session.host.clientId !== req.clientId) return res.status(403).json({ error: 'Only host' });

    await db.collection('sessions').updateOne(
      { _id: session._id },
      { $set: { status: 'ended', endedAt: new Date() } }
    );

    await db.collection('quizzes').updateOne(
      { _id: session.quizId, teamKeyHash: req.teamKeyHash },
      { $set: { lockedBySession: null } }
    );

    await db.collection('quiz_audit').insertOne({
      teamKeyHash: req.teamKeyHash,
      quizId: session.quizId,
      ts: new Date(),
      actor: { name: req.actorName, clientId: req.clientId },
      action: 'quiz.unlock',
      ip: req.ip
    });

    broadcastToTeam(req.teamKeyHash, { type: 'quiz:unlocked', data: { quizId: session.quizId } });

    res.json({ session });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.post('/api/team/sessions/:code/result', async (req, res) => {
  try {
    const { playerName, score, accuracy, playersCount } = req.body;

    const session = await db.collection('sessions').findOne({
      teamKeyHash: req.teamKeyHash,
      code: req.params.code
    });

    if (!session) return res.status(404).json({ error: 'Session not found' });

    await db.collection('session_results').insertOne({
      teamKeyHash: req.teamKeyHash,
      sessionCode: req.params.code,
      quizId: session.quizId,
      playerName,
      clientId: req.clientId,
      score,
      accuracy,
      playersCount,
      createdAt: new Date()
    });

    res.json({ saved: true });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

app.get('/api/team/analytics/kpis', async (req, res) => {
  try {
    const from = req.query.from ? new Date(req.query.from) : new Date(Date.now() - 30 * 24 * 60 * 60 * 1000);
    const to = req.query.to ? new Date(req.query.to) : new Date();

    const results = await db.collection('session_results')
      .find({ teamKeyHash: req.teamKeyHash, createdAt: { $gte: from, $lte: to } })
      .toArray();

    const sessions = new Set(results.map(r => r.sessionCode)).size;
    const players = new Set(results.map(r => r.playerName)).size;
    const avgAcc = results.length ? Math.round(results.reduce((a, r) => a + (r.accuracy || 0), 0) / results.length) : 0;
    const avgTime = results.length ? (results.reduce((a, r) => a + 6, 0) / results.length).toFixed(1) : 0;

    const quizzes = await db.collection('quizzes')
      .find({ teamKeyHash: req.teamKeyHash })
      .sort({ updatedAt: -1 })
      .limit(5)
      .toArray();

    const quizzesData = quizzes.map(q => {
      const qResults = results.filter(r => r.quizId === q._id);
      const plays = qResults.length;
      const avgAccuracy = plays ? Math.round(qResults.reduce((a, r) => a + (r.accuracy || 0), 0) / plays) : 0;
      return { ...q, plays, avgAccuracy };
    });

    const topQuiz = quizzesData[0];

    res.json({
      sessions,
      uniquePlayers: players,
      avgAccuracy: avgAcc,
      avgTimePerQ: parseFloat(avgTime),
      topQuiz: topQuiz ? { id: topQuiz._id, title: topQuiz.title, plays: topQuiz.plays } : null,
      quizzes: quizzesData
    });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

const presenceMap = new Map();

function getPresence(teamKeyHash, quizId) {
  if (!presenceMap.has(teamKeyHash)) presenceMap.set(teamKeyHash, new Map());
  if (!presenceMap.get(teamKeyHash).has(quizId)) presenceMap.get(teamKeyHash).set(quizId, []);
  return presenceMap.get(teamKeyHash).get(quizId);
}

wss.on('connection', async (ws, req, teamKeyHash, clientId, actorName) => {
  let subscribed = new Set();

  ws.on('message', async (data) => {
    try {
      const msg = JSON.parse(data);
      const { type, data: msgData } = msg;

      switch (type) {
        case 'quiz:subscribe':
          await handleQuizSubscribe(ws, teamKeyHash, clientId, actorName, msgData, subscribed);
          break;
        case 'quiz:patch':
          await handleQuizPatch(ws, teamKeyHash, clientId, actorName, msgData, subscribed);
          break;
        case 'quiz:presence':
          handlePresence(teamKeyHash, clientId, actorName, msgData);
          break;
      }
    } catch (e) {
      console.error('WS message error:', e);
      ws.send(JSON.stringify({ type: 'error', data: { msg: e.message } }));
    }
  });

  ws.on('close', () => {
    for (const quizId of subscribed) {
      const presence = getPresence(teamKeyHash, quizId);
      const idx = presence.findIndex(p => p.clientId === clientId);
      if (idx >= 0) presence.splice(idx, 1);
    }
  });
});

async function handleQuizSubscribe(ws, teamKeyHash, clientId, actorName, { quizId }, subscribed) {
  subscribed.add(quizId);

  const quiz = await db.collection('quizzes').findOne({
    _id: quizId,
    teamKeyHash
  });

  if (!quiz) {
    ws.send(JSON.stringify({ type: 'error', data: { msg: 'Quiz not found' } }));
    return;
  }

  ws.send(JSON.stringify({ type: 'quiz:state', data: { quiz } }));

  const presence = getPresence(teamKeyHash, quizId);
  ws.send(JSON.stringify({ type: 'presence:update', data: { quizId, editors: presence } }));
}

async function handleQuizPatch(ws, teamKeyHash, clientId, actorName, { quizId, baseVersion, ops }, subscribed) {
  const quiz = await db.collection('quizzes').findOne({
    _id: quizId,
    teamKeyHash
  });

  if (!quiz) {
    ws.send(JSON.stringify({ type: 'error', data: { msg: 'Quiz not found' } }));
    return;
  }

  if (quiz.lockedBySession) {
    const session = await db.collection('sessions').findOne({
      teamKeyHash,
      code: quiz.lockedBySession,
      status: 'live'
    });
    if (session && session.host.clientId !== clientId) {
      ws.send(JSON.stringify({
        type: 'quiz:locked',
        data: { quizId, sessionCode: quiz.lockedBySession, hostName: session.host.name }
      }));
      return;
    }
  }

  if (baseVersion !== quiz.version) {
    const conflicts = [];
    for (const op of ops) {
      const path = op.path;
      const lastMod = quiz.lastModified?.[path];
      if (lastMod && lastMod.version > baseVersion) {
        conflicts.push({
          path,
          changedBy: lastMod.by,
          changedAtVersion: lastMod.version
        });
      }
    }

    ws.send(JSON.stringify({
      type: 'quiz:conflict',
      data: { quizId, serverVersion: quiz.version, quiz, conflicts }
    }));
    return;
  }

  const updated = JSON.parse(JSON.stringify(quiz));
  for (const op of ops) {
    applyOpToQuiz(updated, op);
  }

  updated.version = quiz.version + 1;
  updated.updatedBy = { name: actorName, clientId };
  updated.updatedAt = new Date();

  ops.forEach(op => {
    if (op.path) {
      updated.lastModified = updated.lastModified || {};
      updated.lastModified[op.path] = { version: updated.version, by: actorName };
    }
  });

  await db.collection('quizzes').replaceOne({ _id: quizId }, updated);

  await db.collection('quiz_audit').insertOne({
    teamKeyHash,
    quizId,
    ts: new Date(),
    actor: { name: actorName, clientId },
    action: 'quiz.patch',
    baseVersion,
    newVersion: updated.version,
    ops,
    ip: 'unknown'
  });

  broadcastToTeam(teamKeyHash, {
    type: 'quiz:patched',
    data: {
      quizId,
      newVersion: updated.version,
      opsApplied: ops,
      updatedBy: { name: actorName, clientId },
      updatedAt: updated.updatedAt
    }
  });
}

function applyOpToQuiz(quiz, op) {
  switch (op.op) {
    case 'setTitle':
      quiz.title = op.value;
      break;
    case 'setDesc':
      quiz.desc = op.value;
      break;
    case 'setTimePerQuestion':
      quiz.timePerQuestion = op.value;
      break;
    case 'addQuestion':
      quiz.questions.push(op.question);
      break;
    case 'updateQuestion': {
      const q = quiz.questions.find(x => x.id === op.questionId);
      if (q) Object.assign(q, op.patch);
      break;
    }
    case 'deleteQuestion':
      quiz.questions = quiz.questions.filter(x => x.id !== op.questionId);
      break;
    case 'reorderQuestions': {
      const map = new Map(quiz.questions.map(q => [q.id, q]));
      quiz.questions = op.order.map(id => map.get(id)).filter(Boolean);
      break;
    }
  }
}

function handlePresence(teamKeyHash, clientId, actorName, { quizId, mode }) {
  const presence = getPresence(teamKeyHash, quizId);
  const idx = presence.findIndex(p => p.clientId === clientId);
  if (idx >= 0) presence.splice(idx, 1);

  if (mode) {
    presence.push({ clientId, name: actorName, mode, at: new Date().toISOString() });
  }

  broadcastToTeam(teamKeyHash, {
    type: 'presence:update',
    data: { quizId, editors: presence }
  });
}

function broadcastToTeam(teamKeyHash, message) {
  wss.clients.forEach(ws => {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  });
}

server.on('upgrade', (req, socket, head) => {
  const url = new URL(req.url, `http://${req.headers.host}`);
  const teamKey = url.searchParams.get('teamKey');
  const clientId = url.searchParams.get('clientId');
  const actorName = url.searchParams.get('actorName');

  if (!teamKey || !clientId || !actorName) {
    socket.destroy();
    return;
  }

  const teamKeyHash = sha256(Buffer.from(teamKey).toString('base64'));

  wss.handleUpgrade(req, socket, head, (ws) => {
    wss.emit('connection', ws, req, teamKeyHash, clientId, actorName);
  });
});

function sha256(str) {
  return crypto.createHash('sha256').update(str).digest('hex');
}

function generateSessionCode() {
  const chars = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789';
  let code = '';
  for (let i = 0; i < 6; i++) {
    code += chars[Math.floor(Math.random() * chars.length)];
  }
  return code;
}

function generateRandomId() {
  return crypto.randomBytes(6).toString('hex').toUpperCase();
}

(async () => {
  try {
    const client = await MongoClient.connect(MONGO_URI, { useUnifiedTopology: true });
    db = client.db('quiz_blitz');

    await db.collection('quizzes').createIndex({ teamKeyHash: 1, updatedAt: -1 });
    await db.collection('quizzes').createIndex({ _id: 1, teamKeyHash: 1 }, { unique: true });
    await db.collection('sessions').createIndex({ teamKeyHash: 1, code: 1 }, { unique: true });
    await db.collection('session_results').createIndex({ teamKeyHash: 1, quizId: 1, createdAt: -1 });
    await db.collection('quiz_audit').createIndex({ teamKeyHash: 1, quizId: 1, ts: -1 });

    server.listen(PORT, '0.0.0.0', () => {
      console.log(`\n✅ Quiz Blitz Pro running on port ${PORT}`);
      console.log(`🗄️  MongoDB: Connected`);
      console.log(`⚡ WebSocket: wss://your-domain.com/ws\n`);
    });
  } catch (err) {
    console.error('❌ Startup failed:', err.message);
    process.exit(1);
  }
})();
